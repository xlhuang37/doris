// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#pragma once

#include <concurrentqueue.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include <gen_cpp/Types_types.h>

#include "common/status.h"
#include "exec/pipeline/closed_slot_table.h"
#include "exec/pipeline/pipeline_task.h"
#include "util/hash_util.hpp"

namespace doris {
#include "common/compile_check_begin.h"

// A query-granular task queue for closed-system profiling, shared by all workers of one
// pipeline scheduler (one per workload group).
//
// Closed mode (used by the "simple" CPU pool):
//   - Every runnable PipelineTask goes into its owning query's own sub-queue. There is no
//     shared queue: a task is only ever reachable through the query that owns it.
//   - The pool's workers are cut into `pipeline_closed_system_slots` contiguous groups of
//     equal size, and each group serves exactly one query. Worker i serves slot
//     `i * slots / workers` and nothing else: no work stealing, no global fallback, no
//     priority. 32 workers with slots=4 means 4 queries running on 8 workers each, which
//     is the experiment this mode exists for.
//   - Slot ownership lives in a static array (`_slots`, sized to the worker count once and
//     never resized). Binding a query means publishing a pointer to its sub-queue into one
//     slot; the workers of that slot pick it up within one execution slice. Only the
//     admission path writes a slot; workers only read it.
//   - Admission is first-come-first-served: the first push of a query queues it, and it is
//     bound as soon as a slot is free. A query that arrives when all slots are taken makes
//     no progress at all until one frees up. That is the point of a closed system, not a
//     bug: the workers of an empty slot idle rather than help anyone else.
//   - Binding is event-driven (query arrival and query termination), so there is no
//     scheduler thread and no periodic rebalance. `ClosedSlotTable` holds the policy: the
//     worker/slot mapping, the arrival queue and the bind/unbind decisions.
//   - A worker caches the `shared_ptr` to the query it serves and refreshes it only when
//     the slot's `seq` moves. That cached reference is what keeps a QueryState alive while
//     a worker may still touch it, so there is no attach/detach handshake.
//   - Each slot has its own parking spot, so enqueueing a task wakes only the workers that
//     are allowed to run it.
//
// General-only mode (used by the "blocking" pool, whose workers sit inside blocking
// execute() calls and cannot re-check their slot every slice): one plain shared lock-free
// queue, no partition, no per-query state. Note that these workers are outside the closed
// partition, so keep the blocking pool small when profiling.
//
// The public interface is kept identical to the previous implementation so the
// scheduler/worker loop (TaskScheduler::_do_work) is unchanged.
class MultiCoreTaskQueue {
public:
    enum class Mode {
        CLOSED,       // static slot partition: each worker group serves one query
        GENERAL_ONLY, // plain shared lock-free queue (blocking pool)
    };

    explicit MultiCoreTaskQueue(int core_size, Mode mode = Mode::CLOSED);

#ifndef BE_TEST
    ~MultiCoreTaskQueue();
    // Get the next task for the worker `core_id`.
    PipelineTaskSPtr take(int core_id);
#else
    virtual ~MultiCoreTaskQueue();
    virtual PipelineTaskSPtr take(int core_id);
#endif

    void close();

    // `core_id` is accepted for API compatibility but placement is by owning query: a task
    // goes to its query's sub-queue (closed mode) or the shared queue (general-only).
    Status push_back(PipelineTaskSPtr task);
    Status push_back(PipelineTaskSPtr task, int core_id);

    // Charge executed CPU time to the owning query's global counter (a profiling total,
    // not a scheduling input), then release the in-flight slot the task held.
    void update_statistics(PipelineTask* task, int64_t time_spent);

    // Release the in-flight slot a task held without charging runtime. Used when a
    // dequeued task turns out to be already running on another worker and is
    // re-queued without being executed.
    void release_task(PipelineTask* task);

    // QueryContext is being destroyed: give up the query's slot so the next waiting query
    // is admitted, and reclaim its state once nothing is left in flight. No-op in
    // general-only mode.
    void notify_query_terminated(const TUniqueId& query_id);

    int cores() const { return _core_size; }

    // Test hook: number of live per-query states (observes teardown/resurrection).
    size_t registry_size_for_test() const;

    // Test hook: how many workers currently serve `query_id`, i.e. the size of the worker
    // group of the slot it holds. 0 when it holds no slot.
    int assigned_workers_for_test(const TUniqueId& query_id) const;

    // Test hook: the slot `query_id` occupies, or -1 while it waits for admission.
    int slot_of_query_for_test(const TUniqueId& query_id) const;

    // Test hook: the slot count currently in effect (the config value, clamped).
    int slot_count_for_test() const;

    // Test hook: which slot worker `worker_id` serves under the current slot count.
    int slot_of_worker_for_test(int worker_id) const { return _slot_of_worker(worker_id); }

protected:
    // Single-attempt take with an explicit wait timeout. Returns nullptr if no task
    // becomes available within `timeout_ms` (or the queue is closed).
    PipelineTaskSPtr _take(int worker_id, uint32_t timeout_ms);

private:
    // ------------------------------------------------------------------
    // Per-query state (closed mode only). Created by the enqueue path, owned by the
    // registry, the slot table and the workers that serve it - whoever holds the last
    // `shared_ptr` destroys it, which is what makes the pointer a worker sees always safe.
    // ------------------------------------------------------------------
    struct QueryState {
        explicit QueryState(TUniqueId id) : query_id(id) {}

        // Registry key. The all-zero sentinel buckets tasks with no QueryContext (e.g.
        // RevokableTask); in closed mode it is admitted like any other query.
        const TUniqueId query_id;
        // This query's own queue. Multi-producer (RPC threads, dependency wakeups,
        // workers) and multi-consumer (the workers of its slot).
        moodycamel::ConcurrentQueue<PipelineTaskSPtr> sub_queue;
        // The slot serving this query, or -1 while it waits for admission. Written only
        // under `_admission_mutex`; read by producers to wake the right workers.
        std::atomic<int> slot_index {-1};
        // Approximate sub-queue length; invariant: >= real length.
        std::atomic<int> pending_approx {0};
        // Tasks of this query between dequeue and release. Ordering contract with
        // `pending_approx` (both seq_cst): a dequeuing worker increments in_flight BEFORE
        // decrementing pending_approx, so at every instant a live task is visible in at
        // least one of the two counters.
        std::atomic<int> in_flight {0};
        // Set once the QueryContext is gone. Guarded by `_admission_mutex`.
        bool terminated = false;
    };
    using QueryStatePtr = std::shared_ptr<QueryState>;

    // Where workers of one slot sleep when there is nothing to run. Producers bump the
    // epoch (under the mutex) only when somebody is actually waiting, so the common
    // enqueue path does no syscall; a worker snapshots the epoch before its last dequeue
    // attempt and skips the wait if it moved, which bounds lost-wakeup staleness. The
    // wait itself is also time-bounded.
    struct ParkPoint {
        std::mutex mutex;
        std::condition_variable cv;
        uint64_t epoch = 0; // guarded by `mutex`
        std::atomic<int> waiters {0};
    };

    // One entry of the static slot array. `owner` is the sub-queue pointer the workers of
    // this slot poll; it is guarded by `park.mutex` and every change bumps `seq`, which
    // workers read without locking to decide whether to refresh their cached copy.
    struct alignas(64) Slot {
        std::atomic<uint64_t> seq {0};
        ParkPoint park;
        QueryStatePtr owner;
    };

    // A worker's private view of its slot: the last `seq` it observed and the query it is
    // serving. Cache-aligned because every worker writes its own entry.
    struct alignas(64) WorkerLocal {
        uint64_t seen_seq = 0;
        int seen_slot = -1;
        QueryStatePtr bound;
    };

    // ---- enqueue / dequeue helpers ----
    Status _push(PipelineTaskSPtr task);
    PipelineTaskSPtr _try_take_once(int worker_id);
    // Refresh the worker's cached binding if its slot or the slot's `seq` changed, and
    // return the query it should serve (nullptr when its slot is empty).
    QueryState* _refresh_binding(int worker_id);
    void _release_in_flight(PipelineTask* task, bool charge, int64_t time_spent);

    int _slot_of_worker(int worker_id) const {
        if (worker_id < 0 || worker_id >= _core_size) {
            return -1;
        }
        return worker_id * _slot_count.load(std::memory_order_acquire) / _core_size;
    }
    // The slot count the config asks for, clamped the same way the slot table clamps it.
    int _configured_slot_count() const;
    // True when the partition in effect no longer matches the config, i.e. somebody
    // retuned the knob and the next admission event should re-shape the slots.
    bool _partition_is_stale() const {
        return _slot_count.load(std::memory_order_relaxed) != _configured_slot_count();
    }
    ParkPoint& _park_point_of(int worker_id);
    // Wake somebody parked on `park`, if anybody is. Cheap (no syscall, no lock) when
    // nobody waits, which is the common case on the enqueue path.
    void _notify_park(ParkPoint& park, bool all);
    void _wake_all_parks();

    // ---- admission (closed mode) ----
    // Queue `qs` for a slot and bind whatever the slot table decides. Called on the first
    // push of a query and on any push to a query that holds no slot.
    void _admit(const QueryStatePtr& qs);
    // Publish the slots the table just changed, which may list a slot more than once when
    // it was vacated and refilled in one pass. Called with `_admission_mutex` held.
    void _publish_slots(std::vector<int>* changed);
    // Move terminated queries that the pool is done with out of `_dying`. Called with
    // `_admission_mutex` held; the registry erase itself happens in `_erase_reclaimed`,
    // because the two locks are never held at the same time.
    void _collect_reclaimable(std::vector<QueryStatePtr>* reclaimed);
    void _erase_reclaimed(const std::vector<QueryStatePtr>& reclaimed);

    QueryStatePtr _find_query(const TUniqueId& query_id) const;

    bool _worker_in_range(int worker_id) const { return worker_id >= 0 && worker_id < _core_size; }

    const int _core_size;
    const Mode _mode;
    std::atomic<bool> _closed {false};

    // The shared queue, general-only mode only. Closed mode keeps every task in its
    // owning query's sub-queue.
    moodycamel::ConcurrentQueue<PipelineTaskSPtr> _queue;
    ParkPoint _general_park;

    // Registry of live QueryStates (closed mode). Holds one reference; the slot table and
    // the workers hold the others.
    mutable std::shared_mutex _registry_mutex;
    std::unordered_map<TUniqueId, QueryStatePtr> _registry;

    // Static slot array and the workers' private views, both sized once in the constructor
    // (to the worker count, the largest usable slot count) and never resized.
    std::vector<Slot> _slots;
    std::vector<WorkerLocal> _worker_local;
    // Mirror of `_slot_table.slot_count()` for the lock-free worker/slot mapping.
    std::atomic<int> _slot_count {1};

    // Slot ownership policy plus the queries waiting for a slot, and the terminated
    // queries not yet reclaimed. Touched only on query arrival and termination.
    mutable std::mutex _admission_mutex;
    ClosedSlotTable<QueryStatePtr> _slot_table;
    std::vector<QueryStatePtr> _dying;

    std::atomic<size_t> _total_task_size = 0;

    static constexpr auto WAIT_CORE_TASK_TIMEOUT_MS = 100;
};
#include "common/compile_check_end.h"
} // namespace doris
