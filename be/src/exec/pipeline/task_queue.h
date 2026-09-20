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
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gen_cpp/Types_types.h>

#include "common/status.h"
#include "exec/pipeline/las_slot_policy.h"
#include "exec/pipeline/pipeline_task.h"
#include "util/hash_util.hpp"

namespace doris {
#include "common/compile_check_begin.h"

// A query-granular task queue shared by all workers of one pipeline scheduler (one per
// workload group).
//
// Core design (full mode, used by the "simple" CPU pool):
//   - All runnable PipelineTasks live in one shared moodycamel::ConcurrentQueue,
//     partitioned by per-query explicit ProducerTokens: each task is enqueued exactly
//     once, through its owning query's token (enqueue serialized by a per-query mutex,
//     since explicit producers are single-producer). Sub-queues are drained lock-free
//     via try_dequeue_from_producer.
//   - A central scheduler thread ranks live queries by attained service (the
//     query-global CPU runtime counter, mirrored onto each QueryState) and periodically
//     publishes the top of that ranking into a shared slot array: slot 0 holds the
//     least-attained (most LAS) query, the last slot the `pipeline_las_slot_count`-th.
//     Ranking is a single append-only vector, re-sorted on every pass; equal attained
//     service keeps arrival order (stable_sort). A query only takes a slot while it has
//     demand (runnable tasks), so momentarily empty queries do not hold the array, and
//     queries past the array are served only by the fallback. Fewer live queries than
//     slots leaves the tail of the array empty.
//   - Attained service counts every pool that spends CPU on the query, not just this
//     one: the pipeline workers charge the runtime of each execution slice and the
//     scanner threads the thread CPU of each scan slice, so a scan-bound query gives up
//     pipeline workers in proportion to the CPU it is really using. The mirror is a
//     store of the absolute counter, refreshed by producers on enqueue and by workers on
//     charge - the only threads allowed to touch a QueryContext. Both are also the
//     moments a query enters or re-enters the competition for a slot (a task woken by a
//     scan block is re-submitted through push_back), so a query can never win a slot on
//     a stale value, and a late refresh loses nothing because the next one reads the
//     true total.
//   - The array is published as an immutable snapshot (`SlotTable`) rather than written
//     in place: the scheduler builds the next table, swaps it in under `_slot_mutex` and
//     bumps `_slot_epoch`; a worker compares the epoch (one relaxed load) on every
//     dequeue attempt and only takes the mutex to refresh its cached snapshot when it
//     moved. The cached snapshot co-owns every QueryState it lists, which is what makes
//     the pointers a worker holds safe without any attach/detach handshake.
//   - How a worker consumes the array is `pipeline_las_slot_policy`: "ordered" walks
//     slots 0..N-1 and takes the first task found (so the least-attained query gets as
//     many workers as it can keep busy), "fixed" pins each worker to one slot with the
//     workers spread evenly over the array (so per-query parallelism is
//     workers-per-slot). Both knobs are re-read on every scheduler pass and travel
//     inside the published snapshot, so a worker's view of the policy and of the slots
//     is always consistent.
//   - When the array yields nothing, workers fall back to a tokenless try_dequeue on the
//     shared queue (work-conserving, priority-blind relief valve), and they re-read the
//     array after every task so scheduler decisions take effect within one execution
//     slice.
//   - "Inelastic first": a task whose pipeline has exactly one task cannot be sped up
//     by parallelism, so queueing it behind elastic work only lengthens the query's
//     critical path. Such tasks keep full per-query bookkeeping (pending/in-flight
//     counters, idle detection, teardown) but bypass the per-query sub-queue and go
//     into a dedicated shared queue that every worker drains before anything else,
//     ahead of the slot array and of attained-service ranking entirely.
//   - Workers and producers notify the scheduler through a mutex-guarded inbox (new
//     queries, query termination); the scheduler sleeps on a condition variable with a
//     timer tick and re-publishes every pass so accumulated CPU is visible without
//     discrete level-crossing events.
//   - Teardown happens only after QueryContext destruction posts QUERY_TERMINATED and
//     the query has nothing pending or in flight. Temporary emptiness while the query is
//     still alive does not reclaim the node. Tasks with no QueryContext (RevokableTask)
//     share a sentinel id and live until queue close. QUERY_TERMINATED only labels the
//     live object; it leaves the ranking vector at the next compact, which is also what
//     drops it from the slot array. Since every reference is a shared_ptr, a stale
//     worker snapshot can outlive the registry entry without dangling - and it can never
//     yield a task, because reclamation requires the sub-queue to be empty.
//
// Degenerate mode (used by the "blocking" pool, whose workers sit inside blocking
// execute() calls and cannot honor the "re-read the array every slice" invariant):
// no scheduler thread, no per-query state, no ranking - just the shared lock-free
// queue plus worker parking. Runtime is still charged to the query-global counter
// so attained-service accounting in the other pools is unaffected.
//
// The public interface is kept identical to the previous implementation so the
// scheduler/worker loop (TaskScheduler::_do_work) is unchanged.
class MultiCoreTaskQueue {
public:
    enum class Mode {
        FULL,         // scheduler thread + per-query sub-queues + ranking + slot array
        GENERAL_ONLY, // plain shared lock-free queue (blocking pool)
    };

    explicit MultiCoreTaskQueue(int core_size, Mode mode = Mode::FULL);

#ifndef BE_TEST
    ~MultiCoreTaskQueue();
    // Get the next task for the worker `core_id`.
    PipelineTaskSPtr take(int core_id);
#else
    virtual ~MultiCoreTaskQueue();
    virtual PipelineTaskSPtr take(int core_id);
#endif

    void close();

    // `core_id` is accepted for API compatibility but placement is global: a task goes
    // to its owning query's sub-queue (full mode) or the shared queue (degenerate).
    Status push_back(PipelineTaskSPtr task);
    Status push_back(PipelineTaskSPtr task, int core_id);

    // Charge executed CPU time to the owning query's global counter (drives
    // attained-service ranking), then release the in-flight slot the task held.
    void update_statistics(PipelineTask* task, int64_t time_spent);

    // Release the in-flight slot a task held without charging runtime. Used when a
    // dequeued task turns out to be already running on another worker and is
    // re-queued without being executed.
    void release_task(PipelineTask* task);

    // QueryContext is being destroyed. Posts QUERY_TERMINATED; the scheduler drops the
    // query from the ranking and reclaims the QueryState once nothing of it is pending
    // or in flight. No-op in degenerate mode.
    void notify_query_terminated(const TUniqueId& query_id);

    int cores() const { return _core_size; }

    // Test hook: block until the scheduler thread has fully processed every inbox
    // message posted before this call and completed the ranking/publication of that
    // pass. No-op in degenerate mode or after close().
    void wait_scheduler_settled_for_test();

    // Test hook: number of live per-query states (observes teardown/resurrection).
    size_t registry_size_for_test() const;

    // Test hook: the slot `query_id` occupies in the published array, or -1 when the
    // ranking did not give it one. Call after wait_scheduler_settled_for_test().
    int slot_of_query_for_test(const TUniqueId& query_id) const;

    // Test hook: size of the published array, i.e. the configured slot count in effect.
    int slot_count_for_test() const;

    // Test hook: the slot worker `worker_id` serves under the published slot count and
    // LasSlotPolicy::FIXED. Independent of the policy actually in effect.
    int slot_of_worker_for_test(int worker_id) const;

    // Test hook: how many times the array has been published, which is what tells a
    // steady state (no publication) from a churning one.
    uint64_t slot_epoch_for_test() const { return _slot_epoch.load(); }

protected:
    // Single-attempt take with an explicit wait timeout. Returns nullptr if no task
    // becomes available within `timeout_ms` (or the queue is closed).
    PipelineTaskSPtr _take(int worker_id, uint32_t timeout_ms);

private:
    // ------------------------------------------------------------------
    // Per-query state (full mode only). Created lazily by the enqueue path and owned by
    // the registry, the scheduler's ranking vector and the published slot snapshots
    // alike: whoever holds the last reference destroys it, which is what makes the
    // pointer a worker reads out of a snapshot always safe. The registry entry - the
    // only way a producer can find the state - is dropped by the scheduler thread, under
    // the exclusive registry lock, once the query has been terminated, compacted out of
    // `_queries` (`!in_sched`) and has nothing pending or in flight.
    // ------------------------------------------------------------------
    struct QueryState {
        QueryState(moodycamel::ConcurrentQueue<PipelineTaskSPtr>& shared_queue, TUniqueId id)
                : query_id(id), token(shared_queue) {}

        // ---- Cold part: written rarely ----
        // Registry key (query_id()). The all-zero sentinel buckets tasks with no
        // QueryContext (e.g. RevokableTask).
        const TUniqueId query_id;
        // This query's sub-queue in the shared queue. Enqueue through it is guarded by
        // `enqueue_mutex` (explicit producers are single-producer); dequeue via
        // try_dequeue_from_producer is lock-free and multi-consumer safe.
        moodycamel::ProducerToken token;
        // Serializes producers enqueueing through `token`. Always acquired while
        // already holding the shared registry lock, which is what fences producers
        // against teardown (teardown runs under the exclusive registry lock).
        std::mutex enqueue_mutex;

        // ---- Scheduler-thread-only bookkeeping (no atomics needed) ----
        // True while this query is in `_queries`. Compact is the only thing that clears
        // it; `_registry.erase` is forbidden while this is set.
        bool in_sched = false;
        // True after QUERY_TERMINATED. The object stays alive (and, until compact, in
        // `_queries`) so nothing has to be unpublished synchronously.
        bool terminated = false;
        // Membership flag for _destroy_candidates (dedup).
        bool in_destroy_candidates = false;

        // ---- Hot part: separate cacheline, touched by workers ----
        // CPU time executed in this pool (per-pool statistic; the authoritative
        // ranking counter is the query-global one behind add_query_runtime_ns()).
        alignas(64) std::atomic<uint64_t> cpu_time_ns {0};
        // Snapshot of query-global attained service, refreshed whenever a task is
        // in hand (enqueue / charge). The scheduler sorts `_queries` by this.
        std::atomic<uint64_t> attained_ns {0};
        // Tasks of this query currently between dequeue and release in this pool.
        // Ordering contract with `pending_approx` (both seq_cst): a dequeuing worker
        // increments in_flight BEFORE decrementing pending_approx, so at every instant
        // a live task is visible in at least one of the two counters (pending_approx
        // is incremented before the physical enqueue and decremented only after
        // in_flight was incremented).
        std::atomic<int> in_flight {0};
        // Approximate sub-queue length; invariant: >= real length.
        std::atomic<int> pending_approx {0};
        // "Nothing here right now": armed by the releaser that drove in_flight to
        // zero while pending_approx was zero; disarmed by producers on enqueue, which
        // is what makes the next push post NEW_QUERY and so wake the scheduler instead
        // of waiting up to a tick for the query to be ranked again. Not a reclaim
        // trigger.
        std::atomic<bool> idle {false};
        // Mirror of the owning query's runnable task count (tasks submitted and not yet
        // finished, minus those parked on a dependency, across every fragment of the
        // query and every pool). Refreshed by producers on enqueue and by workers on
        // release, since the scheduler thread must never dereference a QueryContext.
        // Stays 0 for the sentinel bucket, whose tasks have no QueryContext.
        std::atomic<int> active_tasks {0};
    };
    using QueryStatePtr = std::shared_ptr<QueryState>;

    // ------------------------------------------------------------------
    // The shared slot array, published as an immutable snapshot. `slots` is always
    // exactly as long as the configured slot count - the tail is simply empty when
    // fewer queries are ranked - so the fixed worker/slot mapping does not shift as
    // queries come and go. The policy travels with it so a worker never mixes a policy
    // from one pass with slots from another.
    // ------------------------------------------------------------------
    struct SlotTable {
        std::vector<QueryStatePtr> slots;
        LasSlotPolicy policy = LasSlotPolicy::ORDERED;
    };
    using SlotTablePtr = std::shared_ptr<const SlotTable>;

    // A worker's private view of the array: the last epoch it observed and the snapshot
    // it took then. Holding the snapshot is what keeps the listed QueryStates alive
    // while the worker may still touch them. Cache-aligned because every worker writes
    // its own entry.
    struct alignas(64) WorkerLocal {
        uint64_t seen_epoch = 0;
        SlotTablePtr table;
    };

    // ------------------------------------------------------------------
    // Scheduler inbox. Every query reference travels as a TUniqueId and is re-resolved
    // through the registry at processing time, so a message can never dangle across
    // QueryContext destruction.
    // ------------------------------------------------------------------
    struct SchedulerMessage {
        enum class Type {
            NEW_QUERY,        // enqueue path created or revived query_id
            QUERY_TERMINATED, // QueryContext destructor: reclaim once drained
            SYNC,             // test hook: fulfilled at the end of the draining pass
        };
        Type type;
        TUniqueId query_id; // NEW_QUERY / TERMINATED
        std::shared_ptr<std::promise<void>> sync;
    };

    // ---- enqueue / dequeue helpers ----
    Status _push(PipelineTaskSPtr task);
    PipelineTaskSPtr _try_take_once(int worker_id);
    // Refresh the worker's cached slot snapshot if the scheduler published a new one,
    // and return it (null before the first publication).
    const SlotTable* _refresh_slots(int worker_id);
    // Dequeue one task from `qs`'s sub-queue, with the per-query accounting. False when
    // `qs` is empty or null.
    bool _try_take_from(const QueryStatePtr& qs, PipelineTaskSPtr& task);
    void _release_in_flight(PipelineTask* task, bool charge, int64_t time_spent);
    // Republishes the query's active task count into `qs`. Called by producers and
    // workers, which are the only threads allowed to read it off a task (they resolve
    // through the QueryContext, which the scheduler must not touch).
    void _refresh_query_mirror(QueryState* qs, const PipelineTask* task);
    void _post_message(SchedulerMessage msg);
    void _notify_workers(bool all);

    // ---- scheduler thread ----
    void _scheduler_loop();
    void _handle_message(SchedulerMessage& msg,
                         std::vector<std::shared_ptr<std::promise<void>>>& syncs);
    QueryStatePtr _resolve(const TUniqueId& query_id);
    static bool _is_sentinel(const TUniqueId& query_id) {
        return query_id.hi == 0 && query_id.lo == 0;
    }
    void _add_to_sched(QueryStatePtr node);
    void _try_teardown();
    // Compact, re-rank by attained service and publish the slot array if it changed.
    void _rank_and_publish();
    void _publish(SlotTablePtr table);
    // The published snapshot, for the test hooks. Null before the first publication.
    SlotTablePtr _published_for_test() const;

    bool _worker_in_range(int worker_id) const {
        return worker_id >= 0 && worker_id < static_cast<int>(_worker_local.size());
    }

    const int _core_size;
    const Mode _mode;
    std::atomic<bool> _closed {false};

    // The shared task queue. In full mode it is partitioned by per-query explicit
    // ProducerTokens; tokenless try_dequeue (fallback) sees all sub-queues.
    moodycamel::ConcurrentQueue<PipelineTaskSPtr> _queue;

    // Top-priority queue for inelastic tasks (full mode; see "inelastic first" in the
    // class comment). Tokenless multi-producer/multi-consumer: submits come from RPC
    // threads, dependency wakeups and workers alike, and every worker polls it first.
    moodycamel::ConcurrentQueue<PipelineTaskSPtr> _inelastic_queue;

    // Registry of live QueryStates (full mode). Producers hold the shared lock for the
    // whole enqueue; the fallback dequeue and release paths hold it while mutating a
    // query's counters; the scheduler holds the exclusive lock to verify-and-erase at
    // teardown (creation is on the enqueue path). A looked-up raw pointer is therefore
    // valid for as long as the shared lock is held.
    mutable std::shared_mutex _registry_mutex;
    std::unordered_map<TUniqueId, QueryStatePtr> _registry;

    // Published slot array. Written only by the scheduler thread, under `_slot_mutex`
    // together with the epoch bump; read by workers under the same mutex, but only when
    // the epoch moved, so the steady-state cost on the dequeue path is one relaxed load.
    mutable std::mutex _slot_mutex;
    SlotTablePtr _published_slots;
    std::atomic<uint64_t> _slot_epoch {0};

    // Per-worker cached view of the array (full mode), indexed by core_id.
    std::vector<WorkerLocal> _worker_local;

    // Schedulable queries (scheduler-thread-only). Appended on NEW_QUERY; terminated
    // entries stay until the next compact. While a query is here (`in_sched`),
    // `_registry.erase` is forbidden.
    std::vector<QueryStatePtr> _queries;
    // Queries pending destroy after QUERY_TERMINATED (scheduler-thread-only).
    std::vector<QueryStatePtr> _destroy_candidates;

    // Inbox.
    std::mutex _inbox_mutex;
    std::condition_variable _inbox_cv;
    std::deque<SchedulerMessage> _inbox;

    // Worker parking: producers/scheduler notify (under _park_mutex, bumping the
    // epoch) only when the waiter count is nonzero, so the common enqueue path does no
    // syscall. Workers snapshot the epoch before their dequeue attempt and skip the
    // wait if it moved, bounding lost-wakeup staleness; the wait itself is also
    // time-bounded.
    std::mutex _park_mutex;
    std::condition_variable _park_cv;
    std::atomic<int> _park_waiters {0};
    uint64_t _park_epoch = 0; // guarded by _park_mutex

    std::atomic<size_t> _total_task_size = 0;

    std::thread _scheduler_thread;

    static constexpr auto SCHEDULER_TICK_MS = 20;
    static constexpr auto WAIT_CORE_TASK_TIMEOUT_MS = 100;
};
#include "common/compile_check_end.h"
} // namespace doris
