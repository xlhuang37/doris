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

#include <array>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <future>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gen_cpp/Types_types.h>

#include "common/status.h"
#include "exec/pipeline/pipeline_task.h"
#include "util/hash_util.hpp"

namespace doris {
#include "common/compile_check_begin.h"

// A push-based, query-granular task queue shared by all workers of one pipeline
// scheduler (one per workload group).
//
// Core design (full mode, used by the "simple" CPU pool):
//   - All runnable PipelineTasks live in one shared moodycamel::ConcurrentQueue,
//     partitioned by per-query explicit ProducerTokens: each task is enqueued exactly
//     once, through its owning query's token (enqueue serialized by a per-query mutex,
//     since explicit producers are single-producer). Sub-queues are drained lock-free
//     via try_dequeue_from_producer.
//   - A central scheduler thread owns a query-granular MLFQ (strict absolute priority
//     between levels, driven by the query-global CPU runtime counter) and push-assigns
//     workers to queries by writing per-worker, cache-aligned assignment slots. Only
//     the scheduler writes a slot; only the owning worker reads it.
//   - Core allocation is greedy first-come-first-served: queries are visited in strict
//     level order and, within a level, in arrival order, and each one takes as many of
//     the pool's workers as it can use before the next is considered. A query's demand
//     is its active task count (tasks created and not yet finalized, so blocked tasks
//     count too), which is what lets a query that is momentarily empty but about to
//     unblock keep its cores. Queries reached after the workers run out hold no
//     assigned worker and are served only by the fallback; the MLFQ demotes whoever is
//     hogging cores, which re-links it behind the later arrivals of the deeper level.
//   - Workers obey their slot: dequeue from the assigned query's sub-queue. When the
//     sub-queue is empty they fall back to a tokenless try_dequeue on the shared queue
//     (work-conserving, priority-blind relief valve), re-checking the slot after every
//     task so scheduler decisions take effect within one execution slice.
//   - "Inelastic first": a task whose pipeline has exactly one task cannot be sped up
//     by parallelism, so queueing it behind elastic work only lengthens the query's
//     critical path. Such tasks keep full per-query bookkeeping (pending/in-flight
//     counters, idle detection, teardown) but bypass the per-query sub-queue and go
//     into a dedicated shared queue that every worker drains before anything else,
//     ahead of its assignment and of the MLFQ entirely.
//   - Workers notify the scheduler through a mutex-guarded inbox (attach/detach acks,
//     level demotions, new queries, query termination); the scheduler sleeps on a
//     condition variable with a timer tick and reacts to events instead of polling
//     state.
//   - Teardown happens only after QueryContext destruction posts QUERY_TERMINATED and
//     every assigned worker has acked detaching (workers_attached == 0). Temporary
//     emptiness while the query is still alive does not reclaim the node. Tasks with
//     no QueryContext (RevokableTask) share a sentinel id and live until queue close.
//
// Degenerate mode (used by the "blocking" pool, whose workers sit inside blocking
// execute() calls and cannot honor the "re-check the slot every slice" invariant):
// no scheduler thread, no per-query state, no MLFQ - just the shared lock-free queue
// plus worker parking. Runtime is still charged to the query-global counter so MLFQ
// accounting in the other pools is unaffected.
//
// The public interface is kept identical to the previous implementation so the
// scheduler/worker loop (TaskScheduler::_do_work) is unchanged.
class MultiCoreTaskQueue {
public:
    enum class Mode {
        FULL,         // scheduler thread + per-query sub-queues + MLFQ + worker assignment
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

    // Charge executed CPU time to the owning query's global counter (drives MLFQ
    // demotion), then release the in-flight slot the task held.
    void update_statistics(PipelineTask* task, int64_t time_spent);

    // Release the in-flight slot a task held without charging runtime. Used when a
    // dequeued task turns out to be already running on another worker and is
    // re-queued without being executed.
    void release_task(PipelineTask* task);

    // QueryContext is being destroyed. Posts QUERY_TERMINATED; the scheduler reclaims
    // the QueryState once workers_attached == 0. No-op in degenerate mode.
    void notify_query_terminated(const TUniqueId& query_id);

    int cores() const { return _core_size; }

    // Test hook: block until the scheduler thread has fully processed every inbox
    // message posted before this call and completed the rebalance/dispatch of that
    // pass. No-op in degenerate mode or after close().
    void wait_scheduler_settled_for_test();

    // Test hook: number of live per-query states (observes teardown/resurrection).
    size_t registry_size_for_test() const;

    // Test hook: how many worker slots currently point at `query_id`, i.e. how many
    // cores the last rebalance gave it. Call after wait_scheduler_settled_for_test().
    int assigned_workers_for_test(const TUniqueId& query_id) const;

protected:
    static constexpr int SUB_QUEUE_LEVEL = 4;

    // Single-attempt take with an explicit wait timeout. Returns nullptr if no task
    // becomes available within `timeout_ms` (or the queue is closed).
    PipelineTaskSPtr _take(int worker_id, uint32_t timeout_ms);

private:
    // ------------------------------------------------------------------
    // Per-query state (full mode only). Created lazily by the enqueue path; destroyed
    // exclusively by the scheduler thread, under the exclusive registry lock, once the
    // query has been terminated and every assigned worker has acked detaching.
    // ------------------------------------------------------------------
    struct QueryState {
        QueryState(moodycamel::ConcurrentQueue<PipelineTaskSPtr>& shared_queue,
                   TUniqueId id, int initial_level)
                : query_id(id), token(shared_queue), level(initial_level) {}

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
        int workers_attached = 0; // exact, via the acked attach/detach protocol
        bool linked = false;      // linked into an MLFQ level list
        int linked_level = 0;
        std::list<QueryState*>::iterator pos {};
        // True after QUERY_TERMINATED. Erased only when this is set and
        // workers_attached == 0.
        bool terminated = false;
        // Membership flag for _destroy_candidates (dedup).
        bool in_destroy_candidates = false;
        // Rebalance scratch (valid only within one rebalance pass).
        int rr_grant = 0;

        // ---- Hot part: separate cacheline, touched by workers ----
        // CPU time executed in this pool (per-pool statistic; the authoritative
        // demotion counter is the query-global one behind add_query_runtime_ns()).
        alignas(64) std::atomic<uint64_t> cpu_time_ns {0};
        // MLFQ level; CAS'd upward by workers purely as a duplicate-suppression guard
        // so exactly one LEVEL_DEMOTED message is posted per crossing. The scheduler
        // remains the only entity that moves MLFQ nodes or reassigns workers.
        std::atomic<int> level;
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
        // zero while pending_approx was zero; disarmed by producers on enqueue.
        // Assigned workers self-detach when they observe this. Not a reclaim trigger.
        std::atomic<bool> idle {false};
        // Mirror of the owning query's runnable task count (tasks submitted and not yet
        // finished, minus those parked on a dependency, across every fragment of the
        // query and every pool). Refreshed by producers on enqueue and by workers on
        // release, since the scheduler thread must never dereference a QueryContext.
        // Stays 0 for the sentinel bucket, whose tasks have no QueryContext.
        std::atomic<int> active_tasks {0};
    };

    // ------------------------------------------------------------------
    // Worker control blocks. `WorkerSlot` is written only by the scheduler thread and
    // read only by the owning worker; `WorkerLocal` is private to the worker. Each is
    // cache-aligned to prevent false sharing.
    //
    // Attach/detach protocol (exact workers_attached counting):
    //   - Every scheduler write bumps `seq` (so re-assigning the same pointer after a
    //     self-detach is still observed) and there is AT MOST ONE unacked write per
    //     worker: the scheduler never writes a slot again until the worker acks the
    //     previous write with an ACK message carrying that seq.
    //   - The worker acks every observed seq change. The ack carries the query it was
    //     attached to at the moment of the change (null if it had already
    //     self-detached), which is what the scheduler decrements.
    //   - A worker that finds its assigned query idle self-detaches: it posts DETACHED
    //     and stops using the pointer, without touching the slot.
    //   Each attach (the scheduler increments workers_attached at write time) is
    //   balanced by exactly one decrement (ACK-with-previous or DETACHED), and the
    //   decrement is posted only after the worker stopped using the pointer - which
    //   makes `workers_attached == 0` a safe reclamation gate.
    // ------------------------------------------------------------------
    struct alignas(64) WorkerSlot {
        std::atomic<QueryState*> assigned {nullptr};
        std::atomic<uint64_t> seq {0};
    };
    struct alignas(64) WorkerLocal {
        uint64_t seen_seq = 0;
        QueryState* attached = nullptr;
        // The worker observed its query idle, posted DETACHED, and stopped using
        // `attached`. Reset on the next observed seq change.
        bool detached = false;
    };
    // Scheduler-thread-only view of each worker.
    struct WorkerSched {
        QueryState* target = nullptr; // value of the last slot write
        uint64_t written_seq = 0;
        bool acked = true;          // the last slot write has been acked
        bool self_detached = false; // a DETACHED for `target` was processed
        bool rr_kept = false;       // rebalance scratch: kept in place this pass
    };

    // ------------------------------------------------------------------
    // Scheduler inbox. ACK/DETACHED may carry a QueryState pointer because the
    // worker's attachment keeps `workers_attached` nonzero until the message is
    // processed, which blocks teardown. All other query references travel as
    // TUniqueId and are re-resolved through the registry at processing time, so a
    // message can never dangle across QueryContext destruction.
    // ------------------------------------------------------------------
    struct SchedulerMessage {
        enum class Type {
            ACK,              // worker observed slot write `seq`; `state` = what it was
                              // attached to before (null if it had self-detached)
            DETACHED,         // worker self-detached from `state` (observed it idle)
            LEVEL_DEMOTED,    // CAS winner reports query_id crossed a level threshold
            NEW_QUERY,        // enqueue path created or revived query_id
            QUERY_TERMINATED, // QueryContext destructor: reclaim once detached
            SYNC,             // test hook: fulfilled at the end of the draining pass
        };
        Type type;
        QueryState* state = nullptr; // ACK / DETACHED only
        TUniqueId query_id;          // LEVEL_DEMOTED / NEW_QUERY / TERMINATED
        int worker_id = -1;
        uint64_t seq = 0;
        std::shared_ptr<std::promise<void>> sync;
    };

    // ---- enqueue / dequeue helpers ----
    Status _push(PipelineTaskSPtr task);
    PipelineTaskSPtr _try_take_once(int worker_id);
    void _check_assignment(int worker_id);
    void _release_in_flight(PipelineTask* task, bool charge, int64_t time_spent);
    // Republishes the query's active task count into `qs` and tells the scheduler if it
    // moved. Called by producers and workers, which are the only threads allowed to
    // read it off a task.
    void _refresh_active_tasks(QueryState* qs, const PipelineTask* task);
    void _post_message(SchedulerMessage msg);
    void _notify_workers(bool all);

    // ---- scheduler thread ----
    void _scheduler_loop();
    void _handle_message(SchedulerMessage& msg,
                         std::vector<std::shared_ptr<std::promise<void>>>& syncs);
    QueryState* _resolve(const TUniqueId& query_id);
    static bool _is_sentinel(const TUniqueId& query_id) {
        return query_id.hi == 0 && query_id.lo == 0;
    }
    void _link(QueryState* node, int level);
    void _unlink(QueryState* node);
    void _try_teardown();
    void _rebalance_and_dispatch();
    void _write_assignment(int worker_id, QueryState* value);

    int _compute_level(uint64_t runtime) const;
    bool _worker_in_range(int worker_id) const {
        return worker_id >= 0 && worker_id < static_cast<int>(_worker_slots.size());
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
    // query's counters; the scheduler holds the exclusive lock to create-check nothing
    // (creation is on the enqueue path) and to verify-and-erase at teardown. A
    // looked-up pointer is therefore valid for as long as the shared lock is held, or
    // for as long as the holder is visible in `in_flight`/`workers_attached`.
    mutable std::shared_mutex _registry_mutex;
    std::unordered_map<TUniqueId, std::unique_ptr<QueryState>> _registry;

    // Worker control blocks (full mode), indexed by core_id.
    std::vector<WorkerSlot> _worker_slots;
    std::vector<WorkerLocal> _worker_local;
    std::vector<WorkerSched> _worker_sched; // scheduler-thread-only

    // MLFQ level lists (scheduler-thread-only). Each list is in arrival order: a query
    // is appended when it joins the level (first enqueue, or the re-link performed on
    // demotion) and keeps its place until it leaves, so dealing is FIFO.
    std::array<std::list<QueryState*>, SUB_QUEUE_LEVEL> _levels;
    // Queries pending destroy after QUERY_TERMINATED (scheduler-thread-only).
    std::vector<QueryState*> _destroy_candidates;
    // Queries that received a grant in the current rebalance pass, in (level, arrival)
    // order. Reused across passes to avoid reallocating (scheduler-thread-only).
    std::vector<QueryState*> _granted_queries;
    // A rebalance runs only when something it depends on changed: a query was created
    // or revived, demoted, or terminated, or a worker came free. Set once at startup so
    // the first pass always runs (scheduler-thread-only).
    bool _rebalance_dirty = true;
    // The last pass had grants it could not place because every worker still owed an
    // ack. Keeps the dirty flag set so the next pass retries (scheduler-thread-only).
    bool _grants_pending = false;
    // Some query's `active_tasks` mirror moved, which changes its demand without any
    // scheduler message. Set by producers/workers, consumed by the scheduler.
    std::atomic<bool> _demand_dirty {false};

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
    // Workers a query is granted while it has anything to run. Strict level order still
    // applies: a higher-priority query takes its share first and lower levels divide
    // whatever is left, so this is a per-query cap rather than a reservation.
    static constexpr int TARGET_WORKERS_PER_QUERY = 8;
    // 2.8s, 10s, 25s of query-global runtime, same thresholds as before.
    static constexpr uint64_t QUEUE_LEVEL_LIMIT[SUB_QUEUE_LEVEL - 1] = {
            2800000000ULL, 10000000000ULL, 25000000000ULL};
    static constexpr auto WAIT_CORE_TASK_TIMEOUT_MS = 100;
};
#include "common/compile_check_end.h"
} // namespace doris
