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

#include "exec/pipeline/task_queue.h"

// IWYU pragma: no_include <bits/chrono.h>
#include <algorithm>
#include <chrono> // IWYU pragma: keep
#include <gen_cpp/Types_types.h>
#include <memory>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "exec/pipeline/pipeline_task.h"
#include "util/thread.h"

namespace doris {
#include "common/compile_check_begin.h"

MultiCoreTaskQueue::MultiCoreTaskQueue(int core_size, Mode mode)
        : _core_size(core_size), _mode(mode) {
    if (_mode == Mode::FULL) {
        auto worker_count = static_cast<size_t>(std::max(core_size, 1));
        _worker_slots = std::vector<WorkerSlot>(worker_count);
        _worker_local = std::vector<WorkerLocal>(worker_count);
        _worker_sched = std::vector<WorkerSched>(worker_count);
        _scheduler_thread = std::thread([this]() { _scheduler_loop(); });
    }
}

MultiCoreTaskQueue::~MultiCoreTaskQueue() {
    close();
}

// ---------------------------------------------------------------------------
// Enqueue path
// ---------------------------------------------------------------------------

Status MultiCoreTaskQueue::push_back(PipelineTaskSPtr task) {
    return _push(std::move(task));
}

Status MultiCoreTaskQueue::push_back(PipelineTaskSPtr task, int /*core_id*/) {
    // `core_id` no longer pins a task to a shard; placement is by owning query.
    return _push(std::move(task));
}

Status MultiCoreTaskQueue::_push(PipelineTaskSPtr task) {
    if (_closed.load()) {
        return Status::InternalError("WorkTaskQueue closed");
    }
    task->put_in_runnable_queue();

    if (_mode == Mode::GENERAL_ONLY) {
        if (!_queue.enqueue(std::move(task))) {
            return Status::InternalError("WorkTaskQueue enqueue failed");
        }
        _total_task_size.fetch_add(1);
        DorisMetrics::instance()->pipeline_task_queue_size->increment(1);
        _notify_workers(false);
        return Status::OK();
    }

    const TUniqueId query_id = task->query_id();
    // "Inelastic first": single-task pipelines bypass the per-query sub-queue and go
    // into the dedicated top-priority queue. All per-query bookkeeping is identical,
    // so idle detection and teardown are oblivious to which queue the task sits in.
    const bool inelastic = task->is_inelastic();
    bool enqueued = false;
    bool revived = false;
    bool created = false;
    // Bookkeeping + physical enqueue for one QueryState. Caller holds the registry
    // lock (shared or exclusive) and the per-query enqueue mutex.
    auto do_enqueue = [&](QueryState* qs) -> bool {
        qs->pending_approx.fetch_add(1);
        revived = qs->idle.exchange(false);
        qs->attained_ns.store(task->query_runtime_ns(), std::memory_order_relaxed);
        _refresh_query_mirror(qs, task.get());
        bool ok = inelastic ? _inelastic_queue.enqueue(std::move(task))
                            : _queue.enqueue(qs->token, std::move(task));
        if (!ok) {
            qs->pending_approx.fetch_sub(1);
        }
        return ok;
    };
    {
        // Fast path: the query already has a state. The shared registry lock is held
        // for the whole enqueue, which fences against teardown (exclusive lock).
        std::shared_lock<std::shared_mutex> rlock(_registry_mutex);
        if (_closed.load()) {
            return Status::InternalError("WorkTaskQueue closed");
        }
        auto it = _registry.find(query_id);
        if (it != _registry.end()) {
            QueryState* qs = it->second.get();
            std::lock_guard<std::mutex> elock(qs->enqueue_mutex);
            if (!do_enqueue(qs)) {
                return Status::InternalError("WorkTaskQueue enqueue failed");
            }
            enqueued = true;
        }
    }
    if (!enqueued) {
        // Slow path: first task of this query in this pool (or the query was torn
        // down and this task resurrects it).
        std::unique_lock<std::shared_mutex> wlock(_registry_mutex);
        if (_closed.load()) {
            return Status::InternalError("WorkTaskQueue closed");
        }
        auto& slot = _registry[query_id];
        if (slot == nullptr) {
            slot = std::make_unique<QueryState>(_queue, query_id);
            created = true;
        }
        QueryState* qs = slot.get();
        // The exclusive registry lock already excludes all other producers, but take
        // the enqueue mutex anyway (uncontended) to keep the locking rule uniform.
        std::lock_guard<std::mutex> elock(qs->enqueue_mutex);
        if (!do_enqueue(qs)) {
            return Status::InternalError("WorkTaskQueue enqueue failed");
        }
    }
    _total_task_size.fetch_add(1);
    DorisMetrics::instance()->pipeline_task_queue_size->increment(1);
    if (created || revived) {
        SchedulerMessage msg;
        msg.type = SchedulerMessage::Type::NEW_QUERY;
        msg.query_id = query_id;
        _post_message(std::move(msg));
    }
    _notify_workers(false);
    return Status::OK();
}

// ---------------------------------------------------------------------------
// Worker path
// ---------------------------------------------------------------------------

PipelineTaskSPtr MultiCoreTaskQueue::take(int core_id) {
    return _take(core_id, WAIT_CORE_TASK_TIMEOUT_MS);
}

PipelineTaskSPtr MultiCoreTaskQueue::_take(int worker_id, uint32_t timeout_ms) {
    // Genuinely, this park semaphore thing is quite dumb. It should be
    // replaced with a semaphore design similar to DuckDB.
    PipelineTaskSPtr task = _try_take_once(worker_id);
    if (!task && !_closed.load() && timeout_ms > 0) {
        // Park until a producer or the scheduler signals, bounded by `timeout_ms`.
        // The epoch is sampled under the lock and re-checked before sleeping so a
        // notification between the failed dequeue below and the wait is not lost; a
        // producer racing ahead of the waiter registration is bounded by the timeout.
        std::unique_lock<std::mutex> lk(_park_mutex);
        uint64_t epoch = _park_epoch;
        _park_waiters.fetch_add(1);
        lk.unlock();
        task = _try_take_once(worker_id);
        lk.lock();
        if (!task && _park_epoch == epoch && !_closed.load()) {
            _park_cv.wait_for(lk, std::chrono::milliseconds(timeout_ms));
        }
        _park_waiters.fetch_sub(1);
        lk.unlock();
        if (!task) {
            task = _try_take_once(worker_id);
        }
    }
    if (task) {
        task->pop_out_runnable_queue();
    }
    return task;
}

void MultiCoreTaskQueue::_check_assignment(int worker_id) {
    WorkerSlot& slot = _worker_slots[worker_id];
    WorkerLocal& local = _worker_local[worker_id];
    uint64_t seq = slot.seq.load(std::memory_order_acquire);
    if (seq == local.seen_seq) {
        return;
    }
    // There is at most one unacked slot write, so `assigned` is exactly the value
    // published together with `seq`.
    QueryState* next = slot.assigned.load(std::memory_order_relaxed);
    QueryState* prev = local.detached ? nullptr : local.attached;
    local.attached = next;
    local.seen_seq = seq;
    local.detached = false;
    // The ack is posted after this worker has stopped touching `prev`, which is what
    // lets the scheduler treat workers_attached == 0 as a reclamation gate.
    SchedulerMessage msg;
    msg.type = SchedulerMessage::Type::ACK;
    msg.state = prev;
    msg.worker_id = worker_id;
    msg.seq = seq;
    _post_message(std::move(msg));
}

PipelineTaskSPtr MultiCoreTaskQueue::_try_take_once(int worker_id) {
    if (_closed.load()) {
        return nullptr;
    }
    PipelineTaskSPtr task;
    if (_mode == Mode::FULL) {
        const bool in_range = _worker_in_range(worker_id);
        if (in_range) {
            // Ack any pending slot write first so scheduler decisions are never
            // delayed by more than one execution slice.
            _check_assignment(worker_id);
        }
        // "Inelastic first": single-task pipelines outrank everything, including the
        // worker's own assignment and attained-service ranking. Accounting is the
        // same as the tokenless fallback below (the task was never in a per-query
        // sub-queue).
        if (_inelastic_queue.try_dequeue(task)) {
            {
                std::shared_lock<std::shared_mutex> rlock(_registry_mutex);
                auto it = _registry.find(task->query_id());
                if (it != _registry.end()) {
                    // in_flight up BEFORE pending down: a live task is always visible
                    // in at least one of the two counters (see header contract).
                    it->second->in_flight.fetch_add(1);
                    it->second->pending_approx.fetch_sub(1);
                }
            }
            _total_task_size.fetch_sub(1);
            DorisMetrics::instance()->pipeline_task_queue_size->increment(-1);
            return task;
        }
        if (in_range) {
            WorkerLocal& local = _worker_local[worker_id];
            QueryState* qs = local.attached;
            if (qs != nullptr && !local.detached) {
                if (_queue.try_dequeue_from_producer(qs->token, task)) {
                    // in_flight up BEFORE pending down: a live task is always visible
                    // in at least one of the two counters (see header contract).
                    qs->in_flight.fetch_add(1);
                    qs->pending_approx.fetch_sub(1);
                    _total_task_size.fetch_sub(1);
                    DorisMetrics::instance()->pipeline_task_queue_size->increment(-1);
                    return task;
                }
                if (qs->idle.load()) {
                    // The assigned query has nothing queued and nothing in flight:
                    // stop serving it and tell the scheduler. The slot itself is only
                    // ever rewritten by the scheduler.
                    local.detached = true;
                    SchedulerMessage msg;
                    msg.type = SchedulerMessage::Type::DETACHED;
                    msg.state = qs;
                    msg.worker_id = worker_id;
                    _post_message(std::move(msg));
                }
            }
        }
    }
    // Fallback: work-conserving, priority-blind dequeue across all sub-queues.
    if (!_queue.try_dequeue(task)) {
        return nullptr;
    }
    if (_mode == Mode::FULL) {
        std::shared_lock<std::shared_mutex> rlock(_registry_mutex);
        auto it = _registry.find(task->query_id());
        if (it != _registry.end()) {
            it->second->in_flight.fetch_add(1);
            it->second->pending_approx.fetch_sub(1);
        }
    }
    _total_task_size.fetch_sub(1);
    DorisMetrics::instance()->pipeline_task_queue_size->increment(-1);
    return task;
}

// ---------------------------------------------------------------------------
// Accounting / release
// ---------------------------------------------------------------------------

void MultiCoreTaskQueue::update_statistics(PipelineTask* task, int64_t time_spent) {
    _release_in_flight(task, /*charge=*/true, time_spent);
}

void MultiCoreTaskQueue::release_task(PipelineTask* task) {
    _release_in_flight(task, /*charge=*/false, 0);
}

void MultiCoreTaskQueue::_refresh_query_mirror(QueryState* qs, const PipelineTask* task) {
    qs->active_tasks.store(task->active_task_num(), std::memory_order_relaxed);
    qs->worker_cap.store(task->query_worker_cap(), std::memory_order_relaxed);
}

void MultiCoreTaskQueue::_release_in_flight(PipelineTask* task, bool charge, int64_t time_spent) {
    auto charged_ns = static_cast<uint64_t>(std::max<int64_t>(time_spent, 0));
    if (charge) {
        // Charge the executed CPU time to the owning query's global counter. This
        // counter is shared by all of the query's tasks (across fragments, instances
        // and cores) and across the pipeline/scan schedulers, and drives
        // attained-service ranking. For tasks without a query counter (e.g.
        // RevokableTask) the charge is a no-op and they stay at attained 0.
        task->add_query_runtime_ns(charged_ns);
    }
    if (_mode != Mode::FULL) {
        return;
    }
    const TUniqueId query_id = task->query_id();
    std::shared_lock<std::shared_mutex> rlock(_registry_mutex);
    auto it = _registry.find(query_id);
    if (it == _registry.end()) {
        return;
    }
    QueryState* qs = it->second.get();
    // Both ways a task stops wanting a core - blocking on a dependency and finishing -
    // happen inside the run that is ending here, so the mirror picks up the decrement.
    _refresh_query_mirror(qs, task);
    if (charge) {
        qs->cpu_time_ns.fetch_add(charged_ns, std::memory_order_relaxed);
        qs->attained_ns.store(task->query_runtime_ns(), std::memory_order_relaxed);
    }
    int remaining = qs->in_flight.fetch_sub(1) - 1;
    if (remaining == 0 && qs->pending_approx.load() == 0) {
        // Arm idle so assigned workers self-detach. Reclaim waits for terminate.
        qs->idle.exchange(true);
    }
}

// ---------------------------------------------------------------------------
// Notification plumbing
// ---------------------------------------------------------------------------

void MultiCoreTaskQueue::_post_message(SchedulerMessage msg) {
    if (_mode != Mode::FULL) {
        return;
    }
    {
        std::lock_guard<std::mutex> lk(_inbox_mutex);
        _inbox.push_back(std::move(msg));
    }
    _inbox_cv.notify_one();
}

void MultiCoreTaskQueue::notify_query_terminated(const TUniqueId& query_id) {
    if (_mode != Mode::FULL || _is_sentinel(query_id)) {
        return;
    }
    SchedulerMessage msg;
    msg.type = SchedulerMessage::Type::QUERY_TERMINATED;
    msg.query_id = query_id;
    _post_message(std::move(msg));
}

void MultiCoreTaskQueue::_notify_workers(bool all) {
    if (_park_waiters.load() == 0) {
        return;
    }
    {
        std::lock_guard<std::mutex> lk(_park_mutex);
        ++_park_epoch;
    }
    if (all) {
        _park_cv.notify_all();
    } else {
        _park_cv.notify_one();
    }
}

// ---------------------------------------------------------------------------
// Scheduler thread
// ---------------------------------------------------------------------------

void MultiCoreTaskQueue::_scheduler_loop() {
    Thread::set_self_name("pipe_task_sched");
    std::vector<SchedulerMessage> batch;
    std::vector<std::shared_ptr<std::promise<void>>> syncs;
    while (true) {
        batch.clear();
        {
            // a bitmap design can potentially kill this mutex.
            std::unique_lock<std::mutex> lk(_inbox_mutex);
            if (_inbox.empty() && !_closed.load()) {
                _inbox_cv.wait_for(lk, std::chrono::milliseconds(SCHEDULER_TICK_MS));
            }
            while (!_inbox.empty()) {
                batch.push_back(std::move(_inbox.front()));
                _inbox.pop_front();
            }
        }
        bool closing = _closed.load();
        for (auto& msg : batch) {
            _handle_message(msg, syncs);
        }
        if (!closing) {
            // Every pass: compact tombstones, re-sort by attained service, dispatch.
            // Destroy is only attempted after compact has cleared in_sched.
            _rebalance_and_dispatch();
            _try_teardown();
        }
        for (auto& promise : syncs) {
            promise->set_value();
        }
        syncs.clear();
        if (closing) {
            break;
        }
    }
}

MultiCoreTaskQueue::QueryState* MultiCoreTaskQueue::_resolve(const TUniqueId& query_id) {
    // Only the scheduler thread erases registry entries, so a pointer resolved here
    // stays valid for the rest of this scheduler pass.
    std::shared_lock<std::shared_mutex> rlock(_registry_mutex);
    auto it = _registry.find(query_id);
    return it == _registry.end() ? nullptr : it->second.get();
}

void MultiCoreTaskQueue::_handle_message(SchedulerMessage& msg,
                                         std::vector<std::shared_ptr<std::promise<void>>>& syncs) {
    switch (msg.type) {
    case SchedulerMessage::Type::ACK: {
        if (_worker_in_range(msg.worker_id)) {
            WorkerSched& ws = _worker_sched[msg.worker_id];
            if (msg.seq == ws.written_seq) {
                ws.acked = true;
            }
        }
        if (msg.state != nullptr) {
            // The worker left `state` by observing a slot rewrite; balance the attach.
            msg.state->workers_attached--;
            DCHECK_GE(msg.state->workers_attached, 0);
        }
        break;
    }
    case SchedulerMessage::Type::DETACHED: {
        DCHECK(msg.state != nullptr);
        if (_worker_in_range(msg.worker_id)) {
            WorkerSched& ws = _worker_sched[msg.worker_id];
            // Only meaningful if the worker is still logically on this query; if the
            // slot was rewritten in the meantime, the pending ACK carries prev=null
            // and this message is the balancing decrement of the old attach.
            if (ws.target == msg.state && ws.acked) {
                ws.self_detached = true;
            }
        }
        msg.state->workers_attached--;
        DCHECK_GE(msg.state->workers_attached, 0);
        break;
    }
    case SchedulerMessage::Type::NEW_QUERY: {
        QueryState* qs = _resolve(msg.query_id);
        if (qs != nullptr && !qs->terminated) {
            _add_to_sched(qs);
        }
        break;
    }
    case SchedulerMessage::Type::QUERY_TERMINATED: {
        QueryState* qs = _resolve(msg.query_id);
        if (qs == nullptr) {
            break;
        }
        qs->terminated = true;
        qs->rr_grant = 0;
        // Stay in `_queries` as a live tombstone until the next compact. Do not erase.
        if (!qs->in_destroy_candidates) {
            qs->in_destroy_candidates = true;
            _destroy_candidates.push_back(qs);
        }
        bool wrote = false;
        for (int i = 0; i < static_cast<int>(_worker_sched.size()); ++i) {
            WorkerSched& ws = _worker_sched[i];
            if (ws.acked && ws.target == qs) {
                _write_assignment(i, nullptr);
                wrote = true;
            }
        }
        if (wrote) {
            _notify_workers(true);
        }
        break;
    }
    case SchedulerMessage::Type::SYNC: {
        syncs.push_back(msg.sync);
        break;
    }
    }
}

void MultiCoreTaskQueue::_add_to_sched(QueryState* node) {
    if (node->in_sched) {
        return;
    }
    _queries.push_back(node);
    node->in_sched = true;
}

void MultiCoreTaskQueue::_try_teardown() {
    auto it = _destroy_candidates.begin();
    while (it != _destroy_candidates.end()) {
        QueryState* qs = *it;
        // Compact must have dropped this pointer from `_queries` first. Destroying
        // while in_sched would leave a dead address in the vector.
        if (qs->in_sched || qs->workers_attached > 0) {
            ++it;
            continue;
        }
        bool erased = false;
        {
            std::unique_lock<std::shared_mutex> wlock(_registry_mutex);
            if (qs->terminated && qs->in_flight.load() == 0 && qs->pending_approx.load() == 0) {
                DCHECK(!qs->in_sched);
                _registry.erase(qs->query_id);
                erased = true;
            }
        }
        if (erased) {
            it = _destroy_candidates.erase(it);
        } else {
            ++it;
        }
    }
}

void MultiCoreTaskQueue::_write_assignment(int worker_id, QueryState* value) {
    WorkerSched& ws = _worker_sched[worker_id];
    DCHECK(ws.acked);
    if (value != nullptr) {
        value->workers_attached++;
    }
    ws.target = value;
    ws.acked = false;
    ws.self_detached = false;
    ws.rr_kept = false;
    WorkerSlot& slot = _worker_slots[worker_id];
    uint64_t seq = slot.seq.load(std::memory_order_relaxed) + 1;
    ws.written_seq = seq;
    slot.assigned.store(value, std::memory_order_relaxed);
    // The release store on `seq` publishes `assigned`; the worker acquires on `seq`.
    slot.seq.store(seq, std::memory_order_release);
}

void MultiCoreTaskQueue::_rebalance_and_dispatch() {
    // Compact: drop terminated tombstones from the vector and clear in_sched. The
    // object stays in `_registry` until `_try_teardown` sees !in_sched and the
    // reclaim gate. This is the only place that removes a pointer from `_queries`.
    size_t live = 0;
    for (QueryState* qs : _queries) {
        if (qs->terminated) {
            qs->in_sched = false;
            qs->rr_grant = 0;
            continue;
        }
        _queries[live++] = qs;
    }
    _queries.resize(live);

    std::stable_sort(_queries.begin(), _queries.end(), [](const QueryState* a, const QueryState* b) {
        return a->attained_ns.load(std::memory_order_relaxed) <
               b->attained_ns.load(std::memory_order_relaxed);
    });

    // Phase 1: desired grants per query - greedy least-attained-first. Each query
    // takes as many workers as it can use (capped by its own worker cap: the per-query
    // session variable when set, otherwise the BE config, both re-read every pass so a
    // runtime change lands within one tick) before the next is considered. Demand is
    // the query's runnable task count, which counts its tasks wherever they sit while
    // excluding those parked on a dependency, so a query cannot hold cores for work it
    // cannot perform. The sub-queue terms are a floor: they keep the sentinel bucket
    // (whose tasks have no QueryContext, so no active count) schedulable, and they
    // cover the window where a task has already blocked but its worker has yet to
    // release it.
    // Every scheduled query gets its scratch reset, not just the granted ones,
    // because phase 2 reads the grant of whatever query a worker currently sits on.
    int remaining = static_cast<int>(_worker_slots.size());
    const int default_cap = config::pipeline_query_worker_cap;
    _granted_queries.clear();
    for (QueryState* qs : _queries) {
        qs->rr_grant = 0;
        qs->rr_demand = std::max(qs->active_tasks.load(),
                                 qs->pending_approx.load() + qs->in_flight.load());
        int want = qs->rr_demand;
        // A negative mirror means the query set no session-level override (and is
        // also what the sentinel bucket reports), so fall back to the BE config.
        int cap = qs->worker_cap.load(std::memory_order_relaxed);
        if (cap < 0) {
            cap = default_cap;
        }
        if (cap > 0) {
            want = std::min(want, cap);
        }
        if (remaining > 0 && want > 0) {
            qs->rr_grant = std::min(remaining, want);
            remaining -= qs->rr_grant;
            _granted_queries.push_back(qs);
        }
    }

    // Phase 2: keep stable workers in place. A worker already attached (acked, not
    // self-detached) to a query with a grant consumes one grant with no slot write,
    // so a steady state generates no control traffic at all.
    for (auto& ws : _worker_sched) {
        ws.rr_kept = false;
        if (ws.acked && !ws.self_detached && ws.target != nullptr && !ws.target->terminated &&
            ws.target->rr_grant > 0) {
            ws.target->rr_grant--;
            ws.rr_kept = true;
        }
    }

    // Phase 3: hand leftover grants to movable workers. `_granted_queries` is already
    // in attained-service order, so the least-attained query is served first. A worker
    // is movable when its last slot write was acked (never two outstanding writes per
    // worker) and it was not kept in phase 2: it is unassigned, self-detached, or
    // attached to a query that no longer wants it.
    bool wrote = false;
    size_t scan = 0;
    for (QueryState* qs : _granted_queries) {
        while (qs->rr_grant > 0) {
            int chosen = -1;
            for (size_t n = 0; n < _worker_sched.size(); ++n, ++scan) {
                size_t i = scan % _worker_sched.size();
                WorkerSched& ws = _worker_sched[i];
                if (ws.acked && !ws.rr_kept) {
                    chosen = static_cast<int>(i);
                    ++scan;
                    break;
                }
            }
            if (chosen < 0) {
                // No movable worker anywhere; the rest of the grants wait for the
                // pending acks and are recomputed next pass.
                if (wrote) {
                    _notify_workers(true);
                }
                return;
            }
            _write_assignment(chosen, qs);
            qs->rr_grant--;
            wrote = true;
        }
    }
    if (wrote) {
        _notify_workers(true);
    }
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

void MultiCoreTaskQueue::close() {
    bool expected = false;
    if (!_closed.compare_exchange_strong(expected, true)) {
        return;
    }
    {
        // Taken so the notify cannot slip between the scheduler's empty-check and its
        // wait (the scheduler holds this mutex across both).
        std::lock_guard<std::mutex> lk(_inbox_mutex);
    }
    _inbox_cv.notify_all();
    if (_scheduler_thread.joinable()) {
        _scheduler_thread.join();
    }
    // Fulfill any test-sync promises that raced with shutdown so waiters can't hang.
    {
        std::lock_guard<std::mutex> lk(_inbox_mutex);
        for (auto& msg : _inbox) {
            if (msg.type == SchedulerMessage::Type::SYNC && msg.sync != nullptr) {
                msg.sync->set_value();
            }
        }
        _inbox.clear();
    }
    _notify_workers(true);
    DorisMetrics::instance()->pipeline_task_queue_size->increment(
            -static_cast<int64_t>(_total_task_size.load()));
}

size_t MultiCoreTaskQueue::registry_size_for_test() const {
    std::shared_lock<std::shared_mutex> rlock(_registry_mutex);
    return _registry.size();
}

int MultiCoreTaskQueue::assigned_workers_for_test(const TUniqueId& query_id) const {
    // Holding the registry lock keeps the state alive for the pointer comparison; the
    // slots themselves are never dereferenced here.
    std::shared_lock<std::shared_mutex> rlock(_registry_mutex);
    auto it = _registry.find(query_id);
    if (it == _registry.end()) {
        return 0;
    }
    const QueryState* qs = it->second.get();
    int assigned = 0;
    for (const auto& slot : _worker_slots) {
        if (slot.assigned.load(std::memory_order_acquire) == qs) {
            assigned++;
        }
    }
    return assigned;
}

void MultiCoreTaskQueue::wait_scheduler_settled_for_test() {
    if (_mode != Mode::FULL || _closed.load()) {
        return;
    }
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    SchedulerMessage msg;
    msg.type = SchedulerMessage::Type::SYNC;
    msg.sync = promise;
    _post_message(std::move(msg));
    // Guard against a shutdown racing the post: close() fulfills leftover syncs, but
    // if it drained before our push, poll the closed flag instead of hanging.
    while (future.wait_for(std::chrono::milliseconds(50)) != std::future_status::ready) {
        if (_closed.load()) {
            return;
        }
    }
}

#include "common/compile_check_end.h"
} // namespace doris
