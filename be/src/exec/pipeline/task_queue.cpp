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
        _worker_local = std::vector<WorkerLocal>(worker_count);
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
        auto& entry = _registry[query_id];
        if (entry == nullptr) {
            entry = std::make_shared<QueryState>(_queue, query_id);
            created = true;
        }
        QueryState* qs = entry.get();
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

const MultiCoreTaskQueue::SlotTable* MultiCoreTaskQueue::_refresh_slots(int worker_id) {
    WorkerLocal& local = _worker_local[worker_id];
    if (_slot_epoch.load(std::memory_order_acquire) != local.seen_epoch) {
        // Both are read under the mutex the scheduler publishes under, so the snapshot
        // and the epoch the worker remembers always belong to the same pass.
        std::lock_guard<std::mutex> lk(_slot_mutex);
        local.table = _published_slots;
        local.seen_epoch = _slot_epoch.load(std::memory_order_relaxed);
    }
    return local.table.get();
}

bool MultiCoreTaskQueue::_try_take_from(const QueryStatePtr& qs, PipelineTaskSPtr& task) {
    if (qs == nullptr || !_queue.try_dequeue_from_producer(qs->token, task)) {
        return false;
    }
    // in_flight up BEFORE pending down: a live task is always visible in at least one
    // of the two counters (see header contract).
    qs->in_flight.fetch_add(1);
    qs->pending_approx.fetch_sub(1);
    _total_task_size.fetch_sub(1);
    DorisMetrics::instance()->pipeline_task_queue_size->increment(-1);
    return true;
}

PipelineTaskSPtr MultiCoreTaskQueue::_try_take_once(int worker_id) {
    if (_closed.load()) {
        return nullptr;
    }
    PipelineTaskSPtr task;
    if (_mode == Mode::FULL) {
        // Pick up any newly published array first so scheduler decisions are never
        // delayed by more than one execution slice.
        const SlotTable* slots = _worker_in_range(worker_id) ? _refresh_slots(worker_id) : nullptr;
        // "Inelastic first": single-task pipelines outrank everything, including the
        // slot array and attained-service ranking. Accounting is the same as the
        // tokenless fallback below (the task was never in a per-query sub-queue).
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
        if (slots != nullptr) {
            const auto slot_count = static_cast<int>(slots->slots.size());
            if (slots->policy == LasSlotPolicy::FIXED) {
                // Pinned to one slot: an empty slot means this worker does not serve
                // any ranked query right now and goes straight to the fallback.
                const int slot = las_slot_of_worker(worker_id, slot_count, _core_size);
                if (slot >= 0 && _try_take_from(slots->slots[slot], task)) {
                    return task;
                }
            } else {
                // Ordered: slot 0 is the least-attained query, so the first hit while
                // walking the array is the most deserving task this worker can run.
                for (const QueryStatePtr& qs : slots->slots) {
                    if (_try_take_from(qs, task)) {
                        return task;
                    }
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
        // Arm idle so the next push wakes the scheduler instead of waiting for a tick
        // to be ranked back into the array. Reclaim waits for terminate.
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
            // Every pass: compact tombstones, re-sort by attained service, publish.
            // Destroy is only attempted after compact has cleared in_sched.
            _rank_and_publish();
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

MultiCoreTaskQueue::QueryStatePtr MultiCoreTaskQueue::_resolve(const TUniqueId& query_id) {
    std::shared_lock<std::shared_mutex> rlock(_registry_mutex);
    auto it = _registry.find(query_id);
    return it == _registry.end() ? nullptr : it->second;
}

void MultiCoreTaskQueue::_handle_message(SchedulerMessage& msg,
                                         std::vector<std::shared_ptr<std::promise<void>>>& syncs) {
    switch (msg.type) {
    case SchedulerMessage::Type::NEW_QUERY: {
        QueryStatePtr qs = _resolve(msg.query_id);
        if (qs != nullptr && !qs->terminated) {
            _add_to_sched(std::move(qs));
        }
        break;
    }
    case SchedulerMessage::Type::QUERY_TERMINATED: {
        QueryStatePtr qs = _resolve(msg.query_id);
        if (qs == nullptr) {
            break;
        }
        qs->terminated = true;
        // Stay in `_queries` until the next compact, which is also what drops the query
        // from the slot array; nothing has to be unpublished synchronously because every
        // reference to it - including a worker's cached snapshot - is a shared_ptr.
        if (!qs->in_destroy_candidates) {
            qs->in_destroy_candidates = true;
            _destroy_candidates.push_back(std::move(qs));
        }
        break;
    }
    case SchedulerMessage::Type::SYNC: {
        syncs.push_back(msg.sync);
        break;
    }
    }
}

void MultiCoreTaskQueue::_add_to_sched(QueryStatePtr node) {
    if (node->in_sched) {
        return;
    }
    node->in_sched = true;
    _queries.push_back(std::move(node));
}

void MultiCoreTaskQueue::_try_teardown() {
    auto it = _destroy_candidates.begin();
    while (it != _destroy_candidates.end()) {
        const QueryStatePtr& qs = *it;
        // Compact must have dropped the query from `_queries` first, so that a state
        // reachable through the ranking is always reachable through the registry too.
        if (qs->in_sched) {
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

void MultiCoreTaskQueue::_publish(SlotTablePtr table) {
    {
        std::lock_guard<std::mutex> lk(_slot_mutex);
        _published_slots = std::move(table);
        // The release store publishes the table to the workers, which acquire on the
        // epoch before deciding whether to refresh.
        _slot_epoch.store(_slot_epoch.load(std::memory_order_relaxed) + 1,
                          std::memory_order_release);
    }
    _notify_workers(true);
}

void MultiCoreTaskQueue::_rank_and_publish() {
    // Compact: drop terminated queries from the vector and clear in_sched. The state
    // stays in `_registry` until `_try_teardown` sees !in_sched and the reclaim gate.
    // This is the only place that removes a query from `_queries`.
    auto live_end = std::remove_if(_queries.begin(), _queries.end(), [](const QueryStatePtr& qs) {
        if (!qs->terminated) {
            return false;
        }
        qs->in_sched = false;
        return true;
    });
    _queries.erase(live_end, _queries.end());

    std::stable_sort(_queries.begin(), _queries.end(),
                     [](const QueryStatePtr& a, const QueryStatePtr& b) {
                         return a->attained_ns.load(std::memory_order_relaxed) <
                                b->attained_ns.load(std::memory_order_relaxed);
                     });

    // Fill the array least-attained first. A query takes a slot only while it has
    // demand: its runnable task count, which counts its tasks wherever they sit while
    // excluding those parked on a dependency, so a query cannot hold a slot for work it
    // cannot perform. The sub-queue terms are a floor: they keep the sentinel bucket
    // (whose tasks have no QueryContext, so no active count) schedulable, and they cover
    // the window where a task has already blocked but its worker has yet to release it.
    // Both knobs are re-read here, so either can be retuned without a restart.
    auto next = std::make_shared<SlotTable>();
    next->policy = parse_las_slot_policy(config::pipeline_las_slot_policy);
    // Always exactly `slot_count` long, so an empty tail does not shift the fixed
    // worker/slot mapping.
    const auto slot_count =
            static_cast<size_t>(clamp_las_slot_count(config::pipeline_las_slot_count));
    next->slots.resize(slot_count);
    size_t filled = 0;
    for (const QueryStatePtr& qs : _queries) {
        if (filled == slot_count) {
            break;
        }
        const int demand =
                std::max(qs->active_tasks.load(), qs->pending_approx.load() + qs->in_flight.load());
        if (demand > 0) {
            next->slots[filled++] = qs;
        }
    }

    // Republish only on a real change: a steady state then costs the workers nothing
    // beyond the epoch load, and no snapshot refcount is touched.
    if (_published_slots != nullptr && _published_slots->policy == next->policy &&
        _published_slots->slots == next->slots) {
        return;
    }
    _publish(std::move(next));
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

MultiCoreTaskQueue::SlotTablePtr MultiCoreTaskQueue::_published_for_test() const {
    std::lock_guard<std::mutex> lk(_slot_mutex);
    return _published_slots;
}

int MultiCoreTaskQueue::slot_of_query_for_test(const TUniqueId& query_id) const {
    QueryStatePtr qs;
    {
        std::shared_lock<std::shared_mutex> rlock(_registry_mutex);
        auto it = _registry.find(query_id);
        if (it == _registry.end()) {
            return -1;
        }
        qs = it->second;
    }
    SlotTablePtr table = _published_for_test();
    if (table == nullptr) {
        return -1;
    }
    for (size_t i = 0; i < table->slots.size(); ++i) {
        if (table->slots[i] == qs) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

int MultiCoreTaskQueue::slot_count_for_test() const {
    SlotTablePtr table = _published_for_test();
    return table == nullptr ? 0 : static_cast<int>(table->slots.size());
}

int MultiCoreTaskQueue::slot_of_worker_for_test(int worker_id) const {
    return las_slot_of_worker(worker_id, slot_count_for_test(), _core_size);
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
