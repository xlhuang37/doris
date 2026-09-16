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
#include <string>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "exec/pipeline/pipeline_task.h"
#include "util/uid_util.h"

namespace doris {
#include "common/compile_check_begin.h"

MultiCoreTaskQueue::MultiCoreTaskQueue(int core_size, Mode mode)
        : _core_size(core_size),
          _mode(mode),
          _slots(mode == Mode::CLOSED ? static_cast<size_t>(std::max(core_size, 1)) : 0),
          _worker_local(mode == Mode::CLOSED ? static_cast<size_t>(std::max(core_size, 1)) : 0),
          _slot_table(std::max(core_size, 1)) {
    if (_mode == Mode::CLOSED) {
        // No query exists yet, so this only installs the configured slot count.
        std::lock_guard<std::mutex> l(_admission_mutex);
        std::vector<int> changed;
        _slot_table.rebind(config::pipeline_closed_system_slots, &changed);
        _publish_slots(&changed);
        LOG(INFO) << "closed-system task queue: " << _core_size << " workers, "
                  << _slot_table.slot_count() << " query slots";
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
        _notify_park(_general_park, false);
        return Status::OK();
    }

    const TUniqueId query_id = task->query_id();
    QueryStatePtr qs = _find_query(query_id);
    if (qs == nullptr) {
        // First task of this query in this pool, or the query was reclaimed and this task
        // resurrects it.
        std::unique_lock<std::shared_mutex> wlock(_registry_mutex);
        if (_closed.load()) {
            return Status::InternalError("WorkTaskQueue closed");
        }
        auto& entry = _registry[query_id];
        if (entry == nullptr) {
            entry = std::make_shared<QueryState>(query_id);
        }
        qs = entry;
    }
    // Our own reference keeps the state alive, so the physical enqueue needs no lock.
    qs->pending_approx.fetch_add(1);
    if (!qs->sub_queue.enqueue(std::move(task))) {
        qs->pending_approx.fetch_sub(1);
        return Status::InternalError("WorkTaskQueue enqueue failed");
    }
    _total_task_size.fetch_add(1);
    DorisMetrics::instance()->pipeline_task_queue_size->increment(1);

    if (qs->slot_index.load(std::memory_order_acquire) < 0 || _partition_is_stale()) {
        // The query holds no slot (brand new, or still waiting for one), or the slot count
        // was retuned. Either way admission decides where it belongs; the common case of a
        // bound query and an unchanged partition costs one atomic load and no lock.
        _admit(qs);
    }
    const int slot = qs->slot_index.load(std::memory_order_acquire);
    if (slot >= 0) {
        _notify_park(_slots[static_cast<size_t>(slot)].park, false);
    }
    return Status::OK();
}

// ---------------------------------------------------------------------------
// Worker path
// ---------------------------------------------------------------------------

PipelineTaskSPtr MultiCoreTaskQueue::take(int core_id) {
    return _take(core_id, WAIT_CORE_TASK_TIMEOUT_MS);
}

PipelineTaskSPtr MultiCoreTaskQueue::_take(int worker_id, uint32_t timeout_ms) {
    PipelineTaskSPtr task = _try_take_once(worker_id);
    if (!task && !_closed.load() && timeout_ms > 0) {
        // Park until a producer or an admission decision signals, bounded by `timeout_ms`.
        // The epoch is sampled under the lock and re-checked before sleeping so a
        // notification between the failed dequeue below and the wait is not lost; a
        // producer racing ahead of the waiter registration is bounded by the timeout.
        ParkPoint& park = _park_point_of(worker_id);
        std::unique_lock<std::mutex> lk(park.mutex);
        uint64_t epoch = park.epoch;
        park.waiters.fetch_add(1);
        lk.unlock();
        task = _try_take_once(worker_id);
        lk.lock();
        if (!task && park.epoch == epoch && !_closed.load()) {
            park.cv.wait_for(lk, std::chrono::milliseconds(timeout_ms));
        }
        park.waiters.fetch_sub(1);
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

MultiCoreTaskQueue::QueryState* MultiCoreTaskQueue::_refresh_binding(int worker_id) {
    if (!_worker_in_range(worker_id)) {
        return nullptr;
    }
    WorkerLocal& local = _worker_local[static_cast<size_t>(worker_id)];
    const int slot = _slot_of_worker(worker_id);
    if (slot < 0 || slot >= static_cast<int>(_slots.size())) {
        local.bound.reset();
        local.seen_slot = -1;
        return nullptr;
    }
    Slot& s = _slots[static_cast<size_t>(slot)];
    const uint64_t seq = s.seq.load(std::memory_order_acquire);
    if (slot != local.seen_slot || seq != local.seen_seq) {
        // Take a reference of our own: from here on this worker may keep using the query
        // even if admission rebinds the slot, and the state cannot be freed under it.
        std::lock_guard<std::mutex> l(s.park.mutex);
        local.bound = s.owner;
        local.seen_seq = s.seq.load(std::memory_order_relaxed);
        local.seen_slot = slot;
    }
    return local.bound.get();
}

PipelineTaskSPtr MultiCoreTaskQueue::_try_take_once(int worker_id) {
    if (_closed.load()) {
        return nullptr;
    }
    PipelineTaskSPtr task;
    if (_mode == Mode::GENERAL_ONLY) {
        if (!_queue.try_dequeue(task)) {
            return nullptr;
        }
        _total_task_size.fetch_sub(1);
        DorisMetrics::instance()->pipeline_task_queue_size->increment(-1);
        return task;
    }
    // Closed system: this worker serves exactly one query - the one its slot points at.
    // An empty slot, or a slot whose query has nothing runnable, means idling. There is no
    // fallback queue to raid and no other slot to steal from.
    QueryState* qs = _refresh_binding(worker_id);
    if (qs == nullptr) {
        return nullptr;
    }
    if (!qs->sub_queue.try_dequeue(task)) {
        return nullptr;
    }
    // in_flight up BEFORE pending down: a live task is always visible in at least one of
    // the two counters, which is what makes the reclaim gate safe.
    qs->in_flight.fetch_add(1);
    qs->pending_approx.fetch_sub(1);
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

void MultiCoreTaskQueue::_release_in_flight(PipelineTask* task, bool charge, int64_t time_spent) {
    auto charged_ns = static_cast<uint64_t>(std::max<int64_t>(time_spent, 0));
    if (charge) {
        // Charge the executed CPU time to the owning query's global counter, which is
        // shared by all of the query's tasks across fragments, instances and pools. It is
        // a profiling total: the closed-system partition makes no decision from it.
        task->add_query_runtime_ns(charged_ns);
    }
    if (_mode != Mode::CLOSED) {
        return;
    }
    QueryStatePtr qs = _find_query(task->query_id());
    if (qs == nullptr) {
        return;
    }
    qs->in_flight.fetch_sub(1);
}

// ---------------------------------------------------------------------------
// Parking
// ---------------------------------------------------------------------------

MultiCoreTaskQueue::ParkPoint& MultiCoreTaskQueue::_park_point_of(int worker_id) {
    if (_mode == Mode::CLOSED) {
        const int slot = _slot_of_worker(worker_id);
        if (slot >= 0 && slot < static_cast<int>(_slots.size())) {
            return _slots[static_cast<size_t>(slot)].park;
        }
    }
    return _general_park;
}

void MultiCoreTaskQueue::_notify_park(ParkPoint& park, bool all) {
    if (park.waiters.load() == 0) {
        // Nobody has registered as a waiter yet. A worker on its way to parking always
        // retries the dequeue after registering, so it cannot miss what we just enqueued.
        return;
    }
    {
        std::lock_guard<std::mutex> lk(park.mutex);
        ++park.epoch;
    }
    if (all) {
        park.cv.notify_all();
    } else {
        park.cv.notify_one();
    }
}

void MultiCoreTaskQueue::_wake_all_parks() {
    for (auto& slot : _slots) {
        {
            std::lock_guard<std::mutex> lk(slot.park.mutex);
            ++slot.park.epoch;
        }
        slot.park.cv.notify_all();
    }
    {
        std::lock_guard<std::mutex> lk(_general_park.mutex);
        ++_general_park.epoch;
    }
    _general_park.cv.notify_all();
}

// ---------------------------------------------------------------------------
// Admission
// ---------------------------------------------------------------------------

int MultiCoreTaskQueue::_configured_slot_count() const {
    return std::clamp(config::pipeline_closed_system_slots, 1, std::max(_core_size, 1));
}

MultiCoreTaskQueue::QueryStatePtr MultiCoreTaskQueue::_find_query(const TUniqueId& query_id) const {
    std::shared_lock<std::shared_mutex> rlock(_registry_mutex);
    auto it = _registry.find(query_id);
    return it == _registry.end() ? nullptr : it->second;
}

void MultiCoreTaskQueue::_admit(const QueryStatePtr& qs) {
    if (_closed.load()) {
        return;
    }
    std::vector<QueryStatePtr> reclaimed;
    {
        std::lock_guard<std::mutex> l(_admission_mutex);
        if (qs->terminated) {
            // The QueryContext is already gone; whatever is left in its sub-queue is
            // dropped when the state is reclaimed.
            return;
        }
        std::vector<int> changed;
        _slot_table.add(qs);
        _slot_table.rebind(config::pipeline_closed_system_slots, &changed);
        _publish_slots(&changed);
        _collect_reclaimable(&reclaimed);
    }
    _erase_reclaimed(reclaimed);
}

void MultiCoreTaskQueue::notify_query_terminated(const TUniqueId& query_id) {
    if (_mode != Mode::CLOSED) {
        return;
    }
    QueryStatePtr qs = _find_query(query_id);
    if (qs == nullptr) {
        return;
    }
    std::vector<QueryStatePtr> reclaimed;
    {
        std::lock_guard<std::mutex> l(_admission_mutex);
        if (!qs->terminated) {
            qs->terminated = true;
            _dying.push_back(qs);
        }
        std::vector<int> changed;
        const int vacated = _slot_table.remove(qs);
        if (vacated >= 0) {
            changed.push_back(vacated);
        }
        // Hand the freed slot to the query that has been waiting longest.
        _slot_table.rebind(config::pipeline_closed_system_slots, &changed);
        _publish_slots(&changed);
        _collect_reclaimable(&reclaimed);
    }
    _erase_reclaimed(reclaimed);
}

void MultiCoreTaskQueue::_publish_slots(std::vector<int>* changed) {
    std::sort(changed->begin(), changed->end());
    changed->erase(std::unique(changed->begin(), changed->end()), changed->end());
    for (int slot : *changed) {
        if (slot < 0 || slot >= static_cast<int>(_slots.size())) {
            continue;
        }
        QueryStatePtr owner;
        const auto& table_owner = _slot_table.owner_of(slot);
        if (table_owner.has_value()) {
            owner = *table_owner;
        }
        Slot& s = _slots[static_cast<size_t>(slot)];
        QueryStatePtr previous;
        {
            std::lock_guard<std::mutex> lk(s.park.mutex);
            previous = std::move(s.owner);
            s.owner = owner;
            ++s.park.epoch;
        }
        if (previous != nullptr && previous != owner) {
            previous->slot_index.store(-1, std::memory_order_release);
        }
        if (owner != nullptr) {
            owner->slot_index.store(slot, std::memory_order_release);
        }
        // The release store publishes `owner`; workers acquire on `seq` and then refresh
        // their cached reference under the slot mutex.
        s.seq.fetch_add(1, std::memory_order_release);
        s.park.cv.notify_all();
        LOG(INFO) << "closed-system slot " << slot << " ("
                  << _slot_table.workers_of_slot(slot) << " workers) now serves "
                  << (owner != nullptr ? print_id(owner->query_id) : std::string("nothing"));
    }
    _slot_count.store(_slot_table.slot_count(), std::memory_order_release);
}

void MultiCoreTaskQueue::_collect_reclaimable(std::vector<QueryStatePtr>* reclaimed) {
    auto it = _dying.begin();
    while (it != _dying.end()) {
        const QueryStatePtr& qs = *it;
        // Wait until the pool is done with the query. Reclaiming earlier would destroy
        // whatever is still queued behind it from whichever thread happens to drop the
        // last reference; a query that was cancelled with tasks left over therefore
        // survives until close(), exactly as before.
        if (qs->pending_approx.load() != 0 || qs->in_flight.load() != 0) {
            ++it;
            continue;
        }
        reclaimed->push_back(qs);
        it = _dying.erase(it);
    }
}

void MultiCoreTaskQueue::_erase_reclaimed(const std::vector<QueryStatePtr>& reclaimed) {
    if (reclaimed.empty()) {
        return;
    }
    std::unique_lock<std::shared_mutex> wlock(_registry_mutex);
    for (const auto& qs : reclaimed) {
        auto it = _registry.find(qs->query_id);
        // A resurrecting push may have replaced the entry with a fresh state.
        if (it != _registry.end() && it->second == qs) {
            _registry.erase(it);
        }
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
    _wake_all_parks();
    DorisMetrics::instance()->pipeline_task_queue_size->increment(
            -static_cast<int64_t>(_total_task_size.load()));
}

// ---------------------------------------------------------------------------
// Test hooks
// ---------------------------------------------------------------------------

size_t MultiCoreTaskQueue::registry_size_for_test() const {
    std::shared_lock<std::shared_mutex> rlock(_registry_mutex);
    return _registry.size();
}

int MultiCoreTaskQueue::assigned_workers_for_test(const TUniqueId& query_id) const {
    QueryStatePtr qs = _find_query(query_id);
    if (qs == nullptr) {
        return 0;
    }
    std::lock_guard<std::mutex> l(_admission_mutex);
    const int slot = _slot_table.slot_of(qs);
    return slot < 0 ? 0 : _slot_table.workers_of_slot(slot);
}

int MultiCoreTaskQueue::slot_of_query_for_test(const TUniqueId& query_id) const {
    QueryStatePtr qs = _find_query(query_id);
    if (qs == nullptr) {
        return -1;
    }
    std::lock_guard<std::mutex> l(_admission_mutex);
    return _slot_table.slot_of(qs);
}

int MultiCoreTaskQueue::slot_count_for_test() const {
    std::lock_guard<std::mutex> l(_admission_mutex);
    return _slot_table.slot_count();
}

#include "common/compile_check_end.h"
} // namespace doris
