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

#include <algorithm>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "exec/pipeline/serial_task_queue.h"
#include "exec/pipeline/task_scheduler.h"

namespace doris {
#include "common/compile_check_begin.h"

// Which slot a query's work goes to, for the closed-system serial scheduler. Workers are
// cut into `slot_count` contiguous groups; each slot serves at most one query at a time.
// A query that is not bound to a slot waits, and everything that arrives for it
// (fragment registrations, runnable tasks) is buffered until it is admitted.
//
// Pure policy, no locks: the owner calls it under its own mutex. Templated on the task
// type only so it can be unit-tested without real pipeline tasks.
template <typename Task>
class BasicSlotRouter {
public:
    struct Admission {
        TUniqueId query_id;
        std::vector<SerialFragmentInfo> fragments;
        std::vector<Task> tasks;
    };

    BasicSlotRouter(int worker_count, int requested_slots)
            : _worker_count(std::max(worker_count, 1)),
              _owners(static_cast<size_t>(std::clamp(requested_slots, 1, _worker_count))) {}

    int worker_count() const { return _worker_count; }
    int slot_count() const { return static_cast<int>(_owners.size()); }

    int slot_of_worker(int worker_id) const {
        if (worker_id < 0 || worker_id >= _worker_count) {
            return -1;
        }
        return worker_id * slot_count() / _worker_count;
    }

    int workers_of_slot(int slot) const {
        int count = 0;
        for (int worker = 0; worker < _worker_count; ++worker) {
            if (slot_of_worker(worker) == slot) {
                count++;
            }
        }
        return count;
    }

    // The slot bound to `query_id`, or -1 while it waits (or is unknown).
    int slot_of_query(const TUniqueId& query_id) const {
        auto it = _bound.find(query_id);
        return it == _bound.end() ? -1 : it->second;
    }

    // Returns the slot to register with, or -1 if nothing should be registered now: the
    // query waits (the info is buffered) or has already finished (the info is dropped, so
    // it can never take a place in a slot's FCFS order). The first registration of an
    // unknown query puts it in the waiting queue.
    int on_register(const SerialFragmentInfo& info) {
        const int slot = slot_of_query(info.query_id);
        if (slot >= 0) {
            return slot;
        }
        if (_finished.contains(info.query_id)) {
            return -1;
        }
        auto [it, inserted] = _waiting.try_emplace(info.query_id);
        if (inserted) {
            it->second.arrival_ns = info.arrival_ns;
        }
        it->second.fragments.push_back(info);
        return -1;
    }

    // Returns the slot to push to, or -1 if the task was buffered for a waiting query.
    // Tasks of queries that never registered or already finished go to slot 0, where the
    // serial queue parks them unscheduled exactly as the single-queue scheduler would
    // (a push never registers a query, so it cannot disturb that slot's FCFS order).
    int on_push(const TUniqueId& query_id, const Task& task) {
        const int slot = slot_of_query(query_id);
        if (slot >= 0) {
            return slot;
        }
        auto it = _waiting.find(query_id);
        if (it == _waiting.end()) {
            return 0;
        }
        it->second.tasks.push_back(task);
        return -1;
    }

    int free_slot() const {
        for (int slot = 0; slot < slot_count(); ++slot) {
            if (!_owners[static_cast<size_t>(slot)].has_value()) {
                return slot;
            }
        }
        return -1;
    }

    bool can_admit() const { return !_waiting.empty() && free_slot() >= 0; }

    // Bind the earliest-arriving waiting query to the free `slot` and hand back what was
    // buffered for it, in arrival order. Ties on arrival time break on query id, the same
    // FCFS order the serial dispatch state uses.
    std::optional<Admission> admit(int slot) {
        if (slot < 0 || slot >= slot_count() || _owners[static_cast<size_t>(slot)].has_value() ||
            _waiting.empty()) {
            return std::nullopt;
        }
        auto next = _waiting.begin();
        for (auto it = _waiting.begin(); it != _waiting.end(); ++it) {
            if (it->second.arrival_ns < next->second.arrival_ns) {
                next = it;
            }
        }
        Admission admission {next->first, std::move(next->second.fragments),
                             std::move(next->second.tasks)};
        _waiting.erase(next);
        _owners[static_cast<size_t>(slot)] = admission.query_id;
        _bound[admission.query_id] = slot;
        return admission;
    }

    // Forget `query_id`. Returns the slot it vacated, or -1 if it was waiting or unknown;
    // anything buffered for a waiting query is dropped.
    int on_finish(const TUniqueId& query_id) {
        auto it = _bound.find(query_id);
        if (_waiting.erase(query_id) > 0 || it != _bound.end()) {
            _finished.insert(query_id);
        }
        if (it == _bound.end()) {
            return -1;
        }
        const int slot = it->second;
        _bound.erase(it);
        _owners[static_cast<size_t>(slot)].reset();
        return slot;
    }

    size_t waiting_size() const { return _waiting.size(); }

private:
    struct Waiting {
        int64_t arrival_ns = 0;
        std::vector<SerialFragmentInfo> fragments;
        std::vector<Task> tasks;
    };

    const int _worker_count;
    std::vector<std::optional<TUniqueId>> _owners;
    std::map<TUniqueId, int, TUniqueIdLess> _bound;
    // Ordered by query id, so equal arrival times admit in query-id order.
    std::map<TUniqueId, Waiting, TUniqueIdLess> _waiting;
    std::set<TUniqueId, TUniqueIdLess> _finished;
};

using SlotRouter = BasicSlotRouter<PipelineTaskSPtr>;

// Closed-system profiling on top of the serial scheduler. The pool's workers are split
// evenly into `pipeline_closed_system_slots` slots; each slot owns an unmodified
// SerialTaskQueue and serves one query at a time, so inside a slot the query runs the
// serial algorithm (one pipeline at a time, every worker of the slot on it). A dedicated
// admitter thread moves waiting queries into slots as they free up.
class SlottedSerialTaskScheduler final : public TaskScheduler {
public:
    SlottedSerialTaskScheduler(int thread_num, std::string name,
                               std::shared_ptr<CgroupCpuCtl> cgroup_cpu_ctl);
    ~SlottedSerialTaskScheduler() override;

    Status start() override;
    void stop() override;

    Status register_fragment(const SerialFragmentInfo& info) override;
    void notify_pipeline_finished(const TUniqueId& query_id, int fragment_id,
                                  PipelineId pipeline_id, PipelineFragmentContext* ctx) override;
    void notify_query_finished(const TUniqueId& query_id) override;

protected:
    PipelineTaskSPtr _take_task(int index) override;
    Status _push_task(PipelineTaskSPtr task) override;
    Status _push_task(PipelineTaskSPtr task, int core_id) override;
    void _close_queue() override;
    void _update_statistics(PipelineTask* task, int64_t time_spent) override;

private:
    void _admit_loop();
    void _stop_admitter();

    // Guarded by `_route_lock`. Lock order is always `_route_lock` before a slot queue's
    // own lock; the slot queues never call back into the scheduler.
    std::mutex _route_lock;
    std::condition_variable _admit_cv;
    SlotRouter _router;
    bool _admitter_stop = false;

    std::vector<std::unique_ptr<SerialTaskQueue>> _slots;
    std::thread _admitter;

    static constexpr auto ADMIT_WAIT_TIMEOUT_MS = 100;
};

#include "common/compile_check_end.h"
} // namespace doris
