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

#include "exec/pipeline/slotted_serial_task_scheduler.h"

#include <chrono>

#include "common/config.h"
#include "common/logging.h"
#include "exec/pipeline/pipeline_fragment_context.h"
#include "util/thread.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {
#include "common/compile_check_begin.h"

SlottedSerialTaskScheduler::SlottedSerialTaskScheduler(int thread_num, std::string name,
                                                       std::shared_ptr<CgroupCpuCtl> cgroup_cpu_ctl)
        : TaskScheduler(thread_num, std::move(name), std::move(cgroup_cpu_ctl)),
          _router(thread_num, config::pipeline_closed_system_slots) {
    _slots.reserve(static_cast<size_t>(_router.slot_count()));
    for (int slot = 0; slot < _router.slot_count(); ++slot) {
        _slots.push_back(std::make_unique<SerialTaskQueue>());
    }
    LOG(INFO) << "slotted serial scheduler " << _name << ": " << _router.worker_count()
              << " workers, " << _router.slot_count() << " query slots";
}

SlottedSerialTaskScheduler::~SlottedSerialTaskScheduler() {
    // The base destructor's stop() cannot reach the overrides below, and the workers are
    // parked in this class's slot queues, so shut everything down while they still exist.
    stop();
}

Status SlottedSerialTaskScheduler::start() {
    RETURN_IF_ERROR(TaskScheduler::start());
    _admitter = std::thread([this] {
        Thread::set_self_name("slot_admitter");
        _admit_loop();
    });
    return Status::OK();
}

void SlottedSerialTaskScheduler::stop() {
    _stop_admitter();
    TaskScheduler::stop();
}

void SlottedSerialTaskScheduler::_stop_admitter() {
    {
        std::lock_guard<std::mutex> l(_route_lock);
        _admitter_stop = true;
    }
    _admit_cv.notify_all();
    if (_admitter.joinable()) {
        _admitter.join();
    }
}

void SlottedSerialTaskScheduler::_admit_loop() {
    std::unique_lock<std::mutex> l(_route_lock);
    while (!_admitter_stop) {
        while (_router.can_admit()) {
            const int slot = _router.free_slot();
            auto admission = _router.admit(slot);
            if (!admission.has_value()) {
                break;
            }
            LOG(INFO) << "closed-system slot " << slot << " (" << _router.workers_of_slot(slot)
                      << " workers) now serves " << print_id(admission->query_id)
                      << ", waiting=" << _router.waiting_size();
            // Replayed under `_route_lock`, so no direct registration or push for this query
            // can reach the slot ahead of what was buffered while it waited.
            auto& queue = *_slots[static_cast<size_t>(slot)];
            for (const auto& info : admission->fragments) {
                auto st = queue.register_fragment(info);
                if (!st.ok()) {
                    LOG(WARNING) << "slot " << slot << " failed to register fragment "
                                 << info.fragment_id << " of " << print_id(info.query_id) << ": "
                                 << st.to_string();
                }
            }
            for (auto& task : admission->tasks) {
                static_cast<void>(queue.push_back(std::move(task)));
            }
        }
        _admit_cv.wait_for(l, std::chrono::milliseconds(ADMIT_WAIT_TIMEOUT_MS));
    }
}

Status SlottedSerialTaskScheduler::register_fragment(const SerialFragmentInfo& info) {
    SerialTaskQueue* queue = nullptr;
    {
        std::lock_guard<std::mutex> l(_route_lock);
        if (_admitter_stop) {
            return Status::InternalError("SlottedSerialTaskScheduler stopped");
        }
        const int slot = _router.on_register(info);
        if (slot < 0) {
            _admit_cv.notify_one();
            return Status::OK();
        }
        queue = _slots[static_cast<size_t>(slot)].get();
    }
    return queue->register_fragment(info);
}

Status SlottedSerialTaskScheduler::_push_task(PipelineTaskSPtr task) {
    SerialTaskQueue* queue = nullptr;
    {
        std::lock_guard<std::mutex> l(_route_lock);
        if (_admitter_stop) {
            return Status::InternalError("SlottedSerialTaskScheduler stopped");
        }
        const int slot = _router.on_push(task->query_id(), task);
        if (slot < 0) {
            return Status::OK();
        }
        queue = _slots[static_cast<size_t>(slot)].get();
    }
    return queue->push_back(std::move(task));
}

Status SlottedSerialTaskScheduler::_push_task(PipelineTaskSPtr task, int /*core_id*/) {
    return _push_task(std::move(task));
}

PipelineTaskSPtr SlottedSerialTaskScheduler::_take_task(int index) {
    // The worker/slot mapping is fixed at construction, so it needs no lock.
    const int slot = std::max(_router.slot_of_worker(index), 0);
    return _slots[static_cast<size_t>(slot)]->take(index);
}

void SlottedSerialTaskScheduler::notify_pipeline_finished(const TUniqueId& query_id,
                                                          int fragment_id, PipelineId pipeline_id,
                                                          PipelineFragmentContext* ctx) {
    int slot = -1;
    {
        std::lock_guard<std::mutex> l(_route_lock);
        slot = _router.slot_of_query(query_id);
    }
    if (slot < 0) {
        return;
    }
    auto& queue = *_slots[static_cast<size_t>(slot)];
    PipelineKey key {query_id, fragment_id, pipeline_id};
    int64_t start_ns = queue.wallclock_start_ns(key);
    int64_t end_ns = MonotonicNanos();
    int64_t elapsed = start_ns > 0 && end_ns >= start_ns ? end_ns - start_ns : 0;

    if (ctx != nullptr) {
        ctx->record_pipeline_wallclock(pipeline_id, start_ns, end_ns, elapsed);
    }
    LOG(INFO) << "serial pipeline wallclock query_id=" << print_id(query_id)
              << " fragment_id=" << fragment_id << " pipeline_id=" << pipeline_id
              << " slot=" << slot << " slot_workers=" << _router.workers_of_slot(slot)
              << " wall_clock_ns=" << elapsed;

    queue.on_pipeline_finished(key);
}

void SlottedSerialTaskScheduler::notify_query_finished(const TUniqueId& query_id) {
    {
        std::lock_guard<std::mutex> l(_route_lock);
        const int slot = _router.on_finish(query_id);
        if (slot < 0) {
            return;
        }
        // Retire the query in its slot before the admitter can hand that slot to the next.
        _slots[static_cast<size_t>(slot)]->on_query_finished(query_id);
    }
    _admit_cv.notify_one();
}

void SlottedSerialTaskScheduler::_close_queue() {
    _stop_admitter();
    for (auto& slot : _slots) {
        slot->close();
    }
}

void SlottedSerialTaskScheduler::_update_statistics(PipelineTask* task, int64_t time_spent) {
    if (task != nullptr) {
        task->inc_runtime_ns(time_spent);
    }
}

#include "common/compile_check_end.h"
} // namespace doris
