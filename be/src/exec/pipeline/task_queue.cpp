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
#include <chrono> // IWYU pragma: keep
#include <memory>
#include <string>

#include "common/logging.h"
#include "exec/pipeline/pipeline_task.h"
#include "runtime/workload_group/workload_group.h"

namespace doris {
#include "common/compile_check_begin.h"

PipelineTaskSPtr SubTaskQueue::try_take(bool is_steal) {
    if (_queue.empty()) {
        return nullptr;
    }
    auto task = _queue.front();
    _queue.pop();
    return task;
}

////////////////////  PriorityTaskQueue ////////////////////

PriorityTaskQueue::PriorityTaskQueue() : _closed(false) {}

void PriorityTaskQueue::close() {
    std::unique_lock<std::mutex> lock(_work_size_mutex);
    _closed = true;
    _wait_task.notify_all();
    DorisMetrics::instance()->pipeline_task_queue_size->increment(-_total_task_size);
}

PipelineTaskSPtr PriorityTaskQueue::_try_take_unprotected(bool is_steal) {
    if (_total_task_size == 0 || _closed) {
        return nullptr;
    }

    // Strict absolute priority: serve the lowest non-empty level first.
    for (int level = 0; level < SUB_QUEUE_LEVEL; ++level) {
        while (!_sub_queues[level].empty()) {
            auto task = _sub_queues[level].try_take(is_steal);
            if (!task) {
                break;
            }
            // Lazy demotion: recompute the task's level from its fragment's global
            // runtime, which may have grown (on any core) while the task waited. If
            // the fragment now belongs in a deeper level, defer the task there
            // instead of running it at the current, higher-priority level.
            int want_level = _compute_level(task->fragment_runtime_ns());
            if (want_level > level) {
                _sub_queues[want_level].push_back(task);
                // The task is still queued, so _total_task_size is unchanged; keep
                // scanning. The deferred task will be reached when this same scan
                // advances to `want_level`, where its level will match and it runs.
                continue;
            }
            _total_task_size--;
            DorisMetrics::instance()->pipeline_task_queue_size->increment(-1);
            return task;
        }
    }
    return nullptr;
}

int PriorityTaskQueue::_compute_level(uint64_t runtime) {
    for (int i = 0; i < SUB_QUEUE_LEVEL - 1; ++i) {
        if (runtime <= _queue_level_limit[i]) {
            return i;
        }
    }
    return SUB_QUEUE_LEVEL - 1;
}

PipelineTaskSPtr PriorityTaskQueue::try_take(bool is_steal) {
    // TODO other efficient lock? e.g. if get lock fail, return null_ptr
    std::unique_lock<std::mutex> lock(_work_size_mutex);
    return _try_take_unprotected(is_steal);
}

PipelineTaskSPtr PriorityTaskQueue::take(uint32_t timeout_ms) {
    std::unique_lock<std::mutex> lock(_work_size_mutex);
    auto task = _try_take_unprotected(false);
    if (task) {
        return task;
    } else {
        if (timeout_ms > 0) {
            _wait_task.wait_for(lock, std::chrono::milliseconds(timeout_ms));
        } else {
            _wait_task.wait(lock);
        }
        return _try_take_unprotected(false);
    }
}

Status PriorityTaskQueue::push(PipelineTaskSPtr task) {
    if (_closed) {
        return Status::InternalError("WorkTaskQueue closed");
    }
    // The level is driven by the owning fragment's global CPU runtime, so all of a
    // fragment's tasks are bucketed together. The dequeue path re-checks this in
    // case the fragment crosses a threshold while the task waits.
    auto level = _compute_level(task->fragment_runtime_ns());
    std::unique_lock<std::mutex> lock(_work_size_mutex);

    _sub_queues[level].push_back(task);
    _total_task_size++;
    DorisMetrics::instance()->pipeline_task_queue_size->increment(1);
    _wait_task.notify_one();
    return Status::OK();
}

MultiCoreTaskQueue::~MultiCoreTaskQueue() = default;

MultiCoreTaskQueue::MultiCoreTaskQueue(int core_size)
        : _prio_task_queues(core_size), _closed(false), _core_size(core_size) {}

void MultiCoreTaskQueue::close() {
    if (_closed) {
        return;
    }
    _closed = true;
    // close all priority task queue
    std::ranges::for_each(_prio_task_queues,
                          [](auto& prio_task_queue) { prio_task_queue.close(); });
}

PipelineTaskSPtr MultiCoreTaskQueue::take(int core_id) {
    PipelineTaskSPtr task = nullptr;
    while (!_closed) {
        DCHECK(_prio_task_queues.size() > core_id)
                << " list size: " << _prio_task_queues.size() << " core_id: " << core_id
                << " _core_size: " << _core_size << " _next_core: " << _next_core.load();
        task = _prio_task_queues[core_id].try_take(false);
        if (task) {
            break;
        }
        task = _steal_take(core_id);
        if (task) {
            break;
        }
        task = _prio_task_queues[core_id].take(WAIT_CORE_TASK_TIMEOUT_MS /* timeout_ms */);
        if (task) {
            break;
        }
    }
    if (task) {
        task->pop_out_runnable_queue();
    }
    return task;
}

PipelineTaskSPtr MultiCoreTaskQueue::_steal_take(int core_id) {
    DCHECK(core_id < _core_size);
    int next_id = core_id;
    for (int i = 1; i < _core_size; ++i) {
        ++next_id;
        if (next_id == _core_size) {
            next_id = 0;
        }
        DCHECK(next_id < _core_size);
        auto task = _prio_task_queues[next_id].try_take(true);
        if (task) {
            return task;
        }
    }
    return nullptr;
}

Status MultiCoreTaskQueue::push_back(PipelineTaskSPtr task) {
    int thread_id = task->get_thread_id(_core_size);
    if (thread_id < 0) {
        thread_id = _next_core.fetch_add(1) % _core_size;
    }
    return push_back(task, thread_id);
}

Status MultiCoreTaskQueue::push_back(PipelineTaskSPtr task, int core_id) {
    DCHECK(core_id < _core_size);
    task->put_in_runnable_queue();
    return _prio_task_queues[core_id].push(task);
}

void MultiCoreTaskQueue::update_statistics(PipelineTask* task, int64_t time_spent) {
    // Charge the executed CPU time to the owning fragment's global counter. This
    // counter is shared by all of the fragment's tasks (across instances and
    // cores) and drives the fragment-granular MLFQ demotion. For tasks without a
    // fragment counter (e.g. RevokableTask), this is a no-op and they stay at the
    // highest priority level, which is the desired behavior.
    task->add_fragment_runtime_ns(time_spent);
}

} // namespace doris
