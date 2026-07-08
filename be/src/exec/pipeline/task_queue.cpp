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
#include <iterator>
#include <limits>
#include <memory>

#include "common/config.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "exec/pipeline/pipeline_task.h"

namespace doris {
#include "common/compile_check_begin.h"

MultiCoreTaskQueue::MultiCoreTaskQueue(int core_size) : _core_size(core_size) {}

MultiCoreTaskQueue::~MultiCoreTaskQueue() = default;

int MultiCoreTaskQueue::_compute_level(uint64_t runtime) const {
    for (int i = 0; i < SUB_QUEUE_LEVEL - 1; ++i) {
        if (runtime <= _queue_level_limit[i]) {
            return i;
        }
    }
    return SUB_QUEUE_LEVEL - 1;
}

MultiCoreTaskQueue::QueryNode* MultiCoreTaskQueue::_ensure_node(QueryContext* key) {
    std::unique_lock<std::mutex> lock(_map_mutex);
    auto it = _nodes.find(key);
    if (it != _nodes.end()) {
        return it->second.get();
    }
    auto node = std::make_unique<QueryNode>(_queue);
    node->key = key;
    QueryNode* raw = node.get();
    _nodes.emplace(key, std::move(node));
    return raw;
}

PipelineTaskSPtr MultiCoreTaskQueue::_take(int /*worker_id*/, uint32_t timeout_ms) {
    if (_closed) {
        return nullptr;
    }
    PipelineTaskSPtr task = nullptr;
    if (!_queue.try_dequeue(task) && !_closed && timeout_ms > 0) {
        std::unique_lock<std::mutex> lock(_wait_mutex);
        // Re-check under the lock to avoid missing a wakeup that raced with the empty
        // dequeue above; then park for at most `timeout_ms`.
        if (_total_task_size.load() == 0 && !_closed) {
            _wait_task.wait_for(lock, std::chrono::milliseconds(timeout_ms));
        }
        lock.unlock();
        _queue.try_dequeue(task);
    }
    if (task) {
        _total_task_size.fetch_sub(1);
        DorisMetrics::instance()->pipeline_task_queue_size->increment(-1);
        task->pop_out_runnable_queue();
    }
    return task;
}

PipelineTaskSPtr MultiCoreTaskQueue::take(int core_id) {
    return _take(core_id, WAIT_CORE_TASK_TIMEOUT_MS);
}

Status MultiCoreTaskQueue::_push(PipelineTaskSPtr task) {
    if (_closed) {
        return Status::InternalError("WorkTaskQueue closed");
    }
    task->put_in_runnable_queue();
    QueryNode* node = _ensure_node(task->query_ctx_raw());
    {
        // The sub-queue is single-producer, so serialize this query's enqueues.
        // Enqueues of different queries use different tokens and never contend here.
        std::unique_lock<std::mutex> lock(node->enqueue_mutex);
        if (!_queue.enqueue(node->token, std::move(task))) {
            return Status::MemoryLimitExceeded("failed to enqueue pipeline task");
        }
    }
    _total_task_size.fetch_add(1);
    DorisMetrics::instance()->pipeline_task_queue_size->increment(1);
    {
        std::unique_lock<std::mutex> lock(_wait_mutex);
        _wait_task.notify_one();
    }
    return Status::OK();
}

Status MultiCoreTaskQueue::push_back(PipelineTaskSPtr task) {
    return _push(std::move(task));
}

Status MultiCoreTaskQueue::push_back(PipelineTaskSPtr task, int /*core_id*/) {
    // `core_id` no longer pins a task to a shard; placement is global by query level.
    return _push(std::move(task));
}

void MultiCoreTaskQueue::update_statistics(PipelineTask* task, int64_t time_spent) {
    // Charge the executed CPU time to the owning query's global counter. This counter
    // is shared by all of the query's tasks (across fragments, instances and cores)
    // and across the pipeline/scan schedulers, and drives the query-granular MLFQ
    // demotion. For tasks without a query counter (e.g. RevokableTask) the charge is
    // a no-op and they stay at the highest priority level.
    task->add_query_runtime_ns(time_spent);

    QueryContext* key = task->query_ctx_raw();
    QueryNode* node = nullptr;
    {
        std::unique_lock<std::mutex> lock(_map_mutex);
        auto it = _nodes.find(key);
        if (it == _nodes.end()) {
            return;
        }
        node = it->second.get();
    }

    // Demote the whole query to a deeper level if its accumulated runtime crossed a
    // threshold. Runtime only grows, so this only ever moves the producer downward.
    int want = _compute_level(task->query_runtime_ns());
    if (want > node->current_level.load(std::memory_order_relaxed)) {
        std::unique_lock<std::mutex> lock(node->demotion_mutex);
        int current = node->current_level.load(std::memory_order_relaxed);
        if (want > current && _queue.demote_producer(node->token, want)) {
            node->current_level.store(want, std::memory_order_relaxed);
        }
    }
}

void MultiCoreTaskQueue::release_task(PipelineTask* /*task*/) {
    // No-op: the queue no longer tracks per-query in-flight workers.
}

void MultiCoreTaskQueue::remove_query(QueryContext* key) {
    std::unique_lock<std::mutex> lock(_map_mutex);
    _nodes.erase(key);
}

void MultiCoreTaskQueue::close() {
    std::unique_lock<std::mutex> lock(_wait_mutex);
    if (_closed) {
        return;
    }
    _closed = true;
    _wait_task.notify_all();
    DorisMetrics::instance()->pipeline_task_queue_size->increment(
            -static_cast<int64_t>(_total_task_size.load()));
}

#include "common/compile_check_end.h"
} // namespace doris
