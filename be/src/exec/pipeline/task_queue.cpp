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
#include "runtime/query_cpu_lease.h"
#include "runtime/workload_group/cpu_lease_grantor.h"

namespace doris {
#include "common/compile_check_begin.h"

MultiCoreTaskQueue::MultiCoreTaskQueue(int core_size)
        : _worker_sticky(std::max(core_size, 1), nullptr), _core_size(core_size) {
    static_assert(NUM_LEVELS >= QueryCpuLease::kNumLevels,
                  "level arrays must fit the lease MLFQ levels");
}

MultiCoreTaskQueue::~MultiCoreTaskQueue() = default;

bool MultiCoreTaskQueue::_lease_mode() const {
    return _grantor != nullptr && config::enable_cpu_lease_scheduling;
}

int MultiCoreTaskQueue::_compute_level(uint64_t runtime) const {
    for (int i = 0; i < LEGACY_LEVELS - 1; ++i) {
        if (runtime <= _queue_level_limit[i]) {
            return i;
        }
    }
    return LEGACY_LEVELS - 1;
}

int MultiCoreTaskQueue::_node_level(const QueryNode* node) const {
    uint64_t consumed = _node_runtime(node);
    if (_lease_mode() && node->lease != nullptr) {
        // Parallelism layer (running slots / leveling) then CPU band; mirrors
        // ClickHouse CPULeaseAllocation::computeRequestPriority.
        return node->lease->compute_priority(consumed);
    }
    return _compute_level(consumed);
}

int MultiCoreTaskQueue::_lowest_non_empty_level() const {
    for (int level = 0; level < NUM_LEVELS; ++level) {
        if (!_levels[level].empty()) {
            return level;
        }
    }
    return -1;
}

int MultiCoreTaskQueue::_lease(const QueryNode* /*node*/) const {
    // Soft per-query cap on concurrent workers. When > 0, workers beyond the cap spill
    // to lower-priority queries even if this query has more runnable tasks; default
    // (<= 0) means unbounded (strict absolute priority). In lease mode the per-query
    // cap is governed by cpu_lease_max_threads_per_query.
    int cap = _lease_mode() ? config::cpu_lease_max_threads_per_query
                            : config::pipeline_query_worker_cap;
    return cap > 0 ? cap : std::numeric_limits<int>::max();
}

bool MultiCoreTaskQueue::_admitted(const QueryNode* node) {
    // Legacy mode: every query is implicitly admitted.
    if (!_lease_mode()) {
        return true;
    }
    // Query-less tasks (e.g. RevokableTask) are always allowed.
    if (node->key == nullptr) {
        return true;
    }
    return _grantor->try_serve(node->key);
}

uint64_t MultiCoreTaskQueue::_node_runtime(const QueryNode* node) const {
    if (node->runnable.empty()) {
        return 0;
    }
    return node->runnable.front()->query_runtime_ns();
}

MultiCoreTaskQueue::QueryNode* MultiCoreTaskQueue::_ensure_node(QueryContext* key) {
    auto it = _nodes.find(key);
    if (it != _nodes.end()) {
        return it->second.get();
    }
    auto node = std::make_unique<QueryNode>();
    node->key = key;
    QueryNode* raw = node.get();
    _nodes.emplace(key, std::move(node));
    return raw;
}

void MultiCoreTaskQueue::_link(QueryNode* node, int level) {
    node->level = level;
    _levels[level].push_back(node);
    node->pos = std::prev(_levels[level].end());
    node->linked = true;
}

void MultiCoreTaskQueue::_unlink(QueryNode* node) {
    if (!node->linked) {
        return;
    }
    _levels[node->level].erase(node->pos);
    node->linked = false;
}

void MultiCoreTaskQueue::_relevel_locked(QueryNode* node) {
    if (!node->linked) {
        return;
    }
    int want = _node_level(node);
    if (want != node->level) {
        _unlink(node);
        _link(node, want);
    }
}

PipelineTaskSPtr MultiCoreTaskQueue::_pop_from_node(QueryNode* node, int worker_id) {
    auto task = node->runnable.front();
    node->runnable.pop();
    node->in_flight++;
    // Account a granted pipeline slot on the query's lease (drives parallelism leveling
    // and scanner bundling). Released in _release_worker_slot.
    if (node->lease != nullptr) {
        node->lease->acquire_slot();
    }
    _total_task_size.fetch_sub(1);
    DorisMetrics::instance()->pipeline_task_queue_size->increment(-1);

    if (worker_id >= 0 && worker_id < static_cast<int>(_worker_sticky.size())) {
        _worker_sticky[worker_id] = node->key;
    }

    if (node->runnable.empty()) {
        // No more runnable tasks: unlink from its level (kept in `_nodes` while it
        // still has in-flight workers, so their release can find it).
        _unlink(node);
    } else {
        // Round-robin: rotate this node to the back of its level so the next worker
        // serves a different query first.
        auto& lst = _levels[node->level];
        lst.splice(lst.end(), lst, node->pos);
        node->pos = std::prev(lst.end());
    }
    return task;
}

PipelineTaskSPtr MultiCoreTaskQueue::_try_take_unprotected(int worker_id) {
    if (_total_task_size.load() == 0 || _closed) {
        return nullptr;
    }

    // 1. Locality: keep serving the query this worker last served, as long as it is
    // still the highest-priority (lowest non-empty level) query, under its lease, and
    // still admitted by the grantor.
    if (worker_id >= 0 && worker_id < static_cast<int>(_worker_sticky.size())) {
        QueryContext* sticky = _worker_sticky[worker_id];
        if (sticky != nullptr) {
            auto it = _nodes.find(sticky);
            if (it != _nodes.end()) {
                QueryNode* node = it->second.get();
                if (node->linked && !node->runnable.empty()) {
                    _relevel_locked(node);
                    if (node->level == _lowest_non_empty_level() &&
                        node->in_flight < _lease(node) && _admitted(node)) {
                        return _pop_from_node(node, worker_id);
                    }
                }
            }
        }
    }

    // 2. Strict absolute priority: drain the lowest non-empty level first; within a
    // level, round-robin across queries (the list front is the least-recently served).
    // In lease mode, a query is served only if the grantor admits it (admission set
    // capped at max_active_queries_per_group); non-admitted queries are skipped so
    // workers fall through to admitted, higher-priority queries.
    for (int level = 0; level < NUM_LEVELS; ++level) {
        auto& lst = _levels[level];
        for (auto it = lst.begin(); it != lst.end();) {
            QueryNode* node = *it;
            // Lazy demotion: if the query's level rose (more slots / more CPU), move the
            // whole node (all of its tasks) to the deeper level and keep scanning.
            int want = _node_level(node);
            if (want > level) {
                auto next = std::next(it);
                _unlink(node);
                _link(node, want);
                it = next;
                continue;
            }
            if (!node->runnable.empty() && node->in_flight < _lease(node) && _admitted(node)) {
                return _pop_from_node(node, worker_id);
            }
            ++it;
        }
    }
    return nullptr;
}

PipelineTaskSPtr MultiCoreTaskQueue::_take(int worker_id, uint32_t timeout_ms) {
    PipelineTaskSPtr task = nullptr;
    {
        std::unique_lock<std::mutex> lock(_mutex);
        task = _try_take_unprotected(worker_id);
        if (!task && !_closed && timeout_ms > 0) {
            _wait_task.wait_for(lock, std::chrono::milliseconds(timeout_ms));
            task = _try_take_unprotected(worker_id);
        }
    }
    if (task) {
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
    QueryContext* key = task->query_ctx_raw();
    {
        std::unique_lock<std::mutex> lock(_mutex);
        if (_closed) {
            return Status::InternalError("WorkTaskQueue closed");
        }
        QueryNode* node = _ensure_node(key);
        if (node->lease == nullptr) {
            node->lease = task->cpu_lease();
        }
        node->runnable.push(task);
        if (!node->linked) {
            // An unlinked node has no runnable tasks; (re)link it at the level implied
            // by the owning query's current priority (lease layer+band, or legacy
            // runtime threshold).
            _link(node, _node_level(node));
        }
        _total_task_size.fetch_add(1);
        DorisMetrics::instance()->pipeline_task_queue_size->increment(1);
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

void MultiCoreTaskQueue::_release_worker_slot(QueryContext* key) {
    std::unique_lock<std::mutex> lock(_mutex);
    auto it = _nodes.find(key);
    if (it == _nodes.end()) {
        return;
    }
    QueryNode* node = it->second.get();
    if (node->in_flight > 0) {
        node->in_flight--;
        if (node->lease != nullptr) {
            node->lease->release_slot();
        }
    }
    if (!node->linked && node->runnable.empty() && node->in_flight == 0) {
        _nodes.erase(it);
        // The query has no runnable or in-flight work: free its admission slot so the
        // next highest-priority waiting query can be admitted on a subsequent take().
        // Lock order is always queue-lock -> grantor-lock (same as the take() path).
        if (_lease_mode() && key != nullptr) {
            _grantor->on_idle(key);
        }
    }
}

void MultiCoreTaskQueue::update_statistics(PipelineTask* task, int64_t time_spent) {
    // Charge the executed CPU time to the owning query's global counter. This counter
    // is shared by all of the query's tasks (across fragments, instances and cores)
    // and across the pipeline/scan schedulers, and drives the query-granular MLFQ
    // demotion. For tasks without a query counter (e.g. RevokableTask) the charge is
    // a no-op and they stay at the highest priority level.
    task->add_query_runtime_ns(time_spent);
    _release_worker_slot(task->query_ctx_raw());
}

void MultiCoreTaskQueue::release_task(PipelineTask* task) {
    _release_worker_slot(task->query_ctx_raw());
}

void MultiCoreTaskQueue::close() {
    std::unique_lock<std::mutex> lock(_mutex);
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
