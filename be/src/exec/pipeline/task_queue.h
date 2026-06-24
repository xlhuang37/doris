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

#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <array>
#include <atomic>
#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "exec/pipeline/pipeline_task.h"

namespace doris {
class QueryContext;
class QueryCpuLease;
class CpuLeaseGrantor;
#include "common/compile_check_begin.h"

// A global, query-granular Multilevel Feedback Queue with strict absolute priority
// between levels, shared by all workers of one pipeline scheduler (one per workload
// group). It replaces the old per-core sharded design so that every worker agrees
// on the globally highest-priority query to serve next, instead of each worker only
// seeing its own shard.
//
// Structure:
//   - Absolute-priority levels. A query's level is derived from its QueryContext-global
//     CPU runtime (QueryContext::query_runtime_counter, surfaced via
//     PipelineTask::query_runtime_ns), so a query is demoted as a whole the more CPU it
//     consumes across all of its fragments/instances and across the pipeline/scan
//     schedulers. Legacy mode uses runtime thresholds ({1s,3s,10s,60s,300s}); the
//     optional CPU-lease mode (config::enable_cpu_lease_scheduling) instead levels by
//     parallelism layer (running slots) + CPU band and admits at most N queries per
//     workload group via a shared CpuLeaseGrantor.
//   - Each level holds query "nodes" in a round-robin list. Each node owns a FIFO of
//     that query's runnable PipelineTasks. Within a level, nodes are served
//     round-robin so workers spread across co-resident queries rather than dogpiling
//     one; tasks within a node are FIFO.
//
// Selection (take):
//   1. Locality: a worker prefers to keep serving the query it last served, as long
//      as that query is still at the lowest non-empty level and under its lease.
//   2. Otherwise pick, by strict absolute priority, the lowest non-empty level and
//      round-robin a query in it.
//   3. Lazy demotion: a node's level is recomputed when inspected; if the query has
//      crossed a threshold, the whole node (all its tasks) moves to the deeper level.
//
// The class name and public interface are kept identical to the previous per-core
// implementation so the scheduler/worker loop is unchanged.
class MultiCoreTaskQueue {
public:
    explicit MultiCoreTaskQueue(int core_size);

#ifndef BE_TEST
    ~MultiCoreTaskQueue();
    // Get the next task for the worker `core_id`.
    PipelineTaskSPtr take(int core_id);
#else
    virtual ~MultiCoreTaskQueue();
    virtual PipelineTaskSPtr take(int core_id);
#endif

    void close();

    // `core_id` is accepted for API compatibility but no longer pins the task to a
    // shard: placement is global and driven by the owning query's level.
    Status push_back(PipelineTaskSPtr task);
    Status push_back(PipelineTaskSPtr task, int core_id);

    // Charge executed CPU time to the owning query's global counter (drives demotion)
    // and release the worker slot the task held.
    void update_statistics(PipelineTask* task, int64_t time_spent);

    // Release the worker slot a task held without charging runtime. Used when a
    // dequeued task turns out to be already running on another worker and is
    // re-queued without being executed.
    void release_task(PipelineTask* task);

    int cores() const { return _core_size; }

    // Wire the per-workload-group CPU-lease admission controller. Shared by both inner
    // schedulers of a WG. When set and config::enable_cpu_lease_scheduling is on, the
    // queue switches to lease mode: parallelism-layer + CPU-band leveling and grantor
    // admission. Default (nullptr) keeps the legacy runtime-threshold global MLFQ.
    void set_grantor(CpuLeaseGrantor* grantor) { _grantor = grantor; }

protected:
    // Legacy runtime-threshold leveling uses the first LEGACY_LEVELS levels; the lease
    // mode uses up to QueryCpuLease::kNumLevels. The level arrays are sized to the max.
    static constexpr int LEGACY_LEVELS = 6;
    static constexpr int NUM_LEVELS = 12; // >= QueryCpuLease::kNumLevels and LEGACY_LEVELS

    // One bucket per runnable query. Lives in `_nodes` for the query's lifetime in
    // the queue; it is linked into exactly one level list while it has runnable
    // tasks, and unlinked (but kept in `_nodes`) while it has none but still has
    // in-flight workers.
    struct QueryNode {
        QueryContext* key = nullptr;
        QueryCpuLease* lease = nullptr; // owned by QueryContext; null for query-less tasks
        int level = 0;
        bool linked = false;
        int in_flight = 0;
        std::queue<PipelineTaskSPtr> runnable;
        std::list<QueryNode*>::iterator pos {};
    };

    // Single-attempt take with an explicit wait timeout. Returns nullptr if no task
    // becomes available within `timeout_ms` (or the queue is closed).
    PipelineTaskSPtr _take(int worker_id, uint32_t timeout_ms);

private:
    bool _lease_mode() const;
    // Legacy runtime-threshold level.
    int _compute_level(uint64_t runtime) const;
    // Level for a node: lease priority (layer + CPU band) in lease mode, else legacy.
    int _node_level(const QueryNode* node) const;
    int _lowest_non_empty_level() const;
    int _lease(const QueryNode* node) const;
    QueryNode* _ensure_node(QueryContext* key);
    void _link(QueryNode* node, int level);
    void _unlink(QueryNode* node);
    void _relevel_locked(QueryNode* node);
    uint64_t _node_runtime(const QueryNode* node) const;
    // Whether the grantor admits this node's query (and admits it if there is capacity).
    bool _admitted(const QueryNode* node);
    PipelineTaskSPtr _pop_from_node(QueryNode* node, int worker_id);
    PipelineTaskSPtr _try_take_unprotected(int worker_id);
    Status _push(PipelineTaskSPtr task);
    void _release_worker_slot(QueryContext* key);

    std::mutex _mutex;
    std::condition_variable _wait_task;
    bool _closed = false;

    std::array<std::list<QueryNode*>, NUM_LEVELS> _levels;
    std::unordered_map<QueryContext*, std::unique_ptr<QueryNode>> _nodes;

    // The query each worker last served, used to preserve query locality.
    std::vector<QueryContext*> _worker_sticky;

    std::atomic<size_t> _total_task_size = 0;

    // Per-WG CPU-lease admission controller (nullptr => legacy mode). Not owned.
    CpuLeaseGrantor* _grantor = nullptr;

    int _core_size;
    // 1s, 3s, 10s, 60s, 300s
    uint64_t _queue_level_limit[LEGACY_LEVELS - 1] = {1000000000, 3000000000, 10000000000,
                                                      60000000000, 300000000000};
    static constexpr auto WAIT_CORE_TASK_TIMEOUT_MS = 100;
};
#include "common/compile_check_end.h"
} // namespace doris
