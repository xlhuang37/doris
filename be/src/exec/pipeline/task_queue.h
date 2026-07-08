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
#include "exec/pipeline/priorityconcurrentqueue.h"
#include "exec/pipeline/pipeline_task.h"

namespace doris {
class QueryContext;
#include "common/compile_check_begin.h"

// A global, query-granular Multilevel Feedback Queue with strict absolute priority
// between levels, shared by all workers of one pipeline scheduler (one per workload
// group). It is a thin wrapper around a lock-free multi-priority-level concurrent
// queue (moodycamel_pri::ConcurrentQueue), so that enqueue/dequeue of different queries
// proceed without a global lock.
//
// Structure:
//   - SUB_QUEUE_LEVEL absolute-priority levels (level 0 highest). The underlying
//     concurrent queue drains higher-priority levels before lower ones and, within a
//     level, spreads consumers across producers to reduce contention.
//   - Each query owns exactly one producer sub-queue, represented by a
//     moodycamel_pri::ProducerToken kept in a per-query QueryNode. All of a query's
//     runnable PipelineTasks are enqueued through that token, so a query moves
//     between levels as a whole.
//   - A query's level is derived from its QueryContext-global CPU runtime
//     (QueryContext::query_runtime_counter, surfaced via PipelineTask::query_runtime_ns),
//     so a query is demoted the more CPU it consumes across all of its fragments.
//
// Concurrency:
//   - Enqueue takes only the owning query's per-node enqueue mutex (the sub-queue is
//     single-producer); enqueues of different queries never contend.
//   - Dequeue (take) is lock-free via the concurrent queue's try_dequeue; workers
//     only touch a condition variable on the idle (empty) path.
//   - Demotion runs from update_statistics: after charging CPU time we recompute the
//     query's level and, if it dropped, move its producer with demote_producer under
//     the node's demotion mutex (which does not exclude concurrent enqueues).
//
// The class name and public interface are kept identical to the previous
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

    // Charge executed CPU time to the owning query's global counter and demote the
    // query's producer to a deeper level if its accumulated runtime crossed a
    // threshold.
    void update_statistics(PipelineTask* task, int64_t time_spent);

    // No-op retained for API compatibility (the queue no longer tracks per-query
    // in-flight workers). Used when a dequeued task turns out to be already running
    // on another worker and is re-queued without being executed.
    void release_task(PipelineTask* task);

    // Drop the per-query state (and its ProducerToken) for a finished query. Called
    // when the owning QueryContext is destroyed, so no more tasks can be enqueued for
    // it. Freeing the node lets the concurrent queue recycle the priority-level slot.
    void remove_query(QueryContext* key);

    int cores() const { return _core_size; }

protected:
    static constexpr int SUB_QUEUE_LEVEL = 4;

    struct Traits : public moodycamel_pri::ConcurrentQueueDefaultTraits {
        static const size_t PRIORITY_LEVELS = static_cast<size_t>(SUB_QUEUE_LEVEL);
    };
    using QueueT = moodycamel_pri::ConcurrentQueue<PipelineTaskSPtr, Traits>;

    // Per-query state, kept in `_nodes` for the query's lifetime in the queue.
    struct QueryNode {
        explicit QueryNode(QueueT& queue) : token(queue) {}

        QueryContext* key = nullptr;
        // One producer sub-queue for this query; all of its tasks are enqueued here.
        moodycamel_pri::ProducerToken token;
        // Serializes this query's enqueues (the sub-queue is single-producer).
        std::mutex enqueue_mutex;
        // Serializes demotions of this producer (does not exclude enqueues).
        std::mutex demotion_mutex;
        // The priority level the producer currently sits at.
        std::atomic<int> current_level {0};
    };

    // Single-attempt take with an explicit wait timeout. Returns nullptr if no task
    // becomes available within `timeout_ms` (or the queue is closed).
    PipelineTaskSPtr _take(int worker_id, uint32_t timeout_ms);

private:
    int _compute_level(uint64_t runtime) const;
    QueryNode* _ensure_node(QueryContext* key);
    Status _push(PipelineTaskSPtr task);

    QueueT _queue;

    // Guards `_nodes` for lookup/create/erase only; not held during queue ops.
    std::mutex _map_mutex;
    std::unordered_map<QueryContext*, std::unique_ptr<QueryNode>> _nodes;

    // Used only to park/wake idle workers when the queue is empty.
    std::mutex _wait_mutex;
    std::condition_variable _wait_task;
    std::atomic<bool> _closed = false;

    std::atomic<size_t> _total_task_size = 0;

    int _core_size;
    // 1s, 3s, 10s, 60s, 300s
    uint64_t _queue_level_limit[SUB_QUEUE_LEVEL - 1] = {2800000000, 10000000000,
                                                        25000000000};
    static constexpr auto WAIT_CORE_TASK_TIMEOUT_MS = 100;
};
#include "common/compile_check_end.h"
} // namespace doris
