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

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <ostream>
#include <queue>
#include <set>

#include "common/status.h"
#include "exec/pipeline/pipeline_task.h"

namespace doris {
#include "common/compile_check_begin.h"

class SubTaskQueue {
    friend class PriorityTaskQueue;

public:
    void push_back(PipelineTaskSPtr task) { _queue.emplace(task); }

    PipelineTaskSPtr try_take(bool is_steal);

    bool empty() { return _queue.empty(); }

private:
    std::queue<PipelineTaskSPtr> _queue;
};

// A Multilevel Feedback Queue with strict absolute priority between levels.
//
// Level 0 is the inelastic-first level: a task whose owning fragment is currently
// inelastic (few runnable pipelines, see PipelineFragmentContext::is_inelastic)
// is placed here so the fragment finishes quickly and frees resources. Levels
// 1..6 are the runtime levels: a task's level is derived from its fragment's
// global CPU runtime (see PipelineFragmentContext::fragment_runtime_ns), so a
// fragment is demoted as a whole the more CPU it consumes. Workers always drain
// the lowest non-empty level before serving a deeper one; within a level
// ordering is FIFO.
//
// Both the inelastic boost and the runtime demotion are applied lazily at
// dequeue: when a task is pulled its target level is recomputed (the fragment may
// have turned elastic, or its runtime may have grown, on any core while the task
// waited), and if it now belongs in a deeper level it is re-queued there instead
// of being run.
class PriorityTaskQueue {
public:
    PriorityTaskQueue();

    void close();

    PipelineTaskSPtr try_take(bool is_steal);

    PipelineTaskSPtr take(uint32_t timeout_ms = 0);

    Status push(PipelineTaskSPtr task);

private:
    PipelineTaskSPtr _try_take_unprotected(bool is_steal);
    // Level 0 is reserved for inelastic-first tasks; runtime-demoted tasks occupy
    // levels [RUNTIME_LEVEL_BASE, SUB_QUEUE_LEVEL).
    static constexpr size_t INELASTIC_LEVEL = 0;
    static constexpr size_t RUNTIME_LEVEL_BASE = INELASTIC_LEVEL + 1;
    static constexpr size_t NUM_RUNTIME_LEVELS = 6;
    static constexpr size_t SUB_QUEUE_LEVEL = RUNTIME_LEVEL_BASE + NUM_RUNTIME_LEVELS;
    SubTaskQueue _sub_queues[SUB_QUEUE_LEVEL];
    // Thresholds between the runtime levels: 1s, 3s, 10s, 60s, 300s.
    uint64_t _queue_level_limit[NUM_RUNTIME_LEVELS - 1] = {1000000000, 3000000000, 10000000000,
                                                           60000000000, 300000000000};
    std::mutex _work_size_mutex;
    std::condition_variable _wait_task;
    std::atomic<size_t> _total_task_size = 0;
    bool _closed;

    // Runtime-only level in [RUNTIME_LEVEL_BASE, SUB_QUEUE_LEVEL).
    int _compute_level(uint64_t real_runtime);
    // Final target level for a task: INELASTIC_LEVEL when its fragment is
    // inelastic, otherwise its runtime level.
    int _target_level(const PipelineTaskSPtr& task);
};

// Need consider NUMA architecture
class MultiCoreTaskQueue {
public:
    explicit MultiCoreTaskQueue(int core_size);

#ifndef BE_TEST
    ~MultiCoreTaskQueue();
    // Get the task by core id.
    PipelineTaskSPtr take(int core_id);
#else
    virtual ~MultiCoreTaskQueue();
    virtual PipelineTaskSPtr take(int core_id);
#endif
    void close();

    // TODO combine these methods to `push_back(task, core_id = -1)`
    Status push_back(PipelineTaskSPtr task);

    Status push_back(PipelineTaskSPtr task, int core_id);

    void update_statistics(PipelineTask* task, int64_t time_spent);

    int cores() const { return _core_size; }

private:
    PipelineTaskSPtr _steal_take(int core_id);

    std::vector<PriorityTaskQueue> _prio_task_queues;
    std::atomic<uint32_t> _next_core = 0;
    std::atomic<bool> _closed;

    int _core_size;
    static constexpr auto WAIT_CORE_TASK_TIMEOUT_MS = 100;
};
#include "common/compile_check_end.h"
} // namespace doris
