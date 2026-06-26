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
#include "exec/pipeline/task_scheduler.h"

namespace doris {

class DummyTaskQueue final : public MultiCoreTaskQueue {
    explicit DummyTaskQueue(int core_size) : MultiCoreTaskQueue(core_size) {}
    ~DummyTaskQueue() override = default;
    // Use a short wait so unit tests don't block when the queue is empty.
    std::shared_ptr<PipelineTask> take(int core_id) override { return _take(core_id, 1); }
};

class MockTaskScheduler : public TaskScheduler {
public:
    MockTaskScheduler() : TaskScheduler() { _task_queue = std::make_unique<DummyTaskQueue>(1); }

    Status submit(PipelineTaskSPtr task) override { return _task_queue->push_back(task); }

    Status start() override { return Status::OK(); }

    void stop() override {}

    std::vector<std::pair<std::string, std::vector<int>>> thread_debug_info() override {
        return {};
    }

private:
    std::unique_ptr<DummyTaskQueue> _task_queue;
};
} // namespace doris
