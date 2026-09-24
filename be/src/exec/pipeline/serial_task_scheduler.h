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

#include <memory>
#include <string>
#include <utility>

#include "exec/pipeline/serial_task_queue.h"
#include "exec/pipeline/task_scheduler.h"

namespace doris {

class SerialTaskScheduler final : public TaskScheduler {
public:
    SerialTaskScheduler(int thread_num, std::string name, std::shared_ptr<CgroupCpuCtl> cgroup_cpu_ctl)
            : TaskScheduler(thread_num, std::move(name), std::move(cgroup_cpu_ctl)) {}

    Status register_fragment(const SerialFragmentInfo& info) override {
        return _serial_queue.register_fragment(info);
    }

    void notify_pipeline_finished(const TUniqueId& query_id, int fragment_id,
                                  PipelineId pipeline_id, PipelineFragmentContext* ctx) override;

    void notify_query_finished(const TUniqueId& query_id) override {
        _serial_queue.on_query_finished(query_id);
    }

protected:
    PipelineTaskSPtr _take_task(int index) override { return _serial_queue.take(index); }

    Status _push_task(PipelineTaskSPtr task) override { return _serial_queue.push_back(task); }

    Status _push_task(PipelineTaskSPtr task, int core_id) override {
        return _serial_queue.push_back(task, core_id);
    }

    void _close_queue() override { _serial_queue.close(); }

    void _update_statistics(PipelineTask* task, int64_t time_spent) override {
        _serial_queue.update_statistics(task, time_spent);
    }

private:
    SerialTaskQueue _serial_queue;
};

} // namespace doris
