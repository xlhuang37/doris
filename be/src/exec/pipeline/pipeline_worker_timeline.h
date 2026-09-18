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

#include <concurrentqueue.h>

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

namespace doris {

// Why a pipeline worker gave the task back. Recorded so that iterations which never
// executed the task can be told apart from real execution when reading the timeline.
enum class WorkerReleaseReason {
    // The task was already running on another worker, so it was pushed back immediately.
    PUT_BACK,
    // The task was already finalized, nothing to do.
    FINALIZED,
    // The fragment was canceled, the task was closed instead of executed.
    CANCELED,
    // The task was executed and is still alive afterwards.
    EXECUTED,
    // The task was executed and then closed because it finished or failed.
    CLOSED,
};

const char* to_string(WorkerReleaseReason reason);

// One interval during which a single pipeline worker held one pipeline task,
// from the moment the task was taken out of the queue until it was released.
struct WorkerScheduleRecord {
    // Name of the scheduler this worker belongs to. Together with `worker_index` it
    // identifies the worker, which matters because a query may be served by more than
    // one thread pool (see HybridTaskScheduler).
    std::string scheduler;
    int32_t worker_index = -1;
    int32_t fragment_id = -1;
    int32_t pipeline_id = -1;
    std::string task_name;
    // Monotonic microseconds local to this BE.
    int64_t take_us = 0;
    int64_t release_us = 0;
    WorkerReleaseReason reason = WorkerReleaseReason::EXECUTED;
};

// Per query collector of worker/task intervals. Written by every pipeline worker that
// picks up a task of the query and drained once when the query profile is reported.
class PipelineWorkerTimeline {
public:
    // Appends a record unless the per query cap has been reached, in which case the
    // record is counted as dropped.
    void add(WorkerScheduleRecord record);

    // Moves out everything collected so far.
    std::vector<WorkerScheduleRecord> drain();

    int64_t dropped() const { return _dropped.load(std::memory_order_relaxed); }

private:
    moodycamel::ConcurrentQueue<WorkerScheduleRecord> _records;
    std::atomic<int64_t> _size {0};
    std::atomic<int64_t> _dropped {0};
};

} // namespace doris
