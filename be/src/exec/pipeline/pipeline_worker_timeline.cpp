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

#include "exec/pipeline/pipeline_worker_timeline.h"

#include <utility>

#include "common/config.h"

namespace doris {

const char* to_string(WorkerReleaseReason reason) {
    switch (reason) {
    case WorkerReleaseReason::PUT_BACK:
        return "PUT_BACK";
    case WorkerReleaseReason::FINALIZED:
        return "FINALIZED";
    case WorkerReleaseReason::CANCELED:
        return "CANCELED";
    case WorkerReleaseReason::EXECUTED:
        return "EXECUTED";
    case WorkerReleaseReason::CLOSED:
        return "CLOSED";
    }
    return "UNKNOWN";
}

void PipelineWorkerTimeline::add(WorkerScheduleRecord record) {
    const int64_t max_records = config::max_pipeline_worker_timeline_records_per_query;
    if (max_records >= 0 && _size.fetch_add(1, std::memory_order_relaxed) >= max_records) {
        _dropped.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    _records.enqueue(std::move(record));
}

std::vector<WorkerScheduleRecord> PipelineWorkerTimeline::drain() {
    std::vector<WorkerScheduleRecord> res;
    res.reserve(_records.size_approx());
    WorkerScheduleRecord record;
    while (_records.try_dequeue(record)) {
        res.push_back(std::move(record));
    }
    return res;
}

} // namespace doris
