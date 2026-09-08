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

#include "exec/pipeline/serial_task_scheduler.h"

#include "common/logging.h"
#include "exec/pipeline/pipeline_fragment_context.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {
#include "common/compile_check_begin.h"

void SerialTaskScheduler::notify_pipeline_finished(const TUniqueId& query_id, int fragment_id,
                                                   PipelineId pipeline_id,
                                                   PipelineFragmentContext* ctx) {
    PipelineKey key {query_id, fragment_id, pipeline_id};
    int64_t start_ns = _serial_queue.wallclock_start_ns(key);
    int64_t end_ns = MonotonicNanos();
    int64_t elapsed = start_ns > 0 && end_ns >= start_ns ? end_ns - start_ns : 0;

    if (ctx != nullptr) {
        ctx->record_pipeline_wallclock(pipeline_id, start_ns, end_ns, elapsed);
    }
    LOG(INFO) << "serial pipeline wallclock query_id=" << print_id(query_id)
              << " fragment_id=" << fragment_id << " pipeline_id=" << pipeline_id
              << " wall_clock_ns=" << elapsed;

    _serial_queue.on_pipeline_finished(key);
}

#include "common/compile_check_end.h"
} // namespace doris
