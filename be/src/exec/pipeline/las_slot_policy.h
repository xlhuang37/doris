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

#include <algorithm>
#include <string_view>

namespace doris {

// How a pipeline worker consumes the shared slot array that the scheduler publishes
// (slot 0 holds the least-attained query, i.e. the most LAS one). Pure policy: no
// locks, no queues, no knowledge of PipelineTask, so it can be reasoned about and
// unit-tested on its own.
enum class LasSlotPolicy {
    // Every worker walks the array from slot 0 to the last slot and takes the first
    // task it finds. Parallelism per query is whatever the array order leaves over, so
    // the least-attained query is served by as many workers as it can keep busy.
    ORDERED,
    // Every worker is pinned to one slot and only ever serves that slot, with the
    // workers spread evenly over the array. Per-query parallelism is therefore fixed
    // at workers-per-slot regardless of how much work the query has queued.
    FIXED,
};

inline bool is_las_slot_policy_name(std::string_view name) {
    return name == "ordered" || name == "fixed";
}

// Unknown names fall back to ORDERED; the config validator rejects them up front, so
// this only matters for a value that slipped through (e.g. a test writing directly).
inline LasSlotPolicy parse_las_slot_policy(std::string_view name) {
    return name == "fixed" ? LasSlotPolicy::FIXED : LasSlotPolicy::ORDERED;
}

// The array always has at least one slot, so the least-attained query is staffed even
// if the knob is set to nonsense.
inline int clamp_las_slot_count(int requested) {
    return std::max(requested, 1);
}

// The slot worker `worker_id` serves under LasSlotPolicy::FIXED, or -1 when the worker
// is out of range. Workers are cut into contiguous groups, low worker ids on the low
// (most-LAS) slots: 32 workers over 4 slots means 8 workers per slot. When the pool does
// not divide evenly the leftover workers go to the lowest slots, one each, so the extra
// capacity lands on the least-attained queries. With fewer workers than slots only the
// low slots are served, which again keeps the most-LAS queries staffed instead of leaving
// gaps between the served slots.
inline int las_slot_of_worker(int worker_id, int slot_count, int worker_count) {
    if (slot_count <= 0 || worker_count <= 0 || worker_id < 0 || worker_id >= worker_count) {
        return -1;
    }
    if (worker_count < slot_count) {
        return worker_id;
    }
    const int group = worker_count / slot_count;
    const int leftover = worker_count % slot_count;
    // The `leftover` lowest slots take `group + 1` workers, the rest take `group`.
    const int workers_in_big_groups = leftover * (group + 1);
    if (worker_id < workers_in_big_groups) {
        return worker_id / (group + 1);
    }
    return leftover + (worker_id - workers_in_big_groups) / group;
}

} // namespace doris
