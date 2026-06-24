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
#include <array>
#include <atomic>
#include <cstdint>
#include <limits>

#include "common/config.h"

namespace doris {

// Per-query CPU-lease state for the ClickHouse-style lease scheduling layer (one per
// QueryContext). It tracks how many pipeline worker slots the query currently holds and
// computes the query's MLFQ priority from (a) its parallelism layer and (b) a CPU
// consumption band, mirroring ClickHouse's CPULeaseAllocation::computeRequestPriority +
// MultiLevelFeedbackQueue::pickCpuBand.
//
// Doris workers are cooperative and interchangeable, so we do not model per-OS-thread
// leases/parking; "slots" are just a count of workers currently serving the query, and
// "preemption" happens naturally when the query is re-leveled below another admitted
// query at the next task boundary. The same running-slot count drives scanner bundling
// (scan concurrency = running_slots * scan_threads_per_slot).
class QueryCpuLease {
public:
    // Parallelism layers and CPU bands within a layer (ported from ClickHouse). Total
    // MLFQ levels = kNumLayers * kLayerWidth.
    static constexpr int kNumLayers = 4;
    static constexpr int kLayerWidth = 4;
    static constexpr int kNumLevels = kNumLayers * kLayerWidth; // 16

    QueryCpuLease() = default;

    int running_slots() const { return _running_slots.load(std::memory_order_relaxed); }
    void acquire_slot() { _running_slots.fetch_add(1, std::memory_order_relaxed); }
    void release_slot() {
        // Guard against underflow if release/acquire ever race during teardown.
        int prev = _running_slots.load(std::memory_order_relaxed);
        while (prev > 0 &&
               !_running_slots.compare_exchange_weak(prev, prev - 1, std::memory_order_relaxed)) {
        }
    }

    // MLFQ level for this query (lower value = higher priority). `consumed_ns` is the
    // query's cumulative CPU (the shared QueryContext::_query_runtime_ns).
    int compute_priority(uint64_t consumed_ns) const {
        int leveling = config::cpu_lease_leveling_slots > 0 ? config::cpu_lease_leveling_slots : 8;
        int layer = std::min(running_slots() / leveling, kNumLayers - 1);
        int band = pick_cpu_band(consumed_ns);
        return layer * kLayerWidth + band;
    }

    // CPU consumption band [0, kLayerWidth): smaller = less CPU used = higher priority.
    static int pick_cpu_band(uint64_t cumulative_cpu_ns) {
        // Thresholds in ns; the last is a catch-all. Ported from ClickHouse
        // kCpuBandThresholdsNs (~6s / ~25s / ~100s / inf).
        static constexpr std::array<uint64_t, kLayerWidth> kThresholds = {
                6'296'000'000ULL, 25'004'000'000ULL, 100'016'000'000ULL,
                std::numeric_limits<uint64_t>::max()};
        for (int i = 0; i < kLayerWidth; ++i) {
            if (cumulative_cpu_ns < kThresholds[i]) {
                return i;
            }
        }
        return kLayerWidth - 1;
    }

private:
    // Number of pipeline workers currently serving this query (acquired - released).
    std::atomic<int> _running_slots {0};
};

} // namespace doris
