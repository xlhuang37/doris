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

#include <mutex>
#include <unordered_set>

#include "common/config.h"

namespace doris {

// Per-workload-group admission controller for CPU-lease scheduling. It bounds the number
// of concurrently scheduled queries ("main drivers") to `max_active_queries_per_group`.
// One grantor is shared by both inner pipeline schedulers (simple + blocking) of a
// workload group, so a query's admission is consistent across them.
//
// Admission is priority-driven implicitly: the task queue scans runnable queries in MLFQ
// priority order and calls try_serve() on each; the first that is already admitted (or
// can be admitted because there is free capacity) wins the worker. When a query runs out
// of runnable + in-flight work the queue calls on_idle() to free its admission slot,
// which is then refilled by the next-highest-priority waiting query on the following
// take().
//
// Keys are opaque QueryContext pointers (compared, never dereferenced here).
class CpuLeaseGrantor {
public:
    CpuLeaseGrantor() = default;

    // Returns true if `key` may be served now: true if already admitted, or if there is
    // spare admission capacity (in which case `key` is admitted). Returns false if the
    // admission set is full and `key` is not in it.
    bool try_serve(const void* key) {
        std::lock_guard<std::mutex> l(_mutex);
        if (_admitted.contains(key)) {
            return true;
        }
        int cap = config::max_active_queries_per_group;
        if (cap <= 0) {
            cap = 1;
        }
        if (static_cast<int>(_admitted.size()) < cap) {
            _admitted.insert(key);
            return true;
        }
        return false;
    }

    // Free `key`'s admission slot (call when the query has no runnable or in-flight work).
    void on_idle(const void* key) {
        std::lock_guard<std::mutex> l(_mutex);
        _admitted.erase(key);
    }

    int admitted_count() const {
        std::lock_guard<std::mutex> l(_mutex);
        return static_cast<int>(_admitted.size());
    }

private:
    mutable std::mutex _mutex;
    std::unordered_set<const void*> _admitted;
};

} // namespace doris
