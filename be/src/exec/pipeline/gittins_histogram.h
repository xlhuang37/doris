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

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <vector>

namespace doris {
#include "common/compile_check_begin.h"

// Immutable per-slot Gittins index table, built from a GittinsHistogram snapshot.
// Safe to read from any thread once published.
//
// The index of a query whose attained service lands in slot `a` is
//     max over d in {1, 2, 4, ...}, a + d <= N, of
//         P(final in [a, a + d) | final >= a) / d
// i.e. the best "probability of finishing per unit of further service" over
// exponentially growing lookaheads. It is 0 when no recorded query got as far as
// slot `a`, or when no lookahead fits in the array.
class GittinsIndexTable {
public:
    // `prefix[i]` = number of samples whose final time falls in slots [0, i); size N+1.
    GittinsIndexTable(const std::vector<uint64_t>& prefix, uint64_t slot_width_ns)
            : _slot_width_ns(std::max<uint64_t>(slot_width_ns, 1)),
              _total_samples(prefix.back()),
              _index_by_slot(prefix.size() - 1, 0.0) {
        const size_t num_slots = _index_by_slot.size();
        for (size_t a = 0; a < num_slots; ++a) {
            const uint64_t survivors = _total_samples - prefix[a];
            if (survivors == 0) {
                continue;
            }
            double best = 0.0;
            for (size_t d = 1; a + d <= num_slots; d <<= 1) {
                const uint64_t finished = prefix[a + d] - prefix[a];
                const double idx = static_cast<double>(finished) /
                                   static_cast<double>(survivors) / static_cast<double>(d);
                best = std::max(best, idx);
            }
            _index_by_slot[a] = best;
        }
    }

    double index(uint64_t attained_ns) const {
        const auto slot = static_cast<size_t>(
                std::min<uint64_t>(attained_ns / _slot_width_ns, _index_by_slot.size() - 1));
        return _index_by_slot[slot];
    }

    double index_of_slot(size_t slot) const { return _index_by_slot[slot]; }

    uint64_t total_samples() const { return _total_samples; }

    size_t num_slots() const { return _index_by_slot.size(); }

private:
    const uint64_t _slot_width_ns;
    const uint64_t _total_samples;
    std::vector<double> _index_by_slot;
};

// Empirical distribution of final query CPU times, used to rank running queries by
// (an approximation of) their Gittins index.
//
// Slot i covers final CPU times in [i, i + 1) slot widths; times past the last slot
// are clamped into it, so tail mass still counts as "survives at least this long".
//
// record() may be called from any thread. build() takes a relaxed snapshot of the
// counts; a sample racing with it lands in either this build or the next, and the
// snapshot is always self-consistent because the total is derived from it.
class GittinsHistogram {
public:
    explicit GittinsHistogram(int num_slots, uint64_t slot_width_ns = 1'000'000'000ULL)
            : _slot_width_ns(std::max<uint64_t>(slot_width_ns, 1)),
              _counts(static_cast<size_t>(std::max(num_slots, 1))) {}

    void record(uint64_t final_ns) {
        const auto slot = static_cast<size_t>(
                std::min<uint64_t>(final_ns / _slot_width_ns, _counts.size() - 1));
        _counts[slot].fetch_add(1, std::memory_order_relaxed);
        _version.fetch_add(1, std::memory_order_release);
    }

    // Bumped by every record(); lets a rebuilder skip work when nothing changed.
    uint64_t version() const { return _version.load(std::memory_order_acquire); }

    std::shared_ptr<const GittinsIndexTable> build() const {
        std::vector<uint64_t> prefix(_counts.size() + 1, 0);
        for (size_t i = 0; i < _counts.size(); ++i) {
            prefix[i + 1] = prefix[i] + _counts[i].load(std::memory_order_relaxed);
        }
        return std::make_shared<const GittinsIndexTable>(prefix, _slot_width_ns);
    }

    size_t num_slots() const { return _counts.size(); }

private:
    const uint64_t _slot_width_ns;
    std::vector<std::atomic<uint64_t>> _counts;
    std::atomic<uint64_t> _version {0};
};

#include "common/compile_check_end.h"
} // namespace doris
