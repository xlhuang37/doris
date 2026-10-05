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
#include <vector>

namespace doris {
#include "common/compile_check_begin.h"

// Empirical distribution of final query CPU times, used to rank running queries by
// (an approximation of) their Gittins index.
//
// Slot i covers final CPU times in [i, i + 1) slot widths; times past the last slot
// are clamped into it, so tail mass still counts as "survives at least this long".
// Counts are kept as prefix sums so the completions in any slot range are an O(1)
// difference.
//
// The index of a query whose attained service lands in slot `a` is
//     max over d in {1, 2, 4, ...}, a + d <= N, of
//         P(final in [a, a + d) | final >= a) / d
// i.e. the best "probability of finishing per unit of further service" over
// exponentially growing lookaheads. It is 0 when no recorded query got as far as
// slot `a`, or when no lookahead fits in the array.
//
// Not thread-safe: owned and used by a single (scheduler) thread.
class GittinsHistogram {
public:
    explicit GittinsHistogram(int num_slots, uint64_t slot_width_ns = 1'000'000'000ULL)
            : _num_slots(static_cast<size_t>(std::max(num_slots, 1))),
              _slot_width_ns(std::max<uint64_t>(slot_width_ns, 1)),
              _prefix(_num_slots + 1, 0),
              _index_by_slot(_num_slots, 0.0) {}

    void record(uint64_t final_ns) {
        const size_t slot = _slot_of(final_ns);
        for (size_t i = slot + 1; i <= _num_slots; ++i) {
            ++_prefix[i];
        }
        _rebuild_index();
    }

    double index(uint64_t attained_ns) const { return _index_by_slot[_slot_of(attained_ns)]; }

    double index_of_slot(size_t slot) const { return _index_by_slot[slot]; }

    uint64_t total_samples() const { return _prefix[_num_slots]; }

    size_t num_slots() const { return _num_slots; }

private:
    size_t _slot_of(uint64_t ns) const {
        return static_cast<size_t>(std::min<uint64_t>(ns / _slot_width_ns, _num_slots - 1));
    }

    void _rebuild_index() {
        const uint64_t total = _prefix[_num_slots];
        for (size_t a = 0; a < _num_slots; ++a) {
            const uint64_t survivors = total - _prefix[a];
            double best = 0.0;
            if (survivors > 0) {
                for (size_t d = 1; a + d <= _num_slots; d <<= 1) {
                    const uint64_t finished = _prefix[a + d] - _prefix[a];
                    const double idx = static_cast<double>(finished) /
                                       static_cast<double>(survivors) / static_cast<double>(d);
                    best = std::max(best, idx);
                }
            }
            _index_by_slot[a] = best;
        }
    }

    const size_t _num_slots;
    const uint64_t _slot_width_ns;
    // _prefix[i] = number of recorded queries whose final time falls in slots [0, i).
    std::vector<uint64_t> _prefix;
    std::vector<double> _index_by_slot;
};

#include "common/compile_check_end.h"
} // namespace doris
