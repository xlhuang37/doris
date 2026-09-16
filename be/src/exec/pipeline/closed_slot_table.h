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
#include <cstddef>
#include <deque>
#include <optional>
#include <vector>

namespace doris {
#include "common/compile_check_begin.h"

// Which query owns which slot, and which slot a worker serves, for the closed-system
// pipeline partition: the pool's workers are cut into `slot_count` contiguous groups and
// each group serves exactly one query. Queries that arrive when every slot is taken wait
// in arrival order and are admitted as slots free up.
//
// Pure policy: no locks, no queues, no knowledge of PipelineTask. The owner calls it
// under its own mutex and publishes the returned slot changes to the workers, which is
// also what makes it unit-testable with plain integer keys.
template <typename Key>
class ClosedSlotTable {
public:
    // `worker_count` is the pool size, and therefore also the largest usable slot count
    // (one worker per slot). The table starts with one slot and no queries.
    explicit ClosedSlotTable(int worker_count)
            : _worker_count(std::max(worker_count, 1)), _owners(static_cast<size_t>(_worker_count)) {}

    int worker_count() const { return _worker_count; }
    int slot_count() const { return _slot_count; }

    // Workers are split into contiguous groups, low worker ids first. Exact when the
    // worker count divides evenly (32 workers, 4 slots -> 8 workers per slot); otherwise
    // the remainder is spread over the low slots.
    int slot_of_worker(int worker_id) const {
        if (worker_id < 0 || worker_id >= _worker_count) {
            return -1;
        }
        return worker_id * _slot_count / _worker_count;
    }

    // How many workers currently serve `slot`. Only used for logging and tests.
    int workers_of_slot(int slot) const {
        if (slot < 0 || slot >= _slot_count) {
            return 0;
        }
        int count = 0;
        for (int worker = 0; worker < _worker_count; ++worker) {
            if (slot_of_worker(worker) == slot) {
                count++;
            }
        }
        return count;
    }

    // Queue `key` for admission. No-op if it already holds a slot or is already waiting,
    // so a query that is pushed to repeatedly keeps its original arrival position.
    void add(const Key& key) {
        if (slot_of(key) >= 0) {
            return;
        }
        if (std::find(_waiting.begin(), _waiting.end(), key) != _waiting.end()) {
            return;
        }
        _waiting.push_back(key);
    }

    // Drop `key` from the table, whether it was bound or still waiting. Returns the slot
    // it vacated, or -1 if it held none.
    int remove(const Key& key) {
        _waiting.erase(std::remove(_waiting.begin(), _waiting.end(), key), _waiting.end());
        const int slot = slot_of(key);
        if (slot >= 0) {
            _owners[static_cast<size_t>(slot)].reset();
        }
        return slot;
    }

    // Apply `requested_slots` and fill every free slot from the waiting queue. Slots that
    // fall outside a shrunk partition give their query back to the front of the waiting
    // queue, so it is re-admitted ahead of later arrivals. Every slot whose owner changed
    // is appended to `changed` for the caller to republish; `changed` is never cleared.
    void rebind(int requested_slots, std::vector<int>* changed) {
        const int slots = std::clamp(requested_slots, 1, _worker_count);
        if (slots < _slot_count) {
            // Reverse order so the evicted queries keep their relative arrival order once
            // they are all at the front of the waiting queue.
            for (int slot = _slot_count - 1; slot >= slots; --slot) {
                auto& owner = _owners[static_cast<size_t>(slot)];
                if (owner.has_value()) {
                    _waiting.push_front(*owner);
                    owner.reset();
                    changed->push_back(slot);
                }
            }
        }
        _slot_count = slots;
        for (int slot = 0; slot < _slot_count && !_waiting.empty(); ++slot) {
            auto& owner = _owners[static_cast<size_t>(slot)];
            if (!owner.has_value()) {
                owner = _waiting.front();
                _waiting.pop_front();
                changed->push_back(slot);
            }
        }
    }

    // The slot `key` owns, or -1 when it is waiting or unknown.
    int slot_of(const Key& key) const {
        for (int slot = 0; slot < _slot_count; ++slot) {
            const auto& owner = _owners[static_cast<size_t>(slot)];
            if (owner.has_value() && *owner == key) {
                return slot;
            }
        }
        return -1;
    }

    const std::optional<Key>& owner_of(int slot) const {
        if (slot < 0 || slot >= _slot_count) {
            return _no_owner;
        }
        return _owners[static_cast<size_t>(slot)];
    }

    size_t waiting_size() const { return _waiting.size(); }

private:
    const int _worker_count;
    int _slot_count = 1;
    // Sized to the worker count once, so growing the slot count never reallocates.
    // Entries at or beyond `_slot_count` are always empty.
    std::vector<std::optional<Key>> _owners;
    // Queries with no slot, in arrival order (evicted owners re-enter at the front).
    std::deque<Key> _waiting;
    const std::optional<Key> _no_owner;
};

#include "common/compile_check_end.h"
} // namespace doris
