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

#include <gen_cpp/Types_types.h>

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#include "common/status.h"
#include "exec/pipeline/closed_slot_table.h"
#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_task.h"

namespace doris {
#include "common/compile_check_begin.h"

struct TUniqueIdLess {
    bool operator()(const TUniqueId& a, const TUniqueId& b) const {
        return a.hi < b.hi || (a.hi == b.hi && a.lo < b.lo);
    }
};

struct SerialPipelineInfo {
    PipelineId pipeline_id = 0;
    bool is_exchange_source = false;
    bool is_exchange_sink = false;
    // ExchangeSource recvr plan node id. -1 if not an exchange source.
    int exchange_node_id = -1;
    // ExchangeSink dest plan node id. -1 if not an exchange sink.
    int dest_node_id = -1;
};

struct SerialFragmentInfo {
    TUniqueId query_id;
    int fragment_id = 0;
    // downstream -> upstreams (same as PipelineFragmentContext::_dag)
    std::map<PipelineId, std::vector<PipelineId>> dag;
    std::vector<SerialPipelineInfo> pipelines;
};

struct PipelineKey {
    TUniqueId query_id;
    int fragment_id = 0;
    PipelineId pipeline_id = 0;

    bool operator<(const PipelineKey& rhs) const {
        if (query_id.hi != rhs.query_id.hi) {
            return query_id.hi < rhs.query_id.hi;
        }
        if (query_id.lo != rhs.query_id.lo) {
            return query_id.lo < rhs.query_id.lo;
        }
        if (fragment_id != rhs.fragment_id) {
            return fragment_id < rhs.fragment_id;
        }
        return pipeline_id < rhs.pipeline_id;
    }

    bool operator==(const PipelineKey& rhs) const {
        return query_id == rhs.query_id && fragment_id == rhs.fragment_id &&
               pipeline_id == rhs.pipeline_id;
    }
};

// Policy object: which (query, pipeline) may run. Independent of PipelineTask so it can
// be unit-tested with fake ids.
//
// Workers are cut into slots by `ClosedSlotTable`; each slot serves one query, admitted
// in registration order, and queries beyond the slot count wait for a slot to free up.
// Inside a slot the query runs one pipeline at a time and every worker of the slot takes
// tasks only from that pipeline. Each query keeps its own current pipeline, so queries in
// different slots advance independently.
class SerialDispatchState {
public:
    explicit SerialDispatchState(int worker_count = 1) : _slot_table(worker_count) {}

    // Slot count applied on the next admission event (registration or query finish).
    void set_requested_slots(int slots) { _requested_slots = slots; }

    void register_fragment(const SerialFragmentInfo& info);
    void on_pipeline_finished(const PipelineKey& key);
    void on_query_finished(const TUniqueId& query_id);
    // For every query holding a slot: keep its current pipeline while it is unfinished,
    // otherwise pick its next ready one.
    void advance();
    // Current pipeline of the query served by `worker_id`'s slot.
    std::optional<PipelineKey> current_for_worker(int worker_id) const;
    std::optional<PipelineKey> current_of_query(const TUniqueId& query_id) const;
    // Current pipeline of slot 0; equals the single dispatch target when there is one slot.
    std::optional<PipelineKey> current() const;
    void close();
    bool closed() const { return _closed; }

    int slot_count() const { return _slot_table.slot_count(); }
    int slot_of_worker(int worker_id) const { return _slot_table.slot_of_worker(worker_id); }
    // -1 while the query waits for a slot, or once it has finished.
    int slot_of_query(const TUniqueId& query_id) const { return _slot_table.slot_of(query_id); }
    int workers_of_slot(int slot) const { return _slot_table.workers_of_slot(slot); }

    // First take() of this pipeline records start; 0 if never started.
    int64_t wallclock_start_ns(const PipelineKey& key) const;
    void mark_wallclock_start(const PipelineKey& key, int64_t ns);

private:
    struct PipelineState {
        bool finished = false;
        bool is_exchange_source = false;
        bool is_exchange_sink = false;
        int exchange_node_id = -1;
        int dest_node_id = -1;
        int indegree = 0;
        std::vector<PipelineKey> successors;
        int64_t wallclock_start_ns = 0;
    };

    struct QueryState {
        bool query_finished = false;
        std::map<std::pair<int, PipelineId>, PipelineState> pipelines;
        // Survives losing the slot to a shrink, so the query resumes where it was.
        std::optional<PipelineKey> current;
    };

    void _rebind();
    void _advance_query(const TUniqueId& query_id, QueryState& qs);
    bool _is_eligible(const TUniqueId& query_id, const std::pair<int, PipelineId>& pip_key,
                      const QueryState& qs) const;
    // True if a local unfinished exchange sink sends to this recvr (dest_node_id match).
    bool _has_unfinished_sink_to(const QueryState& qs, int exchange_node_id,
                                 const std::pair<int, PipelineId>& self) const;
    std::optional<PipelineKey> _pick_ready_in_query(const TUniqueId& query_id) const;
    QueryState* _find_query(const TUniqueId& query_id);
    const QueryState* _find_query(const TUniqueId& query_id) const;

    std::map<TUniqueId, QueryState, TUniqueIdLess> _queries;
    ClosedSlotTable<TUniqueId> _slot_table;
    int _requested_slots = 1;
    bool _closed = false;
};

class SerialTaskQueue {
public:
    explicit SerialTaskQueue(int worker_count = 1);

    Status register_fragment(const SerialFragmentInfo& info);
    Status push_back(PipelineTaskSPtr task);
    Status push_back(PipelineTaskSPtr task, int /*core_id*/);
    PipelineTaskSPtr take(int /*core_id*/);
    void on_pipeline_finished(const PipelineKey& key);
    void on_query_finished(const TUniqueId& query_id);
    int64_t wallclock_start_ns(const PipelineKey& key);
    // Slot serving `query_id` (-1 if none) and how many workers that slot has.
    std::pair<int, int> slot_of_query(const TUniqueId& query_id);
    void close();
    void update_statistics(PipelineTask* task, int64_t time_spent);

    // Exposed for tests that drive the policy without worker threads.
    SerialDispatchState& dispatch_state() { return _dispatch; }

private:
    PipelineKey _key_of(const PipelineTaskSPtr& task) const;
    std::condition_variable& _cv_of_worker(int worker_id);
    void _notify_all_slots();

    std::mutex _lock;
    // One per possible slot (sized to the worker count), so a push wakes only workers
    // that may run the task. All of them wait on `_lock`.
    std::vector<std::condition_variable> _slot_cvs;
    SerialDispatchState _dispatch;
    std::map<PipelineKey, std::deque<PipelineTaskSPtr>> _runnable;
    static constexpr auto WAIT_TIMEOUT_MS = 100;
};

#include "common/compile_check_end.h"
} // namespace doris
