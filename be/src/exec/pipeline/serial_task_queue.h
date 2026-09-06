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
    int64_t arrival_ns = 0;
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
class SerialDispatchState {
public:
    void register_fragment(const SerialFragmentInfo& info);
    void on_pipeline_finished(const PipelineKey& key);
    void on_query_finished(const TUniqueId& query_id);
    // If there is no current pipeline, or the current one is finished, pick the next.
    void advance();
    std::optional<PipelineKey> current() const { return _current; }
    void close();
    bool closed() const { return _closed; }

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
        int64_t arrival_ns = 0;
        bool query_finished = false;
        std::map<std::pair<int, PipelineId>, PipelineState> pipelines;
    };

    bool _is_eligible(const TUniqueId& query_id, const std::pair<int, PipelineId>& pip_key,
                      const QueryState& qs) const;
    // True if a local unfinished exchange sink sends to this recvr (dest_node_id match).
    bool _has_unfinished_sink_to(const QueryState& qs, int exchange_node_id,
                                 const std::pair<int, PipelineId>& self) const;
    std::optional<PipelineKey> _pick_ready_in_query(const TUniqueId& query_id) const;
    QueryState* _find_query(const TUniqueId& query_id);
    const QueryState* _find_query(const TUniqueId& query_id) const;

    std::vector<TUniqueId> _fcfs;
    std::map<TUniqueId, QueryState, TUniqueIdLess> _queries;
    std::optional<TUniqueId> _active_query;
    std::optional<PipelineKey> _current;
    bool _closed = false;
};

class SerialTaskQueue {
public:
    Status register_fragment(const SerialFragmentInfo& info);
    Status push_back(PipelineTaskSPtr task);
    Status push_back(PipelineTaskSPtr task, int /*core_id*/);
    PipelineTaskSPtr take(int /*core_id*/);
    void on_pipeline_finished(const PipelineKey& key);
    void on_query_finished(const TUniqueId& query_id);
    int64_t wallclock_start_ns(const PipelineKey& key);
    void close();
    void update_statistics(PipelineTask* task, int64_t time_spent);

    // Exposed for tests that drive the policy without worker threads.
    SerialDispatchState& dispatch_state() { return _dispatch; }

private:
    PipelineKey _key_of(const PipelineTaskSPtr& task) const;

    std::mutex _lock;
    std::condition_variable _cv;
    SerialDispatchState _dispatch;
    std::map<PipelineKey, std::deque<PipelineTaskSPtr>> _runnable;
    static constexpr auto WAIT_TIMEOUT_MS = 100;
};

#include "common/compile_check_end.h"
} // namespace doris
