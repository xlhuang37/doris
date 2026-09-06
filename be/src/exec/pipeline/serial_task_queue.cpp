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

#include "exec/pipeline/serial_task_queue.h"

#include <algorithm>
#include <chrono>

#include "common/logging.h"
#include "exec/pipeline/pipeline_fragment_context.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {
#include "common/compile_check_begin.h"

void SerialDispatchState::close() {
    _closed = true;
    _current.reset();
    _active_query.reset();
}

SerialDispatchState::QueryState* SerialDispatchState::_find_query(const TUniqueId& query_id) {
    auto it = _queries.find(query_id);
    return it == _queries.end() ? nullptr : &it->second;
}

const SerialDispatchState::QueryState* SerialDispatchState::_find_query(
        const TUniqueId& query_id) const {
    auto it = _queries.find(query_id);
    return it == _queries.end() ? nullptr : &it->second;
}

void SerialDispatchState::register_fragment(const SerialFragmentInfo& info) {
    if (_closed) {
        return;
    }
    auto [qit, inserted] = _queries.try_emplace(info.query_id);
    QueryState& qs = qit->second;
    if (inserted) {
        qs.arrival_ns = info.arrival_ns;
        _fcfs.push_back(info.query_id);
        std::stable_sort(_fcfs.begin(), _fcfs.end(), [&](const TUniqueId& a, const TUniqueId& b) {
            const auto& qa = _queries.at(a);
            const auto& qb = _queries.at(b);
            if (qa.arrival_ns != qb.arrival_ns) {
                return qa.arrival_ns < qb.arrival_ns;
            }
            return a.hi < b.hi || (a.hi == b.hi && a.lo < b.lo);
        });
    }

    bool added_matching_sink = false;
    for (const auto& pinfo : info.pipelines) {
        auto pip_key = std::make_pair(info.fragment_id, pinfo.pipeline_id);
        auto [pit, pip_inserted] = qs.pipelines.try_emplace(pip_key);
        PipelineState& ps = pit->second;
        ps.is_exchange_source = pinfo.is_exchange_source;
        ps.is_exchange_sink = pinfo.is_exchange_sink;
        ps.exchange_node_id = pinfo.exchange_node_id;
        ps.dest_node_id = pinfo.dest_node_id;
        if (pip_inserted && pinfo.is_exchange_sink && _current.has_value() &&
            _current->query_id == info.query_id) {
            added_matching_sink = true;
        }
    }

    for (const auto& [downstream, upstreams] : info.dag) {
        auto down_key = std::make_pair(info.fragment_id, downstream);
        auto dit = qs.pipelines.find(down_key);
        if (dit == qs.pipelines.end()) {
            continue;
        }
        dit->second.indegree = static_cast<int>(upstreams.size());
        for (auto upstream : upstreams) {
            auto up_key = std::make_pair(info.fragment_id, upstream);
            auto uit = qs.pipelines.find(up_key);
            if (uit == qs.pipelines.end()) {
                continue;
            }
            uit->second.successors.push_back(
                    PipelineKey {info.query_id, info.fragment_id, downstream});
        }
    }
    // Consumer may have become current before a local producer that sends to it arrived.
    // Yield only if a newly registered sink actually targets the current source's recvr.
    if (added_matching_sink && _current.has_value() && _current->query_id == info.query_id) {
        auto cur_it = qs.pipelines.find(std::make_pair(_current->fragment_id, _current->pipeline_id));
        if (cur_it != qs.pipelines.end() && cur_it->second.is_exchange_source &&
            cur_it->second.exchange_node_id >= 0) {
            const int recvr = cur_it->second.exchange_node_id;
            for (const auto& pinfo : info.pipelines) {
                if (pinfo.is_exchange_sink && pinfo.dest_node_id == recvr) {
                    _current.reset();
                    break;
                }
            }
        }
    }
    advance();
}

void SerialDispatchState::on_pipeline_finished(const PipelineKey& key) {
    auto* qs = _find_query(key.query_id);
    if (qs == nullptr) {
        return;
    }
    auto pip_key = std::make_pair(key.fragment_id, key.pipeline_id);
    auto it = qs->pipelines.find(pip_key);
    if (it == qs->pipelines.end()) {
        return;
    }
    PipelineState& ps = it->second;
    if (ps.finished) {
        return;
    }
    ps.finished = true;
    for (const auto& succ : ps.successors) {
        auto* succ_qs = _find_query(succ.query_id);
        if (succ_qs == nullptr) {
            continue;
        }
        auto sit = succ_qs->pipelines.find(std::make_pair(succ.fragment_id, succ.pipeline_id));
        if (sit != succ_qs->pipelines.end() && sit->second.indegree > 0) {
            sit->second.indegree--;
        }
    }
    if (_current.has_value() && *_current == key) {
        _current.reset();
    }
    advance();
}

void SerialDispatchState::on_query_finished(const TUniqueId& query_id) {
    auto* qs = _find_query(query_id);
    if (qs == nullptr) {
        return;
    }
    qs->query_finished = true;
    if (_current.has_value() && _current->query_id == query_id) {
        _current.reset();
    }
    if (_active_query.has_value() && *_active_query == query_id) {
        _active_query.reset();
    }
    _fcfs.erase(std::remove(_fcfs.begin(), _fcfs.end(), query_id), _fcfs.end());
    advance();
}

bool SerialDispatchState::_is_eligible(const TUniqueId& query_id,
                                       const std::pair<int, PipelineId>& pip_key,
                                       const QueryState& qs) const {
    auto it = qs.pipelines.find(pip_key);
    if (it == qs.pipelines.end()) {
        return false;
    }
    const PipelineState& ps = it->second;
    if (ps.finished || ps.indegree > 0) {
        return false;
    }
    if (ps.is_exchange_source && ps.exchange_node_id >= 0 &&
        _has_unfinished_sink_to(qs, ps.exchange_node_id, pip_key)) {
        return false;
    }
    (void)query_id;
    return true;
}

bool SerialDispatchState::_has_unfinished_sink_to(const QueryState& qs, int exchange_node_id,
                                                  const std::pair<int, PipelineId>& self) const {
    for (const auto& [other_key, other_ps] : qs.pipelines) {
        if (other_key == self || other_ps.finished || !other_ps.is_exchange_sink) {
            continue;
        }
        if (other_ps.dest_node_id == exchange_node_id) {
            return true;
        }
    }
    return false;
}

std::optional<PipelineKey> SerialDispatchState::_pick_ready_in_query(const TUniqueId& query_id) const {
    const auto* qs = _find_query(query_id);
    if (qs == nullptr || qs->query_finished) {
        return std::nullopt;
    }
    std::optional<PipelineKey> best;
    for (const auto& [pip_key, ps] : qs->pipelines) {
        (void)ps;
        if (!_is_eligible(query_id, pip_key, *qs)) {
            continue;
        }
        PipelineKey cand {query_id, pip_key.first, pip_key.second};
        if (!best.has_value() || cand.fragment_id < best->fragment_id ||
            (cand.fragment_id == best->fragment_id && cand.pipeline_id < best->pipeline_id)) {
            best = cand;
        }
    }
    return best;
}

void SerialDispatchState::advance() {
    if (_closed) {
        _current.reset();
        _active_query.reset();
        return;
    }
    if (_current.has_value()) {
        auto* qs = _find_query(_current->query_id);
        if (qs != nullptr && !qs->query_finished) {
            auto it = qs->pipelines.find(
                    std::make_pair(_current->fragment_id, _current->pipeline_id));
            if (it != qs->pipelines.end() && !it->second.finished) {
                return;
            }
        }
        _current.reset();
    }
    if (_active_query.has_value()) {
        auto* qs = _find_query(*_active_query);
        if (qs != nullptr && !qs->query_finished) {
            _current = _pick_ready_in_query(*_active_query);
            return;
        }
        _active_query.reset();
    }
    for (const auto& qid : _fcfs) {
        const auto* qs = _find_query(qid);
        if (qs == nullptr || qs->query_finished) {
            continue;
        }
        _active_query = qid;
        _current = _pick_ready_in_query(qid);
        return;
    }
}

int64_t SerialDispatchState::wallclock_start_ns(const PipelineKey& key) const {
    const auto* qs = _find_query(key.query_id);
    if (qs == nullptr) {
        return 0;
    }
    auto it = qs->pipelines.find(std::make_pair(key.fragment_id, key.pipeline_id));
    if (it == qs->pipelines.end()) {
        return 0;
    }
    return it->second.wallclock_start_ns;
}

void SerialDispatchState::mark_wallclock_start(const PipelineKey& key, int64_t ns) {
    auto* qs = _find_query(key.query_id);
    if (qs == nullptr) {
        return;
    }
    auto it = qs->pipelines.find(std::make_pair(key.fragment_id, key.pipeline_id));
    if (it == qs->pipelines.end()) {
        return;
    }
    if (it->second.wallclock_start_ns == 0) {
        it->second.wallclock_start_ns = ns;
    }
}

PipelineKey SerialTaskQueue::_key_of(const PipelineTaskSPtr& task) const {
    PipelineKey key;
    key.query_id = task->query_id();
    key.pipeline_id = task->pipeline_id();
    if (auto ctx = task->fragment_context().lock()) {
        key.fragment_id = ctx->get_fragment_id();
    }
    return key;
}

Status SerialTaskQueue::register_fragment(const SerialFragmentInfo& info) {
    std::lock_guard<std::mutex> l(_lock);
    if (_dispatch.closed()) {
        return Status::InternalError("SerialTaskQueue closed");
    }
    _dispatch.register_fragment(info);
    _cv.notify_all();
    return Status::OK();
}

Status SerialTaskQueue::push_back(PipelineTaskSPtr task) {
    return push_back(std::move(task), -1);
}

Status SerialTaskQueue::push_back(PipelineTaskSPtr task, int /*core_id*/) {
    std::lock_guard<std::mutex> l(_lock);
    if (_dispatch.closed()) {
        return Status::InternalError("SerialTaskQueue closed");
    }
    task->put_in_runnable_queue();
    auto key = _key_of(task);
    _runnable[key].push_back(std::move(task));
    _dispatch.advance();
    _cv.notify_all();
    return Status::OK();
}

PipelineTaskSPtr SerialTaskQueue::take(int /*core_id*/) {
    std::unique_lock<std::mutex> l(_lock);
    while (!_dispatch.closed()) {
        _dispatch.advance();
        auto current = _dispatch.current();
        if (current.has_value()) {
            auto& dq = _runnable[*current];
            if (!dq.empty()) {
                auto task = dq.front();
                dq.pop_front();
                _dispatch.mark_wallclock_start(*current, MonotonicNanos());
                task->pop_out_runnable_queue();
                return task;
            }
        }
        _cv.wait_for(l, std::chrono::milliseconds(WAIT_TIMEOUT_MS));
    }
    return nullptr;
}

void SerialTaskQueue::on_pipeline_finished(const PipelineKey& key) {
    std::lock_guard<std::mutex> l(_lock);
    _dispatch.on_pipeline_finished(key);
    _runnable.erase(key);
    _cv.notify_all();
}

void SerialTaskQueue::on_query_finished(const TUniqueId& query_id) {
    std::lock_guard<std::mutex> l(_lock);
    _dispatch.on_query_finished(query_id);
    for (auto it = _runnable.begin(); it != _runnable.end();) {
        if (it->first.query_id == query_id) {
            it = _runnable.erase(it);
        } else {
            ++it;
        }
    }
    _cv.notify_all();
}

int64_t SerialTaskQueue::wallclock_start_ns(const PipelineKey& key) {
    std::lock_guard<std::mutex> l(_lock);
    return _dispatch.wallclock_start_ns(key);
}

void SerialTaskQueue::close() {
    std::lock_guard<std::mutex> l(_lock);
    _dispatch.close();
    _runnable.clear();
    _cv.notify_all();
}

void SerialTaskQueue::update_statistics(PipelineTask* task, int64_t time_spent) {
    if (task != nullptr) {
        task->inc_runtime_ns(time_spent);
    }
}

#include "common/compile_check_end.h"
} // namespace doris
