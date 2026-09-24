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
#include <string>

#include "common/config.h"
#include "common/logging.h"
#include "exec/pipeline/pipeline_fragment_context.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {
#include "common/compile_check_begin.h"

void SerialDispatchState::close() {
    _closed = true;
    for (auto& [qid, qs] : _queries) {
        (void)qid;
        qs.current.reset();
    }
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

void SerialDispatchState::_rebind() {
    std::vector<int> changed;
    _slot_table.rebind(_requested_slots, &changed);
    std::sort(changed.begin(), changed.end());
    changed.erase(std::unique(changed.begin(), changed.end()), changed.end());
    for (int slot : changed) {
        const auto& owner = _slot_table.owner_of(slot);
        LOG(INFO) << "serial closed-system slot " << slot << " ("
                  << _slot_table.workers_of_slot(slot) << " workers) now serves "
                  << (owner.has_value() ? print_id(*owner) : std::string("nothing"));
    }
}

void SerialDispatchState::register_fragment(const SerialFragmentInfo& info) {
    if (_closed) {
        return;
    }
    auto [qit, inserted] = _queries.try_emplace(info.query_id);
    QueryState& qs = qit->second;
    if (inserted) {
        _slot_table.add(info.query_id);
    }

    bool added_sink = false;
    for (const auto& pinfo : info.pipelines) {
        auto pip_key = std::make_pair(info.fragment_id, pinfo.pipeline_id);
        auto [pit, pip_inserted] = qs.pipelines.try_emplace(pip_key);
        PipelineState& ps = pit->second;
        ps.is_exchange_source = pinfo.is_exchange_source;
        ps.is_exchange_sink = pinfo.is_exchange_sink;
        ps.exchange_node_id = pinfo.exchange_node_id;
        ps.dest_node_id = pinfo.dest_node_id;
        if (pip_inserted && pinfo.is_exchange_sink) {
            added_sink = true;
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
    if (added_sink && qs.current.has_value()) {
        auto cur_it =
                qs.pipelines.find(std::make_pair(qs.current->fragment_id, qs.current->pipeline_id));
        if (cur_it != qs.pipelines.end() && cur_it->second.is_exchange_source &&
            cur_it->second.exchange_node_id >= 0) {
            const int recvr = cur_it->second.exchange_node_id;
            for (const auto& pinfo : info.pipelines) {
                if (pinfo.is_exchange_sink && pinfo.dest_node_id == recvr) {
                    // First take() may have already stamped start while this source was
                    // current with no producer registered. Drop it so the next take()
                    // after the sink finishes records the real start.
                    cur_it->second.wallclock_start_ns = 0;
                    qs.current.reset();
                    break;
                }
            }
        }
    }
    _rebind();
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
    if (qs->current.has_value() && *qs->current == key) {
        qs->current.reset();
    }
    advance();
}

void SerialDispatchState::on_query_finished(const TUniqueId& query_id) {
    auto* qs = _find_query(query_id);
    if (qs == nullptr) {
        return;
    }
    qs->query_finished = true;
    qs->current.reset();
    _slot_table.remove(query_id);
    // Hand the freed slot to the query that has been waiting longest.
    _rebind();
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

std::optional<PipelineKey> SerialDispatchState::_pick_ready_in_query(
        const TUniqueId& query_id) const {
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

void SerialDispatchState::_advance_query(const TUniqueId& query_id, QueryState& qs) {
    if (qs.current.has_value()) {
        auto it =
                qs.pipelines.find(std::make_pair(qs.current->fragment_id, qs.current->pipeline_id));
        if (it != qs.pipelines.end() && !it->second.finished) {
            return;
        }
        qs.current.reset();
    }
    qs.current = _pick_ready_in_query(query_id);
}

void SerialDispatchState::advance() {
    if (_closed) {
        return;
    }
    for (int slot = 0; slot < _slot_table.slot_count(); ++slot) {
        const auto& owner = _slot_table.owner_of(slot);
        if (!owner.has_value()) {
            continue;
        }
        auto* qs = _find_query(*owner);
        if (qs == nullptr || qs->query_finished) {
            continue;
        }
        _advance_query(*owner, *qs);
    }
}

std::optional<PipelineKey> SerialDispatchState::current_of_query(const TUniqueId& query_id) const {
    const auto* qs = _find_query(query_id);
    if (qs == nullptr || qs->query_finished) {
        return std::nullopt;
    }
    return qs->current;
}

std::optional<PipelineKey> SerialDispatchState::current_for_worker(int worker_id) const {
    if (_closed) {
        return std::nullopt;
    }
    const auto& owner = _slot_table.owner_of(_slot_table.slot_of_worker(worker_id));
    if (!owner.has_value()) {
        return std::nullopt;
    }
    return current_of_query(*owner);
}

std::optional<PipelineKey> SerialDispatchState::current() const {
    return current_for_worker(0);
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

SerialTaskQueue::SerialTaskQueue(int worker_count)
        : _slot_cvs(static_cast<size_t>(std::max(worker_count, 1))),
          _dispatch(std::max(worker_count, 1)) {
    std::lock_guard<std::mutex> l(_lock);
    _dispatch.set_requested_slots(config::pipeline_closed_system_slots);
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

std::condition_variable& SerialTaskQueue::_cv_of_worker(int worker_id) {
    const int slot = _dispatch.slot_of_worker(worker_id);
    return _slot_cvs[static_cast<size_t>(std::max(slot, 0))];
}

void SerialTaskQueue::_notify_all_slots() {
    for (auto& cv : _slot_cvs) {
        cv.notify_all();
    }
}

Status SerialTaskQueue::register_fragment(const SerialFragmentInfo& info) {
    std::lock_guard<std::mutex> l(_lock);
    if (_dispatch.closed()) {
        return Status::InternalError("SerialTaskQueue closed");
    }
    _dispatch.set_requested_slots(config::pipeline_closed_system_slots);
    _dispatch.register_fragment(info);
    _notify_all_slots();
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
    const int slot = _dispatch.slot_of_query(key.query_id);
    if (slot >= 0) {
        _slot_cvs[static_cast<size_t>(slot)].notify_one();
    }
    return Status::OK();
}

PipelineTaskSPtr SerialTaskQueue::take(int core_id) {
    std::unique_lock<std::mutex> l(_lock);
    while (!_dispatch.closed()) {
        _dispatch.advance();
        auto current = _dispatch.current_for_worker(core_id);
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
        _cv_of_worker(core_id).wait_for(l, std::chrono::milliseconds(WAIT_TIMEOUT_MS));
    }
    return nullptr;
}

void SerialTaskQueue::on_pipeline_finished(const PipelineKey& key) {
    std::lock_guard<std::mutex> l(_lock);
    _dispatch.on_pipeline_finished(key);
    _runnable.erase(key);
    _notify_all_slots();
}

void SerialTaskQueue::on_query_finished(const TUniqueId& query_id) {
    std::lock_guard<std::mutex> l(_lock);
    _dispatch.set_requested_slots(config::pipeline_closed_system_slots);
    _dispatch.on_query_finished(query_id);
    for (auto it = _runnable.begin(); it != _runnable.end();) {
        if (it->first.query_id == query_id) {
            it = _runnable.erase(it);
        } else {
            ++it;
        }
    }
    _notify_all_slots();
}

int64_t SerialTaskQueue::wallclock_start_ns(const PipelineKey& key) {
    std::lock_guard<std::mutex> l(_lock);
    return _dispatch.wallclock_start_ns(key);
}

std::pair<int, int> SerialTaskQueue::slot_of_query(const TUniqueId& query_id) {
    std::lock_guard<std::mutex> l(_lock);
    const int slot = _dispatch.slot_of_query(query_id);
    return {slot, _dispatch.workers_of_slot(slot)};
}

void SerialTaskQueue::close() {
    std::lock_guard<std::mutex> l(_lock);
    _dispatch.close();
    _runnable.clear();
    _notify_all_slots();
}

void SerialTaskQueue::update_statistics(PipelineTask* task, int64_t time_spent) {
    if (task != nullptr) {
        task->inc_runtime_ns(time_spent);
    }
}

#include "common/compile_check_end.h"
} // namespace doris
