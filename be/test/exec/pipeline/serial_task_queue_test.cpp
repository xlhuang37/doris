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

#include <gtest/gtest.h>

#include <map>
#include <vector>

namespace doris {

namespace {

TUniqueId make_qid(int64_t lo) {
    TUniqueId id;
    id.hi = 0;
    id.lo = lo;
    return id;
}

SerialFragmentInfo make_fragment(int64_t query_lo, int fragment_id,
                                 const std::vector<SerialPipelineInfo>& pipelines,
                                 const std::map<PipelineId, std::vector<PipelineId>>& dag) {
    SerialFragmentInfo info;
    info.query_id = make_qid(query_lo);
    info.fragment_id = fragment_id;
    info.pipelines = pipelines;
    info.dag = dag;
    return info;
}

// Pipeline 0 feeds pipeline 1 inside fragment 0.
SerialFragmentInfo make_two_stage_query(int64_t query_lo) {
    std::map<PipelineId, std::vector<PipelineId>> dag;
    dag[1] = {0};
    return make_fragment(query_lo, 0, {{0, false, false}, {1, false, false}}, dag);
}

} // namespace

TEST(SerialDispatchStateTest, FcfsQueryOrder) {
    SerialDispatchState state;
    state.register_fragment(make_fragment(1, 0, {{0, false, false}}, {}));
    state.register_fragment(make_fragment(2, 0, {{0, false, false}}, {}));
    auto cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->query_id.lo, 1);
    EXPECT_EQ(cur->pipeline_id, 0);

    state.on_pipeline_finished({make_qid(1), 0, 0});
    // Query 1 is still registered; do not start query 2 until query 1 is finished.
    cur = state.current();
    EXPECT_FALSE(cur.has_value());

    state.on_query_finished(make_qid(1));
    cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->query_id.lo, 2);
}

TEST(SerialDispatchStateTest, LaterSubmitDoesNotPreemptCurrentQuery) {
    SerialDispatchState state;
    // Query 2 registers first and takes the only slot.
    state.register_fragment(make_fragment(2, 0, {{0, false, false}}, {}));
    auto cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->query_id.lo, 2);

    state.register_fragment(make_fragment(1, 0, {{0, false, false}}, {}));
    cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->query_id.lo, 2);

    state.on_pipeline_finished({make_qid(2), 0, 0});
    state.on_query_finished(make_qid(2));
    cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->query_id.lo, 1);
}

TEST(SerialDispatchStateTest, DoNotDispatchSecondQueryWhileFirstRegistered) {
    SerialDispatchState state;
    state.register_fragment(make_fragment(1, 0, {{0, false, false}}, {}));
    state.register_fragment(make_fragment(2, 0, {{0, false, false}}, {}));
    auto cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->query_id.lo, 1);

    state.advance();
    cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->query_id.lo, 1);
}

TEST(SerialDispatchStateTest, KahnTopoOrder) {
    SerialDispatchState state;
    state.register_fragment(make_two_stage_query(1));
    auto cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->pipeline_id, 0);

    state.advance();
    cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->pipeline_id, 0);

    state.on_pipeline_finished({make_qid(1), 0, 0});
    cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->pipeline_id, 1);
}

TEST(SerialDispatchStateTest, DelayExchangeSourceUntilSinkFinishes) {
    SerialDispatchState state;
    // Consumer fragment arrives first; it is the only pipeline so it becomes current.
    // exchange_node_id 10 is the recvr that fragment 0's sink targets.
    state.register_fragment(make_fragment(1, 1, {{0, true, false, 10, -1}}, {}));
    auto cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->fragment_id, 1);

    const PipelineKey source_key {make_qid(1), 1, 0};
    state.mark_wallclock_start(source_key, 1000);
    EXPECT_EQ(state.wallclock_start_ns(source_key), 1000);

    // Local producer arrives: yield the source so the sink can finish first.
    state.register_fragment(make_fragment(1, 0, {{0, false, true, -1, 10}}, {}));
    cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->fragment_id, 0);
    EXPECT_EQ(cur->pipeline_id, 0);
    EXPECT_EQ(state.wallclock_start_ns(source_key), 0);

    state.on_pipeline_finished({make_qid(1), 0, 0});
    cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->fragment_id, 1);
    EXPECT_EQ(cur->pipeline_id, 0);

    state.mark_wallclock_start(source_key, 2000);
    EXPECT_EQ(state.wallclock_start_ns(source_key), 2000);
}

TEST(SerialDispatchStateTest, IndependentPipelinesUseFragmentThenId) {
    SerialDispatchState state;
    state.register_fragment(make_fragment(1, 0, {{2, false, false}, {1, false, false}}, {}));
    auto cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->pipeline_id, 1);
}

// Intermediate fragment: exchange source feeds an exchange sink in the same fragment.
// The sink must not delay the source (Kahn: source first), or nothing is eligible.
TEST(SerialDispatchStateTest, SameFragmentExchangeSourceNotBlockedByOwnSink) {
    SerialDispatchState state;
    std::map<PipelineId, std::vector<PipelineId>> dag;
    dag[1] = {0};
    // Source recvr node 10; this fragment's sink sends to node 20 (downstream), not 10.
    state.register_fragment(
            make_fragment(1, 0, {{0, true, false, 10, -1}, {1, false, true, -1, 20}}, dag));
    auto cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->fragment_id, 0);
    EXPECT_EQ(cur->pipeline_id, 0);

    state.on_pipeline_finished({make_qid(1), 0, 0});
    cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->pipeline_id, 1);
}

// A downstream fragment's exchange source waits only for sinks that target its recvr.
TEST(SerialDispatchStateTest, OtherFragmentSinkDelaysExchangeSource) {
    SerialDispatchState state;
    std::map<PipelineId, std::vector<PipelineId>> dag;
    dag[1] = {0};
    state.register_fragment(
            make_fragment(1, 1, {{0, true, false, 10, -1}, {1, false, true, -1, 20}}, dag));
    // Producer fragment with a local exchange sink targeting node 10.
    state.register_fragment(make_fragment(1, 0, {{0, false, true, -1, 10}}, {}));
    auto cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->fragment_id, 0);
    EXPECT_EQ(cur->pipeline_id, 0);

    state.on_pipeline_finished({make_qid(1), 0, 0});
    cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->fragment_id, 1);
    EXPECT_EQ(cur->pipeline_id, 0);
}

// F0 scan+sink -> F1 source+sink -> F2 source+sink. After F0, F1 must not wait on F2.
TEST(SerialDispatchStateTest, DownstreamSinkDoesNotBlockUpstreamSource) {
    SerialDispatchState state;
    std::map<PipelineId, std::vector<PipelineId>> f1_dag;
    f1_dag[1] = {0};
    std::map<PipelineId, std::vector<PipelineId>> f2_dag;
    f2_dag[1] = {0};
    state.register_fragment(make_fragment(1, 0, {{0, false, true, -1, 10}}, {}));
    state.register_fragment(
            make_fragment(1, 1, {{0, true, false, 10, -1}, {1, false, true, -1, 20}}, f1_dag));
    state.register_fragment(
            make_fragment(1, 2, {{0, true, false, 20, -1}, {1, false, true, -1, 30}}, f2_dag));

    auto cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->fragment_id, 0);
    EXPECT_EQ(cur->pipeline_id, 0);

    state.on_pipeline_finished({make_qid(1), 0, 0});
    cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->fragment_id, 1);
    EXPECT_EQ(cur->pipeline_id, 0);

    state.on_pipeline_finished({make_qid(1), 1, 0});
    cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->fragment_id, 1);
    EXPECT_EQ(cur->pipeline_id, 1);

    state.on_pipeline_finished({make_qid(1), 1, 1});
    cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->fragment_id, 2);
    EXPECT_EQ(cur->pipeline_id, 0);
}

TEST(SerialDispatchStateTest, SlotsPartitionWorkersAcrossQueries) {
    SerialDispatchState state(8);
    state.set_requested_slots(2);
    state.register_fragment(make_two_stage_query(1));
    state.register_fragment(make_two_stage_query(2));
    ASSERT_EQ(state.slot_count(), 2);
    EXPECT_EQ(state.slot_of_query(make_qid(1)), 0);
    EXPECT_EQ(state.slot_of_query(make_qid(2)), 1);
    EXPECT_EQ(state.workers_of_slot(0), 4);
    EXPECT_EQ(state.workers_of_slot(1), 4);

    for (int worker = 0; worker < 8; ++worker) {
        auto cur = state.current_for_worker(worker);
        ASSERT_TRUE(cur.has_value());
        EXPECT_EQ(cur->query_id.lo, worker < 4 ? 1 : 2);
        EXPECT_EQ(cur->pipeline_id, 0);
    }
    EXPECT_FALSE(state.current_for_worker(-1).has_value());
    EXPECT_FALSE(state.current_for_worker(8).has_value());
}

// Finishing a pipeline of one query moves only that query; the other slot keeps its
// current pipeline, and a slot whose query has nothing ready idles instead of helping.
TEST(SerialDispatchStateTest, QueriesInDifferentSlotsAdvanceIndependently) {
    SerialDispatchState state(8);
    state.set_requested_slots(2);
    state.register_fragment(make_two_stage_query(1));
    state.register_fragment(make_two_stage_query(2));

    state.on_pipeline_finished({make_qid(1), 0, 0});
    auto q1 = state.current_for_worker(0);
    auto q2 = state.current_for_worker(4);
    ASSERT_TRUE(q1.has_value());
    ASSERT_TRUE(q2.has_value());
    EXPECT_EQ(q1->query_id.lo, 1);
    EXPECT_EQ(q1->pipeline_id, 1);
    EXPECT_EQ(q2->query_id.lo, 2);
    EXPECT_EQ(q2->pipeline_id, 0);

    state.on_pipeline_finished({make_qid(1), 0, 1});
    EXPECT_FALSE(state.current_for_worker(0).has_value());
    q2 = state.current_for_worker(4);
    ASSERT_TRUE(q2.has_value());
    EXPECT_EQ(q2->pipeline_id, 0);

    state.on_pipeline_finished({make_qid(2), 0, 0});
    q2 = state.current_for_worker(7);
    ASSERT_TRUE(q2.has_value());
    EXPECT_EQ(q2->pipeline_id, 1);
    EXPECT_FALSE(state.current_for_worker(3).has_value());
}

TEST(SerialDispatchStateTest, QueryBeyondSlotCountWaitsForAFreeSlot) {
    SerialDispatchState state(8);
    state.set_requested_slots(2);
    state.register_fragment(make_two_stage_query(1));
    state.register_fragment(make_two_stage_query(2));
    state.register_fragment(make_two_stage_query(3));
    EXPECT_EQ(state.slot_of_query(make_qid(3)), -1);
    EXPECT_FALSE(state.current_of_query(make_qid(3)).has_value());

    // Query 2 finishing frees slot 1 (workers 4-7); query 1 in slot 0 is untouched.
    state.on_query_finished(make_qid(2));
    EXPECT_EQ(state.slot_of_query(make_qid(3)), 1);
    EXPECT_EQ(state.slot_of_query(make_qid(1)), 0);
    auto cur = state.current_for_worker(5);
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->query_id.lo, 3);
    EXPECT_EQ(cur->pipeline_id, 0);
    cur = state.current_for_worker(0);
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->query_id.lo, 1);
}

// Shrinking the slot count evicts the query in the dropped slot; it waits ahead of later
// arrivals and resumes on the pipeline it had when it gets a slot again.
TEST(SerialDispatchStateTest, ShrinkEvictsAndResumesCurrentPipeline) {
    SerialDispatchState state(8);
    state.set_requested_slots(2);
    state.register_fragment(make_two_stage_query(1));
    state.register_fragment(make_two_stage_query(2));
    state.on_pipeline_finished({make_qid(2), 0, 0});

    state.set_requested_slots(1);
    state.register_fragment(make_two_stage_query(3));
    ASSERT_EQ(state.slot_count(), 1);
    EXPECT_EQ(state.slot_of_query(make_qid(1)), 0);
    EXPECT_EQ(state.slot_of_query(make_qid(2)), -1);
    EXPECT_EQ(state.workers_of_slot(0), 8);
    auto cur = state.current_for_worker(7);
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->query_id.lo, 1);

    state.on_query_finished(make_qid(1));
    EXPECT_EQ(state.slot_of_query(make_qid(2)), 0);
    EXPECT_EQ(state.slot_of_query(make_qid(3)), -1);
    cur = state.current_for_worker(0);
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->query_id.lo, 2);
    EXPECT_EQ(cur->pipeline_id, 1);
}

TEST(SerialDispatchStateTest, CloseClearsEverySlot) {
    SerialDispatchState state(4);
    state.set_requested_slots(2);
    state.register_fragment(make_two_stage_query(1));
    state.register_fragment(make_two_stage_query(2));
    state.close();
    for (int worker = 0; worker < 4; ++worker) {
        EXPECT_FALSE(state.current_for_worker(worker).has_value());
    }
}

} // namespace doris
