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

SerialFragmentInfo make_fragment(int64_t query_lo, int64_t arrival_ns, int fragment_id,
                                 const std::vector<SerialPipelineInfo>& pipelines,
                                 const std::map<PipelineId, std::vector<PipelineId>>& dag) {
    SerialFragmentInfo info;
    info.query_id = make_qid(query_lo);
    info.arrival_ns = arrival_ns;
    info.fragment_id = fragment_id;
    info.pipelines = pipelines;
    info.dag = dag;
    return info;
}

} // namespace

TEST(SerialDispatchStateTest, FcfsQueryOrder) {
    SerialDispatchState state;
    state.register_fragment(make_fragment(1, 100, 0, {{0, false, false}}, {}));
    state.register_fragment(make_fragment(2, 200, 0, {{0, false, false}}, {}));
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
    // Query 2 is submitted first even though query 1 arrived earlier on the BE.
    state.register_fragment(make_fragment(2, 200, 0, {{0, false, false}}, {}));
    auto cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->query_id.lo, 2);

    state.register_fragment(make_fragment(1, 100, 0, {{0, false, false}}, {}));
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
    state.register_fragment(make_fragment(1, 100, 0, {{0, false, false}}, {}));
    state.register_fragment(make_fragment(2, 200, 0, {{0, false, false}}, {}));
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
    std::map<PipelineId, std::vector<PipelineId>> dag;
    dag[1] = {0};
    state.register_fragment(
            make_fragment(1, 100, 0, {{0, false, false}, {1, false, false}}, dag));
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
    state.register_fragment(make_fragment(1, 100, 1, {{0, true, false}}, {}));
    auto cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->fragment_id, 1);

    // Local producer arrives: yield the source so the sink can finish first.
    state.register_fragment(make_fragment(1, 100, 0, {{0, false, true}}, {}));
    cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->fragment_id, 0);
    EXPECT_EQ(cur->pipeline_id, 0);

    state.on_pipeline_finished({make_qid(1), 0, 0});
    cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->fragment_id, 1);
    EXPECT_EQ(cur->pipeline_id, 0);
}

TEST(SerialDispatchStateTest, IndependentPipelinesUseFragmentThenId) {
    SerialDispatchState state;
    state.register_fragment(
            make_fragment(1, 100, 0, {{2, false, false}, {1, false, false}}, {}));
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
    state.register_fragment(make_fragment(
            1, 100, 0, {{0, true, false}, {1, false, true}}, dag));
    auto cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->fragment_id, 0);
    EXPECT_EQ(cur->pipeline_id, 0);

    state.on_pipeline_finished({make_qid(1), 0, 0});
    cur = state.current();
    ASSERT_TRUE(cur.has_value());
    EXPECT_EQ(cur->pipeline_id, 1);
}

// A downstream fragment's exchange source waits for another fragment's sink, not its own.
TEST(SerialDispatchStateTest, OtherFragmentSinkDelaysExchangeSource) {
    SerialDispatchState state;
    std::map<PipelineId, std::vector<PipelineId>> dag;
    dag[1] = {0};
    state.register_fragment(make_fragment(
            1, 100, 1, {{0, true, false}, {1, false, true}}, dag));
    // Producer fragment with a local exchange sink.
    state.register_fragment(make_fragment(1, 100, 0, {{0, false, true}}, {}));
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

} // namespace doris
