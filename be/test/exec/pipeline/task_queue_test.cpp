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

#include "exec/pipeline/task_queue.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>

#include "common/config.h"
#include "exec/pipeline/pipeline_task.h"

namespace doris {

// Tests for the global, query-granular absolute-priority pipeline MLFQ
// (MultiCoreTaskQueue). Level thresholds are {1s,3s,10s,60s,300s} of query runtime,
// so: <=1s -> L0, (1s,3s] -> L1, ... A query's level is read from its tasks'
// query_runtime_ns(); the bucket key is query_ctx_raw() (compared, never dereferenced,
// so opaque fake pointers are fine here).
static constexpr uint64_t kSecondNs = 1'000'000'000ULL;

class MockPipelineTask : public PipelineTask {
public:
    MockPipelineTask(QueryContext* key, uint64_t runtime_ns) : _key(key), _runtime_ns(runtime_ns) {}

    uint64_t query_runtime_ns() const override { return _runtime_ns; }
    QueryContext* query_ctx_raw() const override { return _key; }

    void set_runtime_ns(uint64_t runtime_ns) { _runtime_ns = runtime_ns; }

private:
    QueryContext* _key;
    uint64_t _runtime_ns;
};

namespace {
QueryContext* qkey(uintptr_t id) {
    return reinterpret_cast<QueryContext*>(id);
}
PipelineTaskSPtr make_task(QueryContext* key, uint64_t runtime_ns) {
    return std::make_shared<MockPipelineTask>(key, runtime_ns);
}
} // namespace

// A lower-runtime query's tasks are served before a higher-runtime query's, globally,
// regardless of push order or which worker asks (the H1 per-core-sharding regression).
TEST(GlobalPipelineMLFQTest, AbsolutePriorityAcrossQueries) {
    MultiCoreTaskQueue q(1);
    auto* qa = qkey(0xA); // runtime 0 -> L0
    auto* qb = qkey(0xB); // runtime 5s -> L1

    // Push the lower-priority query first.
    ASSERT_TRUE(q.push_back(make_task(qb, 5 * kSecondNs)).ok());
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());

    auto t1 = q.take(0);
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qa);

    auto t2 = q.take(0);
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qb);

    q.close();
}

// Within one level, distinct workers are spread across co-resident queries
// round-robin rather than dogpiling one query.
TEST(GlobalPipelineMLFQTest, WithinLevelRoundRobinSpread) {
    MultiCoreTaskQueue q(3);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);
    auto* qc = qkey(0xC);

    // All at L0, two tasks each; insertion order A, B, C.
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qb, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qb, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qc, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qc, 0)).ok());

    // Three different workers each take once: should land on three different queries.
    auto t0 = q.take(0);
    auto t1 = q.take(1);
    auto t2 = q.take(2);
    ASSERT_NE(t0, nullptr);
    ASSERT_NE(t1, nullptr);
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t0->query_ctx_raw(), qa);
    EXPECT_EQ(t1->query_ctx_raw(), qb);
    EXPECT_EQ(t2->query_ctx_raw(), qc);

    q.close();
}

// A worker keeps serving the same query across takes while that query remains the
// highest priority (locality).
TEST(GlobalPipelineMLFQTest, QueryLocality) {
    MultiCoreTaskQueue q(2);
    auto* qa = qkey(0xA); // L0
    auto* qb = qkey(0xB); // L1

    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    }
    ASSERT_TRUE(q.push_back(make_task(qb, 5 * kSecondNs)).ok());

    // Worker 0 should stick to query A for all three of its tasks before touching B.
    for (int i = 0; i < 3; ++i) {
        auto t = q.take(0);
        ASSERT_NE(t, nullptr);
        EXPECT_EQ(t->query_ctx_raw(), qa) << "iteration " << i;
    }
    auto t = q.take(0);
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->query_ctx_raw(), qb);

    q.close();
}

// A newly arrived higher-priority query pulls a worker off its current (lower
// priority) query at the next take (cooperative preemption).
TEST(GlobalPipelineMLFQTest, PreemptionByHigherPriorityQuery) {
    MultiCoreTaskQueue q(1);
    auto* qa = qkey(0xA); // L1 (5s)
    auto* qc = qkey(0xC); // L0 (0)

    ASSERT_TRUE(q.push_back(make_task(qa, 5 * kSecondNs)).ok());
    ASSERT_TRUE(q.push_back(make_task(qa, 5 * kSecondNs)).ok());

    auto t1 = q.take(0); // worker latches onto A (only query present)
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qa);

    // Higher-priority query C arrives.
    ASSERT_TRUE(q.push_back(make_task(qc, 0)).ok());

    auto t2 = q.take(0); // should preempt to C despite A still having a runnable task
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qc);

    q.close();
}

// With the soft per-query worker cap set, workers beyond the cap spill to a
// lower-priority query even though the top query still has runnable tasks.
TEST(GlobalPipelineMLFQTest, LeaseSpillWhenCapped) {
    int saved = config::pipeline_query_worker_cap;
    config::pipeline_query_worker_cap = 1;

    MultiCoreTaskQueue q(2);
    auto* qa = qkey(0xA); // L0, three tasks
    auto* qb = qkey(0xB); // L1, one task

    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    }
    ASSERT_TRUE(q.push_back(make_task(qb, 5 * kSecondNs)).ok());

    auto t0 = q.take(0); // A takes its single allowed worker slot
    ASSERT_NE(t0, nullptr);
    EXPECT_EQ(t0->query_ctx_raw(), qa);

    // A is at its cap (1 in-flight, not released), so worker 1 spills to B.
    auto t1 = q.take(1);
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qb);

    q.close();
    config::pipeline_query_worker_cap = saved;
}

} // namespace doris
