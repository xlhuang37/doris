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

#include "exec/pipeline/pipeline_task.h"

namespace doris {

// Tests for the lock-free, query-granular pipeline MLFQ (MultiCoreTaskQueue). Each
// query owns one producer sub-queue that starts at the highest priority level (L0);
// a query is demoted to a deeper level by update_statistics once its accumulated
// query_runtime_ns() crosses a threshold. Level thresholds are {2.8s,10s,25s}, so:
// <=2.8s -> L0, (2.8s,10s] -> L1, (10s,25s] -> L2, >25s -> L3. The bucket key is
// query_ctx_raw() (compared, never dereferenced, so opaque fake pointers are fine).
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

// Basic FIFO for a single query: everything pushed comes back out, then the queue is
// empty (take returns nullptr after a short wait).
TEST(LockFreePipelineMLFQTest, SingleQueueDrains) {
    MultiCoreTaskQueue q(1);
    auto* qa = qkey(0xA);

    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());

    for (int i = 0; i < 2; ++i) {
        auto t = q.take(0);
        ASSERT_NE(t, nullptr);
        EXPECT_EQ(t->query_ctx_raw(), qa);
    }

    q.close();
}

// Two queries both start at L0, so both of their tasks are served (order between
// co-resident L0 queries is not guaranteed).
TEST(LockFreePipelineMLFQTest, CoResidentQueriesBothServed) {
    MultiCoreTaskQueue q(2);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);

    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qb, 0)).ok());

    int from_a = 0;
    int from_b = 0;
    for (int i = 0; i < 2; ++i) {
        auto t = q.take(0);
        ASSERT_NE(t, nullptr);
        if (t->query_ctx_raw() == qa) {
            ++from_a;
        } else if (t->query_ctx_raw() == qb) {
            ++from_b;
        }
    }
    EXPECT_EQ(from_a, 1);
    EXPECT_EQ(from_b, 1);

    q.close();
}

// After a query is demoted via update_statistics, a freshly-arrived L0 query is
// served ahead of it (strict absolute priority between levels).
TEST(LockFreePipelineMLFQTest, DemotedQueryYieldsToFreshQuery) {
    MultiCoreTaskQueue q(1);
    auto* qb = qkey(0xB); // will be demoted to a deeper level
    auto* qa = qkey(0xA); // stays at L0

    // qb enqueues first (producer created at L0) and accumulates enough runtime to be
    // demoted below L0.
    ASSERT_TRUE(q.push_back(make_task(qb, 5 * kSecondNs)).ok());

    // Charge qb's runtime so its producer is demoted (5s -> L1). update_statistics
    // reads query_runtime_ns() from the task, which the mock reports directly.
    auto qb_stat_task = make_task(qb, 5 * kSecondNs);
    q.update_statistics(qb_stat_task.get(), 0);

    // A fresh L0 query arrives.
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());

    // The L0 query wins despite being pushed after qb.
    auto t1 = q.take(0);
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qa);

    auto t2 = q.take(0);
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qb);

    q.close();
}

// remove_query drops the per-query state; a subsequent push for the same key
// recreates it and still works.
TEST(LockFreePipelineMLFQTest, RemoveQueryThenReuse) {
    MultiCoreTaskQueue q(1);
    auto* qa = qkey(0xA);

    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    auto t1 = q.take(0);
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qa);

    // The query finished; drop its node/token.
    q.remove_query(qa);

    // Re-using the same key recreates the node and enqueues normally.
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    auto t2 = q.take(0);
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qa);

    q.close();
}

} // namespace doris
