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

#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <set>

#include "common/config.h"
#include "exec/pipeline/pipeline_task.h"

namespace doris {

// Tests for the push-based, query-granular pipeline task queue. Level thresholds are
// derived from the query-global runtime surfaced via PipelineTask::query_runtime_ns()
// (<= 2.8s -> L0, (2.8s, 10s] -> L1, ...). Registry keys are TUniqueId values from
// query_id(); tests encode a fake QueryContext* into that id and never dereference it.
//
// The central scheduler runs on its own thread; tests use
// wait_scheduler_settled_for_test() to make its assignment decisions deterministic:
// it blocks until every event posted before the call (new queries, detach acks,
// demotions, terminate) has been processed and the resulting worker assignments have
// been dispatched.
static constexpr uint64_t kSecondNs = 1'000'000'000ULL;

class MockPipelineTask : public PipelineTask {
public:
    MockPipelineTask(QueryContext* key, uint64_t runtime_ns, bool inelastic = false)
            : _key(key), _runtime_ns(runtime_ns), _inelastic(inelastic) {}

    uint64_t query_runtime_ns() const override { return _runtime_ns; }
    QueryContext* query_ctx_raw() const override { return _key; }
    TUniqueId query_id() const override {
        TUniqueId id;
        id.lo = static_cast<int64_t>(reinterpret_cast<uintptr_t>(_key));
        return id;
    }
    bool is_inelastic() const override { return _inelastic; }

    void set_runtime_ns(uint64_t runtime_ns) { _runtime_ns = runtime_ns; }

private:
    QueryContext* _key;
    uint64_t _runtime_ns;
    bool _inelastic;
};

// Use a short empty-queue wait so tests don't block for the production 100ms.
class TestTaskQueue final : public MultiCoreTaskQueue {
public:
    explicit TestTaskQueue(int core_size, Mode mode = Mode::FULL)
            : MultiCoreTaskQueue(core_size, mode) {}
    PipelineTaskSPtr take(int core_id) override { return _take(core_id, 1); }
};

namespace {
QueryContext* qkey(uintptr_t id) {
    return reinterpret_cast<QueryContext*>(id);
}
TUniqueId qid(uintptr_t id) {
    TUniqueId tid;
    tid.lo = static_cast<int64_t>(id);
    return tid;
}
PipelineTaskSPtr make_task(QueryContext* key, uint64_t runtime_ns) {
    return std::make_shared<MockPipelineTask>(key, runtime_ns);
}
PipelineTaskSPtr make_inelastic_task(QueryContext* key, uint64_t runtime_ns) {
    return std::make_shared<MockPipelineTask>(key, runtime_ns, /*inelastic=*/true);
}
} // namespace

// A lower-runtime query is staffed before a higher-runtime query, regardless of push
// order: the scheduler assigns the single worker to the L0 query, and the L1 query is
// only reached through the work-conserving fallback afterwards.
TEST(PushBasedTaskQueueTest, AbsolutePriorityAcrossQueries) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA); // runtime 0 -> L0
    auto* qb = qkey(0xB); // runtime 5s -> L1

    // Push the lower-priority query first.
    ASSERT_TRUE(q.push_back(make_task(qb, 5 * kSecondNs)).ok());
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    q.wait_scheduler_settled_for_test();

    auto t1 = q.take(0);
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qa);

    // The assigned query is drained; the fallback picks up the unassigned one.
    auto t2 = q.take(0);
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qb);

    q.close();
}

// Within one level, the chunked fair-share dealing spreads distinct workers across
// co-resident queries rather than dogpiling one query.
TEST(PushBasedTaskQueueTest, WithinLevelFairSpread) {
    TestTaskQueue q(3);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);
    auto* qc = qkey(0xC);

    // All at L0, two tasks each.
    for (auto* key : {qa, qb, qc}) {
        ASSERT_TRUE(q.push_back(make_task(key, 0)).ok());
        ASSERT_TRUE(q.push_back(make_task(key, 0)).ok());
    }
    q.wait_scheduler_settled_for_test();

    // Three different workers each take once: they land on three different queries.
    std::set<QueryContext*> served;
    for (int worker = 0; worker < 3; ++worker) {
        auto t = q.take(worker);
        ASSERT_NE(t, nullptr) << "worker " << worker;
        served.insert(t->query_ctx_raw());
    }
    EXPECT_EQ(served.size(), 3);

    q.close();
}

// A worker keeps serving its assigned query across takes while that query has demand
// (locality: steady state generates no reassignments).
TEST(PushBasedTaskQueueTest, QueryLocality) {
    TestTaskQueue q(2);
    auto* qa = qkey(0xA); // L0
    auto* qb = qkey(0xB); // L1

    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    }
    ASSERT_TRUE(q.push_back(make_task(qb, 5 * kSecondNs)).ok());
    q.wait_scheduler_settled_for_test();

    // Worker 0 sticks to query A for all three of its tasks before touching B.
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

// A newly arrived higher-priority query pulls the worker off its current lower
// priority query: the scheduler overwrites the assignment slot and the worker
// obeys it on its next take (push-based preemption).
TEST(PushBasedTaskQueueTest, PreemptionByHigherPriorityQuery) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA); // L1 (5s)
    auto* qc = qkey(0xC); // L0 (0)

    ASSERT_TRUE(q.push_back(make_task(qa, 5 * kSecondNs)).ok());
    ASSERT_TRUE(q.push_back(make_task(qa, 5 * kSecondNs)).ok());
    q.wait_scheduler_settled_for_test();

    auto t1 = q.take(0); // worker latches onto A (only query present)
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qa);

    // Higher-priority query C arrives; the scheduler reassigns the worker.
    ASSERT_TRUE(q.push_back(make_task(qc, 0)).ok());
    q.wait_scheduler_settled_for_test();

    auto t2 = q.take(0); // preempts to C despite A still having a runnable task
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qc);

    q.close();
}

// Event-driven demotion: when a query's accumulated runtime crosses a level
// threshold, the CAS-winning release posts the demotion, the scheduler relinks the
// query at the deeper level and hands the worker to the fresh L0 query.
TEST(PushBasedTaskQueueTest, DemotionReassignsWorker) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);

    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok()); // L0 at push time
    }
    q.wait_scheduler_settled_for_test();

    auto t1 = q.take(0);
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qa);

    // The query burned 5s of runtime during this slice: releasing the task detects
    // the L0 -> L1 crossing and notifies the scheduler.
    static_cast<MockPipelineTask*>(t1.get())->set_runtime_ns(5 * kSecondNs);
    q.update_statistics(t1.get(), 1000);

    ASSERT_TRUE(q.push_back(make_task(qb, 0)).ok()); // fresh L0 query
    q.wait_scheduler_settled_for_test();

    // The worker was stripped from the demoted query and pushed to the L0 one.
    auto t2 = q.take(0);
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qb);

    // The demoted query is still drained through the fallback (work conservation).
    auto t3 = q.take(0);
    ASSERT_NE(t3, nullptr);
    EXPECT_EQ(t3->query_ctx_raw(), qa);

    q.close();
}

// Idle + detach does not reclaim a live query. Only terminate + workers_attached==0
// frees the QueryState. A later push after reclaim recreates it.
TEST(PushBasedTaskQueueTest, TerminateReclaimsAfterDetach) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA);

    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.registry_size_for_test(), 1);

    auto t = q.take(0);
    ASSERT_NE(t, nullptr);
    q.update_statistics(t.get(), 1000);

    // Worker observes idle and detaches; the query is still valid so state stays.
    EXPECT_EQ(q.take(0), nullptr);
    q.wait_scheduler_settled_for_test();
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.registry_size_for_test(), 1);

    q.notify_query_terminated(qid(0xA));
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.registry_size_for_test(), 0);

    // Recreate after reclaim (production will not enqueue after QueryContext dtor).
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    EXPECT_EQ(q.registry_size_for_test(), 1);
    q.wait_scheduler_settled_for_test();
    auto t2 = q.take(0);
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qa);
    q.update_statistics(t2.get(), 1000);

    q.close();
}

// Terminate while a worker is still attached: the scheduler writes a null
// assignment; after the worker acks, the state is reclaimed.
TEST(PushBasedTaskQueueTest, TerminateUnassignsAttachedWorker) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA);

    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    q.wait_scheduler_settled_for_test();

    auto t = q.take(0);
    ASSERT_NE(t, nullptr);
    q.update_statistics(t.get(), 1000);

    q.notify_query_terminated(qid(0xA));
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.take(0), nullptr);
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.registry_size_for_test(), 0);

    q.close();
}

// Work returning after a temporary drain keeps the same QueryState; idle never
// starts reclaim for a real query.
TEST(PushBasedTaskQueueTest, IdleDoesNotTeardownWhileQueryAlive) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA);

    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    q.wait_scheduler_settled_for_test();

    auto t = q.take(0);
    ASSERT_NE(t, nullptr);
    q.update_statistics(t.get(), 1000);
    q.wait_scheduler_settled_for_test();

    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    q.wait_scheduler_settled_for_test();
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.registry_size_for_test(), 1);

    auto t2 = q.take(0);
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qa);
    q.update_statistics(t2.get(), 1000);

    q.close();
}

// release_task (the "task already running elsewhere" re-queue dance) releases the
// in-flight slot without charging runtime, and the re-queued task is still served.
TEST(PushBasedTaskQueueTest, ReleaseAndRequeue) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA);

    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    q.wait_scheduler_settled_for_test();

    auto t = q.take(0);
    ASSERT_NE(t, nullptr);
    q.release_task(t.get());
    ASSERT_TRUE(q.push_back(t, 0).ok());

    auto t2 = q.take(0);
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2.get(), t.get());
    q.update_statistics(t2.get(), 1000);

    q.close();
}

// Degenerate mode (blocking pool): plain shared queue, no scheduler thread, no
// per-query state; produce/consume and accounting calls are safe.
TEST(PushBasedTaskQueueTest, GeneralOnlyMode) {
    TestTaskQueue q(1, MultiCoreTaskQueue::Mode::GENERAL_ONLY);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);

    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qb, 5 * kSecondNs)).ok());

    auto t1 = q.take(0);
    ASSERT_NE(t1, nullptr);
    q.update_statistics(t1.get(), 1000);
    auto t2 = q.take(0);
    ASSERT_NE(t2, nullptr);
    q.release_task(t2.get());
    EXPECT_EQ(q.take(0), nullptr);

    q.close();
    EXPECT_FALSE(q.push_back(make_task(qa, 0)).ok());
}

// "Inelastic first": a single-task pipeline's task outranks a pre-existing backlog
// from another query, including the worker's own assignment. Even though the worker
// was assigned to query A (the only query known to the scheduler when it settled),
// the inelastic task from query B is served first.
TEST(PushBasedTaskQueueTest, InelasticFirstBeatsBacklog) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA); // elastic backlog, L0
    auto* qb = qkey(0xB); // inelastic, L1 (worse MLFQ level - priority still wins)

    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    }
    q.wait_scheduler_settled_for_test(); // worker 0 assigned to A

    ASSERT_TRUE(q.push_back(make_inelastic_task(qb, 5 * kSecondNs)).ok());

    // The inelastic task jumps ahead of A's backlog and of the MLFQ.
    auto t1 = q.take(0);
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qb);
    q.update_statistics(t1.get(), 1000);

    // Elastic work is drained normally afterwards.
    for (int i = 0; i < 3; ++i) {
        auto t = q.take(0);
        ASSERT_NE(t, nullptr);
        EXPECT_EQ(t->query_ctx_raw(), qa) << "iteration " << i;
        q.update_statistics(t.get(), 1000);
    }

    q.close();
}

// Inelastic tasks keep full per-query accounting; idle does not reclaim, terminate
// after detach does. A later inelastic push recreates the state.
TEST(PushBasedTaskQueueTest, InelasticAccountingAndTeardown) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA);

    ASSERT_TRUE(q.push_back(make_inelastic_task(qa, 0)).ok());
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.registry_size_for_test(), 1);

    auto t = q.take(0);
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->query_ctx_raw(), qa);
    q.update_statistics(t.get(), 1000);

    EXPECT_EQ(q.take(0), nullptr);
    q.wait_scheduler_settled_for_test();
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.registry_size_for_test(), 1);

    q.notify_query_terminated(qid(0xA));
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.registry_size_for_test(), 0);

    ASSERT_TRUE(q.push_back(make_inelastic_task(qa, 0)).ok());
    EXPECT_EQ(q.registry_size_for_test(), 1);
    auto t2 = q.take(0);
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qa);
    q.update_statistics(t2.get(), 1000);

    q.close();
}

// One query mixing both kinds: the inelastic task is served first even if pushed
// last, everything drains, and the counters reconcile so teardown still happens.
TEST(PushBasedTaskQueueTest, InelasticMixedWithElasticSameQuery) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA);

    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_inelastic_task(qa, 0)).ok());
    q.wait_scheduler_settled_for_test();

    auto t1 = q.take(0);
    ASSERT_NE(t1, nullptr);
    EXPECT_TRUE(t1->is_inelastic());
    q.update_statistics(t1.get(), 1000);

    for (int i = 0; i < 2; ++i) {
        auto t = q.take(0);
        ASSERT_NE(t, nullptr);
        EXPECT_FALSE(t->is_inelastic()) << "iteration " << i;
        q.update_statistics(t.get(), 1000);
    }

    EXPECT_EQ(q.take(0), nullptr);
    q.wait_scheduler_settled_for_test();
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.registry_size_for_test(), 1);

    q.notify_query_terminated(qid(0xA));
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.registry_size_for_test(), 0);

    q.close();
}

// Tasks with no QueryContext share the sentinel bucket and are still reclaimed
// by idle + one-generation grace (no QUERY_TERMINATED will ever arrive).
TEST(PushBasedTaskQueueTest, SentinelIdleTeardown) {
    TestTaskQueue q(1);
    ASSERT_TRUE(q.push_back(make_task(nullptr, 0)).ok());
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.registry_size_for_test(), 1);

    auto t = q.take(0);
    ASSERT_NE(t, nullptr);
    q.update_statistics(t.get(), 1000);
    EXPECT_EQ(q.take(0), nullptr);
    q.wait_scheduler_settled_for_test();
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.registry_size_for_test(), 0);

    q.close();
}

// Degenerate mode ignores the inelastic flag: everything goes through the one shared
// queue in FIFO order.
TEST(PushBasedTaskQueueTest, GeneralOnlyModeIgnoresInelastic) {
    TestTaskQueue q(1, MultiCoreTaskQueue::Mode::GENERAL_ONLY);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);

    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_inelastic_task(qb, 0)).ok());

    // FIFO: the elastic task pushed first comes out first.
    auto t1 = q.take(0);
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qa);
    q.update_statistics(t1.get(), 1000);

    auto t2 = q.take(0);
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qb);
    q.update_statistics(t2.get(), 1000);

    q.close();
}

// close() rejects further pushes and unblocks takers.
TEST(PushBasedTaskQueueTest, CloseRejectsWork) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA);
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    q.close();
    EXPECT_FALSE(q.push_back(make_task(qa, 0)).ok());
    EXPECT_EQ(q.take(0), nullptr);
}

} // namespace doris
