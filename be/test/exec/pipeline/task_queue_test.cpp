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

#include <array>
#include <cstdint>
#include <memory>

#include "common/config.h"
#include "exec/pipeline/gittins_histogram.h"
#include "exec/pipeline/gittins_preset_distribution.h"
#include "exec/pipeline/pipeline_task.h"
#include "util/defer_op.h"

namespace doris {

// Tests for the push-based, query-granular pipeline task queue. Ranking is by
// attained service (PipelineTask::query_runtime_ns()), least first; equal service
// keeps arrival order. Registry keys are TUniqueId values from query_id(); tests
// encode a fake QueryContext* into that id and never dereference it.
//
// The central scheduler runs on its own thread; tests use
// wait_scheduler_settled_for_test() to make its assignment decisions deterministic:
// it blocks until every event posted before the call (new queries, detach acks,
// terminate) has been processed and the resulting worker assignments have been
// dispatched.
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
    // Defaults to 0, which leaves demand at sub-queue depth for tests that do not care
    // about the active-task signal (demand is the max of the two).
    int active_task_num() const override { return _active_task_num; }
    // Defaults to -1, i.e. no session-level override, so the BE config applies. The
    // base implementation would go through the QueryContext, which is a fake pointer
    // here and must never be dereferenced.
    int query_worker_cap() const override { return _worker_cap; }
    int64_t query_mem_bytes() const override { return _mem_bytes; }

    void set_runtime_ns(uint64_t runtime_ns) { _runtime_ns = runtime_ns; }
    void set_mem_bytes(int64_t bytes) { _mem_bytes = bytes; }
    void set_active_task_num(int num) { _active_task_num = num; }
    void set_worker_cap(int cap) { _worker_cap = cap; }

private:
    QueryContext* _key;
    uint64_t _runtime_ns;
    bool _inelastic;
    int _active_task_num = 0;
    int _worker_cap = -1;
    int64_t _mem_bytes = 0;
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
// A task reporting `active` live tasks for its query, i.e. a query whose demand exceeds
// what is sitting in its sub-queue.
PipelineTaskSPtr make_task_with_active(QueryContext* key, uint64_t runtime_ns, int active) {
    auto task = std::make_shared<MockPipelineTask>(key, runtime_ns);
    task->set_active_task_num(active);
    return task;
}
// As above, but the query also carries a session-level worker cap.
PipelineTaskSPtr make_task_with_active_and_cap(QueryContext* key, uint64_t runtime_ns, int active,
                                               int cap) {
    auto task = std::make_shared<MockPipelineTask>(key, runtime_ns);
    task->set_active_task_num(active);
    task->set_worker_cap(cap);
    return task;
}
PipelineTaskSPtr make_task_with_mem(QueryContext* key, uint64_t runtime_ns, int64_t mem_bytes) {
    auto task = std::make_shared<MockPipelineTask>(key, runtime_ns);
    task->set_mem_bytes(mem_bytes);
    return task;
}
} // namespace

// The least-attained-service tests below predate Gittins ranking and assume it is
// off; the pre-installed distribution would otherwise reorder queries by index.
class PushBasedTaskQueueTest : public testing::Test {
protected:
    void SetUp() override {
        _old_enable_gittins = config::enable_pipeline_gittins_scheduling;
        config::enable_pipeline_gittins_scheduling = false;
    }
    void TearDown() override { config::enable_pipeline_gittins_scheduling = _old_enable_gittins; }

private:
    bool _old_enable_gittins = true;
};

// A lower-attained query is staffed before a higher-attained query, regardless of
// push order: the scheduler assigns the single worker to the 0-runtime query, and
// the 5s query is only reached through the work-conserving fallback afterwards.
TEST_F(PushBasedTaskQueueTest, AbsolutePriorityAcrossQueries) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA); // runtime 0
    auto* qb = qkey(0xB); // runtime 5s

    // Push the higher-attained query first.
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

// At equal attained service, cores are dealt greedily in arrival order: the oldest
// query takes everything it can use before the next one is considered, so with three
// two-task queries and three workers the split is 2/1/0 in push order.
TEST_F(PushBasedTaskQueueTest, EqualAttainedGreedyFcfs) {
    TestTaskQueue q(3);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);
    auto* qc = qkey(0xC);

    // All at attained 0, two tasks each.
    for (auto* key : {qa, qb, qc}) {
        ASSERT_TRUE(q.push_back(make_task(key, 0)).ok());
        ASSERT_TRUE(q.push_back(make_task(key, 0)).ok());
    }
    q.wait_scheduler_settled_for_test();

    EXPECT_EQ(q.assigned_workers_for_test(qid(0xA)), 2);
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xB)), 1);
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xC)), 0);

    q.close();
}

// Demand is the query's active task count, not its sub-queue depth: a query with a
// single queued task but three live tasks (the rest blocked on dependencies elsewhere)
// is granted three cores, and the trailing query is left to the fallback.
TEST_F(PushBasedTaskQueueTest, DemandComesFromActiveTaskCount) {
    TestTaskQueue q(4);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);

    ASSERT_TRUE(q.push_back(make_task_with_active(qa, 0, /*active=*/3)).ok());
    ASSERT_TRUE(q.push_back(make_task_with_active(qb, 0, /*active=*/2)).ok());
    ASSERT_TRUE(q.push_back(make_task_with_active(qb, 0, /*active=*/2)).ok());
    q.wait_scheduler_settled_for_test();

    // Sub-queue depth alone would have granted A a single core.
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xA)), 3);
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xB)), 1);

    q.close();
}

// Arrival order decides who gets the cores: the same three queries pushed in the
// opposite order produce the mirrored allocation.
TEST_F(PushBasedTaskQueueTest, AllocationAtEqualAttainedIsFifo) {
    auto allocation = [](const std::array<uintptr_t, 3>& push_order) {
        TestTaskQueue q(3);
        for (uintptr_t id : push_order) {
            EXPECT_TRUE(q.push_back(make_task(qkey(id), 0)).ok());
            EXPECT_TRUE(q.push_back(make_task(qkey(id), 0)).ok());
        }
        q.wait_scheduler_settled_for_test();
        std::array<int, 3> assigned {};
        for (size_t i = 0; i < push_order.size(); ++i) {
            assigned[i] = q.assigned_workers_for_test(qid(push_order[i]));
        }
        q.close();
        return assigned;
    };

    // Indexed by push position, not by query id: the first pushed always wins.
    EXPECT_EQ(allocation({0xA, 0xB, 0xC}), (std::array<int, 3> {2, 1, 0}));
    EXPECT_EQ(allocation({0xC, 0xB, 0xA}), (std::array<int, 3> {2, 1, 0}));
}

// Least attained is satisfied to its full demand first; leftover cores spill to
// higher-attained queries, and equal attained among those keeps arrival order.
TEST_F(PushBasedTaskQueueTest, AllocationSpillsToHigherAttained) {
    TestTaskQueue q(3);
    auto* qa = qkey(0xA); // runtime 0
    auto* qb = qkey(0xB); // runtime 5s, first of the two higher-attained queries
    auto* qc = qkey(0xC); // runtime 5s

    // Pushed highest-attained first to show sort order beats arrival order.
    ASSERT_TRUE(q.push_back(make_task(qb, 5 * kSecondNs)).ok());
    ASSERT_TRUE(q.push_back(make_task(qc, 5 * kSecondNs)).ok());
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    q.wait_scheduler_settled_for_test();

    EXPECT_EQ(q.assigned_workers_for_test(qid(0xA)), 2);
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xB)), 1);
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xC)), 0);

    q.close();
}

// Once settled, repeated scheduler passes with no state change leave the allocation
// exactly where it was.
TEST_F(PushBasedTaskQueueTest, SteadyStateKeepsAllocation) {
    TestTaskQueue q(2);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);

    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    }
    ASSERT_TRUE(q.push_back(make_task(qb, 0)).ok());
    q.wait_scheduler_settled_for_test();

    const int a_assigned = q.assigned_workers_for_test(qid(0xA));
    const int b_assigned = q.assigned_workers_for_test(qid(0xB));
    EXPECT_EQ(a_assigned, 2);
    EXPECT_EQ(b_assigned, 0);

    for (int i = 0; i < 3; ++i) {
        q.wait_scheduler_settled_for_test();
        EXPECT_EQ(q.assigned_workers_for_test(qid(0xA)), a_assigned) << "pass " << i;
        EXPECT_EQ(q.assigned_workers_for_test(qid(0xB)), b_assigned) << "pass " << i;
    }

    q.close();
}

// A worker keeps serving its assigned query across takes while that query has demand
// (locality: steady state generates no reassignments).
TEST_F(PushBasedTaskQueueTest, QueryLocality) {
    TestTaskQueue q(2);
    auto* qa = qkey(0xA); // attained 0
    auto* qb = qkey(0xB); // attained 5s

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

// A newly arrived lower-attained query pulls the worker off its current
// higher-attained query: the scheduler overwrites the assignment slot and the
// worker obeys it on its next take (push-based preemption).
TEST_F(PushBasedTaskQueueTest, PreemptionByHigherPriorityQuery) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA); // attained 5s
    auto* qc = qkey(0xC); // attained 0

    ASSERT_TRUE(q.push_back(make_task(qa, 5 * kSecondNs)).ok());
    ASSERT_TRUE(q.push_back(make_task(qa, 5 * kSecondNs)).ok());
    q.wait_scheduler_settled_for_test();

    auto t1 = q.take(0); // worker latches onto A (only query present)
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qa);

    // Lower-attained query C arrives; the scheduler reassigns the worker.
    ASSERT_TRUE(q.push_back(make_task(qc, 0)).ok());
    q.wait_scheduler_settled_for_test();

    auto t2 = q.take(0); // preempts to C despite A still having a runnable task
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qc);

    q.close();
}

// After a running query accumulates more attained service, a fresh zero-attained
// query is staffed first on the next settled rebalance.
TEST_F(PushBasedTaskQueueTest, HigherAttainedYieldsToFresherQuery) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);

    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    }
    q.wait_scheduler_settled_for_test();

    auto t1 = q.take(0);
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qa);

    // A burned 5s of runtime during this slice; B arrives at attained 0.
    static_cast<MockPipelineTask*>(t1.get())->set_runtime_ns(5 * kSecondNs);
    q.update_statistics(t1.get(), 1000);

    ASSERT_TRUE(q.push_back(make_task(qb, 0)).ok());
    q.wait_scheduler_settled_for_test();

    auto t2 = q.take(0);
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qb);

    // The higher-attained query is still drained through the fallback.
    auto t3 = q.take(0);
    ASSERT_NE(t3, nullptr);
    EXPECT_EQ(t3->query_ctx_raw(), qa);

    q.close();
}

// A and B start at equal attained service (A arrives first and holds the worker).
// After A accumulates service, the next settled pass moves the worker to B.
TEST_F(PushBasedTaskQueueTest, RebalanceMovesWorkerToLeastAttained) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);

    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qb, 0)).ok());
    q.wait_scheduler_settled_for_test();

    EXPECT_EQ(q.assigned_workers_for_test(qid(0xA)), 1);
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xB)), 0);

    auto t1 = q.take(0);
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qa);
    static_cast<MockPipelineTask*>(t1.get())->set_runtime_ns(5 * kSecondNs);
    q.update_statistics(t1.get(), 1000);
    q.wait_scheduler_settled_for_test();

    EXPECT_EQ(q.assigned_workers_for_test(qid(0xB)), 1);
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xA)), 0);

    auto t2 = q.take(0);
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qb);

    q.close();
}

// Attained service is the query's CPUContext CPU time (query_runtime_ns()), not the
// wall-clock time the scheduler measures: a long update_statistics() slice leaves the
// ranking alone, while CPU charged outside the pipeline workers (e.g. by scanners) is
// picked up the next time one of the query's tasks is enqueued.
TEST_F(PushBasedTaskQueueTest, AttainedFollowsQueryCpuNotSchedulerTime) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);

    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qb, 0)).ok());
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xA)), 1);

    auto t1 = q.take(0);
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qa);
    // 5s of wall-clock in the scheduler, but no CPU charged to A's CPUContext.
    q.update_statistics(t1.get(), static_cast<int64_t>(5 * kSecondNs));
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xA)), 1);
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xB)), 0);

    // A's scanners charged 5s of CPU; its next enqueued task carries that value.
    ASSERT_TRUE(q.push_back(make_task(qa, 5 * kSecondNs)).ok());
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xB)), 1);
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xA)), 0);

    q.close();
}

// pipeline_query_worker_cap bounds a single query even when its demand and the
// pool are larger; leftover cores spill to the next query.
TEST_F(PushBasedTaskQueueTest, WorkerCapLimitsGrant) {
    const int32_t old_cap = config::pipeline_query_worker_cap;
    config::pipeline_query_worker_cap = 8;
    Defer restore_cap {[&]() { config::pipeline_query_worker_cap = old_cap; }};
    TestTaskQueue q(12);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);

    ASSERT_TRUE(q.push_back(make_task_with_active(qa, 0, /*active=*/20)).ok());
    ASSERT_TRUE(q.push_back(make_task_with_active(qb, 0, /*active=*/5)).ok());
    q.wait_scheduler_settled_for_test();

    EXPECT_EQ(q.assigned_workers_for_test(qid(0xA)), 8);
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xB)), 4);

    q.close();
}

// A query carrying its own pipeline_query_worker_cap session variable is bounded by
// that value instead of the BE config, and only that query is affected: the query
// without an override still gets the config's 8.
TEST_F(PushBasedTaskQueueTest, SessionWorkerCapOverridesConfig) {
    const int32_t old_cap = config::pipeline_query_worker_cap;
    config::pipeline_query_worker_cap = 8;
    Defer restore_cap {[&]() { config::pipeline_query_worker_cap = old_cap; }};
    TestTaskQueue q(12);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);

    ASSERT_TRUE(q.push_back(make_task_with_active_and_cap(qa, 0, /*active=*/20, /*cap=*/3)).ok());
    ASSERT_TRUE(q.push_back(make_task_with_active(qb, 0, /*active=*/20)).ok());
    q.wait_scheduler_settled_for_test();

    EXPECT_EQ(q.assigned_workers_for_test(qid(0xA)), 3);
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xB)), 8);

    q.close();
}

// A session cap of 0 means unbounded, so it can also lift a restrictive BE config.
TEST_F(PushBasedTaskQueueTest, SessionWorkerCapZeroIsUnbounded) {
    const int32_t old_cap = config::pipeline_query_worker_cap;
    config::pipeline_query_worker_cap = 4;
    Defer restore_cap {[&]() { config::pipeline_query_worker_cap = old_cap; }};
    TestTaskQueue q(6);
    auto* qa = qkey(0xA);

    ASSERT_TRUE(q.push_back(make_task_with_active_and_cap(qa, 0, /*active=*/20, /*cap=*/0)).ok());
    q.wait_scheduler_settled_for_test();

    EXPECT_EQ(q.assigned_workers_for_test(qid(0xA)), 6);

    q.close();
}

// The cap is re-read on every rebalance pass, so raising it after the query is already
// running widens the grant without any restart or re-push.
TEST_F(PushBasedTaskQueueTest, WorkerCapChangeTakesEffectAtRuntime) {
    const int32_t old_cap = config::pipeline_query_worker_cap;
    config::pipeline_query_worker_cap = 2;
    Defer restore_cap {[&]() { config::pipeline_query_worker_cap = old_cap; }};
    TestTaskQueue q(12);
    auto* qa = qkey(0xA);

    ASSERT_TRUE(q.push_back(make_task_with_active(qa, 0, /*active=*/20)).ok());
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xA)), 2);

    // Let every worker run once so the two assigned ones ack their slot write; only
    // acked workers are eligible to be kept in place or moved by the next pass. The
    // dequeued task is held (never released), so the query stays non-idle and its
    // demand stays at the active count.
    PipelineTaskSPtr held;
    for (int i = 0; i < 12; ++i) {
        auto task = q.take(i);
        if (task != nullptr) {
            held = std::move(task);
        }
    }
    ASSERT_NE(held, nullptr);
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xA)), 2);

    config::pipeline_query_worker_cap = 8;
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xA)), 8);

    q.close();
}

// Idle + detach does not reclaim a live query. Only terminate + workers_attached==0
// frees the QueryState. A later push after reclaim recreates it.
TEST_F(PushBasedTaskQueueTest, TerminateReclaimsAfterDetach) {
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
TEST_F(PushBasedTaskQueueTest, TerminateUnassignsAttachedWorker) {
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
TEST_F(PushBasedTaskQueueTest, IdleDoesNotTeardownWhileQueryAlive) {
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
TEST_F(PushBasedTaskQueueTest, ReleaseAndRequeue) {
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
TEST_F(PushBasedTaskQueueTest, GeneralOnlyMode) {
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
TEST_F(PushBasedTaskQueueTest, InelasticFirstBeatsBacklog) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA); // elastic backlog
    auto* qb = qkey(0xB); // inelastic, higher attained — still served first

    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    }
    q.wait_scheduler_settled_for_test(); // worker 0 assigned to A

    ASSERT_TRUE(q.push_back(make_inelastic_task(qb, 5 * kSecondNs)).ok());

    // The inelastic task jumps ahead of A's backlog and of attained-service ranking.
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
TEST_F(PushBasedTaskQueueTest, InelasticAccountingAndTeardown) {
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
TEST_F(PushBasedTaskQueueTest, InelasticMixedWithElasticSameQuery) {
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

// Tasks with no QueryContext share one sentinel bucket. Idle does not reclaim it
// (no QUERY_TERMINATED will ever arrive); the state lives until queue close.
TEST_F(PushBasedTaskQueueTest, SentinelSurvivesIdle) {
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
    EXPECT_EQ(q.registry_size_for_test(), 1);

    q.close();
}

// Degenerate mode ignores the inelastic flag: everything goes through the one shared
// queue in FIFO order.
TEST_F(PushBasedTaskQueueTest, GeneralOnlyModeIgnoresInelastic) {
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
TEST_F(PushBasedTaskQueueTest, CloseRejectsWork) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA);
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    q.close();
    EXPECT_FALSE(q.push_back(make_task(qa, 0)).ok());
    EXPECT_EQ(q.take(0), nullptr);
}

// ---------------------------------------------------------------------------
// Gittins index
// ---------------------------------------------------------------------------

TEST(GittinsHistogramTest, EmptyHistoryIsZeroEverywhere) {
    GittinsHistogram h(8);
    auto table = h.build();
    EXPECT_EQ(table->total_samples(), 0);
    for (size_t slot = 0; slot < table->num_slots(); ++slot) {
        EXPECT_EQ(table->index_of_slot(slot), 0.0) << "slot " << slot;
    }
    EXPECT_EQ(table->index(100 * kSecondNs), 0.0);
}

// Every query finishes in [2s, 3s). The index peaks right before the completion
// mass, decays with the lookahead needed to reach it, and is 0 past it.
TEST(GittinsHistogramTest, IndexPeaksBeforeCompletionMass) {
    GittinsHistogram h(8);
    for (int i = 0; i < 10; ++i) {
        h.record(2 * kSecondNs + kSecondNs / 2);
    }
    auto table = h.build();
    EXPECT_DOUBLE_EQ(table->index_of_slot(0), 0.25); // reached at d = 4
    EXPECT_DOUBLE_EQ(table->index_of_slot(1), 0.5);  // reached at d = 2
    EXPECT_DOUBLE_EQ(table->index_of_slot(2), 1.0);  // reached at d = 1
    EXPECT_DOUBLE_EQ(table->index_of_slot(3), 0.0);  // no survivors
    // Lookup floors attained service into its slot.
    EXPECT_DOUBLE_EQ(table->index(kSecondNs + kSecondNs / 2), 0.5);
}

// The probability is conditional on having survived to the current slot, so queries
// that already outlived the short half of a bimodal distribution are compared only
// against the long half.
TEST(GittinsHistogramTest, ConditionsOnSurvivors) {
    GittinsHistogram h(8);
    for (int i = 0; i < 5; ++i) {
        h.record(kSecondNs / 2);     // slot 0
        h.record(4 * kSecondNs + 1); // slot 4
    }
    auto table = h.build();
    EXPECT_DOUBLE_EQ(table->index_of_slot(0), 0.5);  // 5 of 10 finish within d = 1
    EXPECT_DOUBLE_EQ(table->index_of_slot(1), 0.25); // 5 of 5 finish within d = 4
    EXPECT_DOUBLE_EQ(table->index_of_slot(3), 0.5);  // 5 of 5 finish within d = 2
    EXPECT_DOUBLE_EQ(table->index_of_slot(4), 1.0);
    EXPECT_DOUBLE_EQ(table->index_of_slot(5), 0.0);
}

// Final times past the array land in the last slot, and attained service past the
// array is looked up there too.
TEST(GittinsHistogramTest, ClampsIntoLastSlot) {
    GittinsHistogram h(4);
    h.record(100 * kSecondNs);
    auto table = h.build();
    EXPECT_EQ(table->total_samples(), 1);
    EXPECT_DOUBLE_EQ(table->index_of_slot(3), 1.0);
    EXPECT_DOUBLE_EQ(table->index(1000 * kSecondNs), 1.0);
    EXPECT_DOUBLE_EQ(table->index_of_slot(0), 0.25); // reached at d = 4 = whole array
}

// A built table is a snapshot: later samples only show up in the next build, and
// the version tells a rebuilder whether there is anything new.
TEST(GittinsHistogramTest, BuildIsSnapshot) {
    GittinsHistogram h(8);
    const uint64_t v0 = h.version();
    auto before = h.build();
    h.record(2 * kSecondNs);
    EXPECT_NE(h.version(), v0);
    EXPECT_EQ(before->total_samples(), 0);
    EXPECT_DOUBLE_EQ(before->index_of_slot(2), 0.0);
    auto after = h.build();
    EXPECT_EQ(after->total_samples(), 1);
    EXPECT_DOUBLE_EQ(after->index_of_slot(2), 1.0);
}

// The pre-installed distribution's longest query took ~410.6s, so slot 410 has one
// survivor that finishes within the next second, and nothing survives past it.
TEST(GittinsPresetTest, TableMatchesPreset) {
    auto table = build_preset_gittins_table(512);
    EXPECT_EQ(table->total_samples(), kGittinsPresetCpuTimeMs.size());
    EXPECT_DOUBLE_EQ(table->index(410 * kSecondNs + kSecondNs / 2), 1.0);
    EXPECT_DOUBLE_EQ(table->index(411 * kSecondNs), 0.0);
    EXPECT_DOUBLE_EQ(table->index(450 * kSecondNs), 0.0);
    // Many short queries finish in [1s, 2s), so a query that has used 1.5s is more
    // likely to finish soon than a fresh one.
    EXPECT_GT(table->index(kSecondNs + kSecondNs / 2), table->index(0));
}

// With the preset, a query at 1.5s of attained service outranks a fresh one - the
// reverse of least-attained-service. Turning the policy off restores
// least-attained-service on the next pass.
TEST(GittinsTaskQueueTest, PresetOutranksLeastAttained) {
    const bool old_enable = config::enable_pipeline_gittins_scheduling;
    config::enable_pipeline_gittins_scheduling = true;
    Defer restore {[&]() { config::enable_pipeline_gittins_scheduling = old_enable; }};
    TestTaskQueue q(1);
    auto* qa = qkey(0xA); // fresh
    auto* qb = qkey(0xB); // 1.5s attained
    const uint64_t b_attained = kSecondNs + kSecondNs / 2;

    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qb, b_attained)).ok());
    q.wait_scheduler_settled_for_test();

    EXPECT_EQ(q.assigned_workers_for_test(qid(0xB)), 1);
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xA)), 0);
    ASSERT_TRUE(q.push_back(make_task(qb, b_attained)).ok());
    auto t1 = q.take(0);
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qb);

    config::enable_pipeline_gittins_scheduling = false;
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xA)), 1);
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xB)), 0);
    auto t2 = q.take(0); // preempts to A despite B still having a runnable task
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qa);

    q.close();
}

// Finishing queries do not change the preset ranking.
TEST(GittinsTaskQueueTest, TerminationDoesNotChangeRanking) {
    const bool old_enable = config::enable_pipeline_gittins_scheduling;
    config::enable_pipeline_gittins_scheduling = true;
    Defer restore {[&]() { config::enable_pipeline_gittins_scheduling = old_enable; }};
    TestTaskQueue q(1);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);
    const uint64_t b_attained = kSecondNs + kSecondNs / 2;

    for (uintptr_t i = 0; i < 50; ++i) {
        q.notify_query_terminated(qid(0x100 + i));
    }
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qb, b_attained)).ok());
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xB)), 1);
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xA)), 0);

    q.close();
}

TEST(GittinsMemoryCostTest, PressureIsZeroUpToThreshold) {
    EXPECT_DOUBLE_EQ(gittins_memory_pressure(0.0, 0.8), 0.0);
    EXPECT_DOUBLE_EQ(gittins_memory_pressure(0.5, 0.8), 0.0);
    EXPECT_DOUBLE_EQ(gittins_memory_pressure(0.8, 0.8), 0.0);
    EXPECT_DOUBLE_EQ(gittins_memory_pressure(-1.0, 0.8), 0.0);
}

TEST(GittinsMemoryCostTest, PressureIsQuadraticAboveThreshold) {
    EXPECT_NEAR(gittins_memory_pressure(0.9, 0.8), 0.01 / 0.2, 1e-12);
    // At full capacity the pressure is (1 - t).
    EXPECT_NEAR(gittins_memory_pressure(1.0, 0.8), 0.2, 1e-12);
    EXPECT_NEAR(gittins_memory_pressure(1.0, 0.5), 0.5, 1e-12);
    // Continuous and increasing past the threshold.
    EXPECT_LT(gittins_memory_pressure(0.81, 0.8), gittins_memory_pressure(0.85, 0.8));
    EXPECT_LT(gittins_memory_pressure(0.81, 0.8), 1e-3);
}

TEST(GittinsMemoryCostTest, HoldingCostRoundsToGigabytes) {
    constexpr int64_t kGb = 1024LL * 1024 * 1024;
    EXPECT_DOUBLE_EQ(gittins_holding_cost(8 * kGb, 0.0), 1.0);
    EXPECT_DOUBLE_EQ(gittins_holding_cost(0, 0.5), 1.0);
    EXPECT_DOUBLE_EQ(gittins_holding_cost(-kGb, 0.5), 1.0);
    // 0.4 GB rounds to 0, 1.6 GB rounds to 2.
    EXPECT_DOUBLE_EQ(gittins_holding_cost(kGb * 4 / 10, 0.5), 1.0);
    EXPECT_DOUBLE_EQ(gittins_holding_cost(kGb * 16 / 10, 0.5), 2.0);
    EXPECT_DOUBLE_EQ(gittins_holding_cost(8 * kGb, 0.2), 2.6);
}

// Two queries at the same attained service share a table index, so without memory
// pressure arrival order wins. Above the threshold the 8 GB query's holding cost lifts
// it ahead; once pressure drops the ranking returns to arrival order.
TEST(GittinsTaskQueueTest, MemoryPressureFavorsMemoryHeavyQuery) {
    const bool old_enable = config::enable_pipeline_gittins_scheduling;
    const bool old_mem_cost = config::enable_pipeline_gittins_memory_cost;
    const double old_threshold = config::pipeline_gittins_mem_pressure_threshold;
    config::enable_pipeline_gittins_scheduling = true;
    config::enable_pipeline_gittins_memory_cost = true;
    config::pipeline_gittins_mem_pressure_threshold = 0.8;
    Defer restore {[&]() {
        config::enable_pipeline_gittins_scheduling = old_enable;
        config::enable_pipeline_gittins_memory_cost = old_mem_cost;
        config::pipeline_gittins_mem_pressure_threshold = old_threshold;
    }};
    constexpr int64_t kGb = 1024LL * 1024 * 1024;
    TestTaskQueue q(1);
    q.set_memory_usage_ratio_for_test(0.5);
    auto* qa = qkey(0xA); // no memory
    auto* qb = qkey(0xB); // 8 GB

    ASSERT_TRUE(q.push_back(make_task_with_mem(qa, 0, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task_with_mem(qa, 0, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task_with_mem(qb, 0, 8 * kGb)).ok());
    ASSERT_TRUE(q.push_back(make_task_with_mem(qb, 0, 8 * kGb)).ok());
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xA)), 1);
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xB)), 0);
    auto t1 = q.take(0); // acks the assignment so the worker can be moved
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qa);

    q.set_memory_usage_ratio_for_test(1.0);
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xB)), 1);
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xA)), 0);
    auto t2 = q.take(0);
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qb);

    // Turning the cost off restores arrival order even under pressure.
    config::enable_pipeline_gittins_memory_cost = false;
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xA)), 1);
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xB)), 0);

    q.close();
}

} // namespace doris
