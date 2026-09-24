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

    void set_runtime_ns(uint64_t runtime_ns) { _runtime_ns = runtime_ns; }
    void set_active_task_num(int num) { _active_task_num = num; }
    void set_worker_cap(int cap) { _worker_cap = cap; }

private:
    QueryContext* _key;
    uint64_t _runtime_ns;
    bool _inelastic;
    int _active_task_num = 0;
    int _worker_cap = -1;
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
} // namespace

// A lower-attained query is staffed before a higher-attained query, regardless of
// push order: the scheduler assigns the single worker to the 0-runtime query, and
// the 5s query is only reached through the work-conserving fallback afterwards.
TEST(PushBasedTaskQueueTest, AbsolutePriorityAcrossQueries) {
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
TEST(PushBasedTaskQueueTest, EqualAttainedGreedyFcfs) {
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
TEST(PushBasedTaskQueueTest, DemandComesFromActiveTaskCount) {
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
TEST(PushBasedTaskQueueTest, AllocationAtEqualAttainedIsFifo) {
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
TEST(PushBasedTaskQueueTest, AllocationSpillsToHigherAttained) {
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
TEST(PushBasedTaskQueueTest, SteadyStateKeepsAllocation) {
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
TEST(PushBasedTaskQueueTest, QueryLocality) {
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
TEST(PushBasedTaskQueueTest, PreemptionByHigherPriorityQuery) {
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
TEST(PushBasedTaskQueueTest, HigherAttainedYieldsToFresherQuery) {
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
TEST(PushBasedTaskQueueTest, RebalanceMovesWorkerToLeastAttained) {
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

// pipeline_query_worker_cap bounds a single query even when its demand and the
// pool are larger; leftover cores spill to the next query.
TEST(PushBasedTaskQueueTest, WorkerCapLimitsGrant) {
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
TEST(PushBasedTaskQueueTest, SessionWorkerCapOverridesConfig) {
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
TEST(PushBasedTaskQueueTest, SessionWorkerCapZeroIsUnbounded) {
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
TEST(PushBasedTaskQueueTest, WorkerCapChangeTakesEffectAtRuntime) {
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

// Tasks with no QueryContext share one sentinel bucket. Idle does not reclaim it
// (no QUERY_TERMINATED will ever arrive); the state lives until queue close.
TEST(PushBasedTaskQueueTest, SentinelSurvivesIdle) {
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
