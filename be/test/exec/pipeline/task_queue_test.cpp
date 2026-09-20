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
#include <string>

#include "common/config.h"
#include "exec/pipeline/pipeline_task.h"

namespace doris {

// Tests for the query-granular pipeline task queue. Ranking is by attained service
// (PipelineTask::query_runtime_ns(), the query-global counter that the pipeline workers
// and the scanner threads both charge), least first; equal service keeps arrival order.
// The top of that ranking is published into a shared slot array, slot 0 holding the
// least-attained query. Registry keys are TUniqueId values from query_id(); tests encode
// a fake QueryContext* into that id and never dereference it.
//
// The central scheduler runs on its own thread; tests use
// wait_scheduler_settled_for_test() to make its decisions deterministic: it blocks until
// every event posted before the call (new queries, terminate) has been processed and the
// resulting array has been published.
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

    void set_runtime_ns(uint64_t runtime_ns) { _runtime_ns = runtime_ns; }
    void set_active_task_num(int num) { _active_task_num = num; }

private:
    QueryContext* _key;
    uint64_t _runtime_ns;
    bool _inelastic;
    int _active_task_num = 0;
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
} // namespace

class PushBasedTaskQueueTest : public testing::Test {
protected:
    // Start every test from the shipped defaults so a be.conf in the environment cannot
    // change what they assert, and restore whatever was there afterwards.
    void SetUp() override {
        _old_slot_count = config::pipeline_las_slot_count;
        _old_slot_policy = config::pipeline_las_slot_policy;
        set_slots(8, "ordered");
    }
    void TearDown() override {
        config::pipeline_las_slot_count = _old_slot_count;
        config::pipeline_las_slot_policy = _old_slot_policy;
    }
    static void set_slots(int slot_count, const std::string& policy) {
        config::pipeline_las_slot_count = slot_count;
        config::pipeline_las_slot_policy = policy;
    }

private:
    int32_t _old_slot_count = 0;
    std::string _old_slot_policy;
};

// A lower-attained query takes slot 0 regardless of push order, and a worker walking the
// array serves it first; the higher-attained query is reached from slot 1 once slot 0
// runs dry.
TEST_F(PushBasedTaskQueueTest, AbsolutePriorityAcrossQueries) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA); // runtime 0
    auto* qb = qkey(0xB); // runtime 5s

    // Push the higher-attained query first.
    ASSERT_TRUE(q.push_back(make_task(qb, 5 * kSecondNs)).ok());
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    q.wait_scheduler_settled_for_test();

    EXPECT_EQ(q.slot_of_query_for_test(qid(0xA)), 0);
    EXPECT_EQ(q.slot_of_query_for_test(qid(0xB)), 1);

    auto t1 = q.take(0);
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qa);

    // Slot 0 is drained, so the walk continues into slot 1.
    auto t2 = q.take(0);
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qb);

    q.close();
}

// At equal attained service the array keeps arrival order, so the slot a query gets is
// decided by when it first pushed - mirroring the push order mirrors the slots.
TEST_F(PushBasedTaskQueueTest, RankingAtEqualAttainedIsFifo) {
    auto slots = [](const std::array<uintptr_t, 3>& push_order) {
        TestTaskQueue q(1);
        for (uintptr_t id : push_order) {
            EXPECT_TRUE(q.push_back(make_task(qkey(id), 0)).ok());
        }
        q.wait_scheduler_settled_for_test();
        std::array<int, 3> assigned {};
        for (size_t i = 0; i < push_order.size(); ++i) {
            assigned[i] = q.slot_of_query_for_test(qid(push_order[i]));
        }
        q.close();
        return assigned;
    };

    // Indexed by push position, not by query id: the first pushed always takes slot 0.
    EXPECT_EQ(slots({0xA, 0xB, 0xC}), (std::array<int, 3> {0, 1, 2}));
    EXPECT_EQ(slots({0xC, 0xB, 0xA}), (std::array<int, 3> {0, 1, 2}));
}

// Attained service beats arrival order: the query pushed last but with no service yet
// takes slot 0, and the two queries at equal higher service follow in arrival order.
TEST_F(PushBasedTaskQueueTest, RankingOrdersSlotsByAttainedService) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA); // runtime 0
    auto* qb = qkey(0xB); // runtime 5s, first of the two higher-attained queries
    auto* qc = qkey(0xC); // runtime 5s

    ASSERT_TRUE(q.push_back(make_task(qb, 5 * kSecondNs)).ok());
    ASSERT_TRUE(q.push_back(make_task(qc, 5 * kSecondNs)).ok());
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    q.wait_scheduler_settled_for_test();

    EXPECT_EQ(q.slot_of_query_for_test(qid(0xA)), 0);
    EXPECT_EQ(q.slot_of_query_for_test(qid(0xB)), 1);
    EXPECT_EQ(q.slot_of_query_for_test(qid(0xC)), 2);

    q.close();
}

// What CPU burned outside this pool - scanner threads, above all - looks like from here:
// the query-global counter grows with no charge of our own, and the ranking picks the new
// total up on the query's next enqueue. That enqueue is also how a task woken by a scan
// block re-enters the queue, so a scan-bound query cannot win slot 0 on a stale value.
TEST_F(PushBasedTaskQueueTest, AttainedServiceComesFromTheQueryCounterOnEnqueue) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);

    // Both start with no attained service; A arrives first, so it holds slot 0.
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qb, 0)).ok());
    q.wait_scheduler_settled_for_test();
    ASSERT_EQ(q.slot_of_query_for_test(qid(0xA)), 0);
    ASSERT_EQ(q.slot_of_query_for_test(qid(0xB)), 1);

    // A burns 5s on scanner threads: no task of A ran here, nothing was charged through
    // update_statistics, but its next enqueue carries the counter's new value.
    ASSERT_TRUE(q.push_back(make_task(qa, 5 * kSecondNs)).ok());
    q.wait_scheduler_settled_for_test();

    EXPECT_EQ(q.slot_of_query_for_test(qid(0xB)), 0);
    EXPECT_EQ(q.slot_of_query_for_test(qid(0xA)), 1);

    // The worker follows the array, so B is served ahead of A's backlog.
    auto t = q.take(0);
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->query_ctx_raw(), qb);

    q.close();
}

// A query only holds a slot while it has work to do: once its last task is released the
// next pass drops it from the array, without reclaiming the still-live query.
TEST_F(PushBasedTaskQueueTest, QueryWithoutDemandLosesItsSlot) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA);

    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.slot_of_query_for_test(qid(0xA)), 0);

    auto t = q.take(0);
    ASSERT_NE(t, nullptr);
    q.update_statistics(t.get(), 1000);
    q.wait_scheduler_settled_for_test();

    EXPECT_EQ(q.slot_of_query_for_test(qid(0xA)), -1);
    EXPECT_EQ(q.registry_size_for_test(), 1);

    q.close();
}

// Demand is the query's active task count, not its sub-queue depth: a query whose
// sub-queue is empty but whose other fragments still have runnable tasks keeps its slot,
// which is what lets it pick up again without waiting to be ranked back in.
TEST_F(PushBasedTaskQueueTest, DemandComesFromActiveTaskCount) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA);

    ASSERT_TRUE(q.push_back(make_task_with_active(qa, 0, /*active=*/3)).ok());
    q.wait_scheduler_settled_for_test();

    auto t = q.take(0);
    ASSERT_NE(t, nullptr);
    q.update_statistics(t.get(), 1000);
    q.wait_scheduler_settled_for_test();

    // Sub-queue depth alone would have dropped A from the array here.
    EXPECT_EQ(q.slot_of_query_for_test(qid(0xA)), 0);

    q.close();
}

// The array is exactly as long as the configured slot count; queries past it hold no
// slot and are served only by the work-conserving fallback. The knob is re-read on every
// pass, so growing it admits the waiting query without a restart or a re-push.
TEST_F(PushBasedTaskQueueTest, SlotCountChangeTakesEffectAtRuntime) {
    set_slots(2, "ordered");
    TestTaskQueue q(1);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);
    auto* qc = qkey(0xC);

    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qb, 1 * kSecondNs)).ok());
    ASSERT_TRUE(q.push_back(make_task(qc, 2 * kSecondNs)).ok());
    q.wait_scheduler_settled_for_test();

    EXPECT_EQ(q.slot_count_for_test(), 2);
    EXPECT_EQ(q.slot_of_query_for_test(qid(0xA)), 0);
    EXPECT_EQ(q.slot_of_query_for_test(qid(0xB)), 1);
    EXPECT_EQ(q.slot_of_query_for_test(qid(0xC)), -1);

    config::pipeline_las_slot_count = 4;
    q.wait_scheduler_settled_for_test();

    EXPECT_EQ(q.slot_count_for_test(), 4);
    EXPECT_EQ(q.slot_of_query_for_test(qid(0xC)), 2);

    q.close();
}

// A nonsensical slot count still leaves one slot, so the least-attained query is staffed
// and everyone else drains through the fallback.
TEST_F(PushBasedTaskQueueTest, SlotCountIsClampedToOne) {
    set_slots(0, "ordered");
    TestTaskQueue q(1);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);

    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qb, 5 * kSecondNs)).ok());
    q.wait_scheduler_settled_for_test();

    EXPECT_EQ(q.slot_count_for_test(), 1);
    EXPECT_EQ(q.slot_of_query_for_test(qid(0xA)), 0);
    EXPECT_EQ(q.slot_of_query_for_test(qid(0xB)), -1);

    auto t1 = q.take(0);
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qa);
    auto t2 = q.take(0);
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qb);

    q.close();
}

// Ordered policy: which worker asks is irrelevant, every one of them starts at slot 0.
TEST_F(PushBasedTaskQueueTest, OrderedPolicyIgnoresWorkerIdentity) {
    set_slots(2, "ordered");
    TestTaskQueue q(2);
    auto* qa = qkey(0xA); // attained 0 -> slot 0
    auto* qb = qkey(0xB); // attained 5s -> slot 1

    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qb, 5 * kSecondNs)).ok());
    q.wait_scheduler_settled_for_test();
    ASSERT_EQ(q.slot_of_query_for_test(qid(0xA)), 0);
    ASSERT_EQ(q.slot_of_query_for_test(qid(0xB)), 1);

    auto t = q.take(1);
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->query_ctx_raw(), qa);

    q.close();
}

// Fixed policy: a worker serves its own slot even when a lower slot holds a
// less-attained query with work waiting, which is what bounds per-query parallelism to
// workers-per-slot.
TEST_F(PushBasedTaskQueueTest, FixedPolicyServesOnlyItsOwnSlot) {
    set_slots(2, "fixed");
    TestTaskQueue q(2);
    auto* qa = qkey(0xA); // attained 0 -> slot 0, served by worker 0
    auto* qb = qkey(0xB); // attained 5s -> slot 1, served by worker 1

    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qb, 5 * kSecondNs)).ok());
    q.wait_scheduler_settled_for_test();
    ASSERT_EQ(q.slot_of_query_for_test(qid(0xA)), 0);
    ASSERT_EQ(q.slot_of_query_for_test(qid(0xB)), 1);
    EXPECT_EQ(q.slot_of_worker_for_test(0), 0);
    EXPECT_EQ(q.slot_of_worker_for_test(1), 1);

    auto t1 = q.take(1);
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qb);

    auto t2 = q.take(0);
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qa);

    q.close();
}

// Fixed policy keeps the global fallback: a worker whose own slot is empty still helps
// instead of idling.
TEST_F(PushBasedTaskQueueTest, FixedPolicyFallsBackWhenItsSlotIsEmpty) {
    set_slots(2, "fixed");
    TestTaskQueue q(2);
    auto* qa = qkey(0xA);

    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    q.wait_scheduler_settled_for_test();
    ASSERT_EQ(q.slot_of_query_for_test(qid(0xA)), 0);

    auto t = q.take(1);
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->query_ctx_raw(), qa);

    q.close();
}

// Fixed policy spreads the workers evenly over the array: contiguous groups of equal
// size when the count divides, the remainder on the low (most-LAS) slots otherwise, and
// one worker per low slot when there are fewer workers than slots.
TEST_F(PushBasedTaskQueueTest, FixedPolicyDistributesWorkersEvenly) {
    {
        set_slots(4, "fixed");
        TestTaskQueue q(8);
        ASSERT_TRUE(q.push_back(make_task(qkey(0xA), 0)).ok());
        q.wait_scheduler_settled_for_test();
        ASSERT_EQ(q.slot_count_for_test(), 4);
        const std::array<int, 8> expected {0, 0, 1, 1, 2, 2, 3, 3};
        for (int worker = 0; worker < 8; ++worker) {
            EXPECT_EQ(q.slot_of_worker_for_test(worker), expected[worker]) << "worker " << worker;
        }
        q.close();
    }
    {
        set_slots(3, "fixed");
        TestTaskQueue q(4);
        ASSERT_TRUE(q.push_back(make_task(qkey(0xA), 0)).ok());
        q.wait_scheduler_settled_for_test();
        ASSERT_EQ(q.slot_count_for_test(), 3);
        const std::array<int, 4> expected {0, 0, 1, 2};
        for (int worker = 0; worker < 4; ++worker) {
            EXPECT_EQ(q.slot_of_worker_for_test(worker), expected[worker]) << "worker " << worker;
        }
        q.close();
    }
    {
        set_slots(4, "fixed");
        TestTaskQueue q(2);
        ASSERT_TRUE(q.push_back(make_task(qkey(0xA), 0)).ok());
        q.wait_scheduler_settled_for_test();
        ASSERT_EQ(q.slot_count_for_test(), 4);
        EXPECT_EQ(q.slot_of_worker_for_test(0), 0);
        EXPECT_EQ(q.slot_of_worker_for_test(1), 1);
        q.close();
    }
}

// The policy travels with the published array, so flipping the knob at runtime changes
// what a worker serves on its next take.
TEST_F(PushBasedTaskQueueTest, PolicyChangeTakesEffectAtRuntime) {
    set_slots(2, "ordered");
    TestTaskQueue q(2);
    auto* qa = qkey(0xA); // attained 0 -> slot 0
    auto* qb = qkey(0xB); // attained 5s -> slot 1

    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qb, 5 * kSecondNs)).ok());
    ASSERT_TRUE(q.push_back(make_task(qb, 5 * kSecondNs)).ok());
    q.wait_scheduler_settled_for_test();

    // Held, never released, so neither query's ranking or demand moves underneath us.
    auto ordered = q.take(1);
    ASSERT_NE(ordered, nullptr);
    EXPECT_EQ(ordered->query_ctx_raw(), qa);

    config::pipeline_las_slot_policy = "fixed";
    q.wait_scheduler_settled_for_test();
    ASSERT_EQ(q.slot_of_query_for_test(qid(0xA)), 0);
    ASSERT_EQ(q.slot_of_query_for_test(qid(0xB)), 1);

    auto fixed = q.take(1);
    ASSERT_NE(fixed, nullptr);
    EXPECT_EQ(fixed->query_ctx_raw(), qb);

    q.close();
}

// A worker keeps draining the lowest occupied slot across takes while that query has
// work, so a steady state keeps serving the least-attained query.
TEST_F(PushBasedTaskQueueTest, LowestSlotIsDrainedFirst) {
    TestTaskQueue q(2);
    auto* qa = qkey(0xA); // attained 0
    auto* qb = qkey(0xB); // attained 5s

    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    }
    ASSERT_TRUE(q.push_back(make_task(qb, 5 * kSecondNs)).ok());
    q.wait_scheduler_settled_for_test();

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

// Once settled, repeated scheduler passes with no state change leave the array exactly
// where it was and publish nothing, so the workers pay only the epoch load.
TEST_F(PushBasedTaskQueueTest, SteadyStateDoesNotRepublish) {
    TestTaskQueue q(2);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);

    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    }
    ASSERT_TRUE(q.push_back(make_task(qb, 5 * kSecondNs)).ok());
    q.wait_scheduler_settled_for_test();

    const uint64_t epoch = q.slot_epoch_for_test();
    for (int i = 0; i < 3; ++i) {
        q.wait_scheduler_settled_for_test();
        EXPECT_EQ(q.slot_of_query_for_test(qid(0xA)), 0) << "pass " << i;
        EXPECT_EQ(q.slot_of_query_for_test(qid(0xB)), 1) << "pass " << i;
        EXPECT_EQ(q.slot_epoch_for_test(), epoch) << "pass " << i;
    }

    q.close();
}

// A newly arrived lower-attained query takes slot 0 from the query sitting there, and
// the worker follows the array on its next take (preemption without any handshake).
TEST_F(PushBasedTaskQueueTest, PreemptionByHigherPriorityQuery) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA); // attained 5s
    auto* qc = qkey(0xC); // attained 0

    ASSERT_TRUE(q.push_back(make_task(qa, 5 * kSecondNs)).ok());
    ASSERT_TRUE(q.push_back(make_task(qa, 5 * kSecondNs)).ok());
    q.wait_scheduler_settled_for_test();

    auto t1 = q.take(0); // slot 0 holds A (only query present)
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qa);

    // Lower-attained query C arrives and displaces A to slot 1.
    ASSERT_TRUE(q.push_back(make_task(qc, 0)).ok());
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.slot_of_query_for_test(qid(0xC)), 0);
    EXPECT_EQ(q.slot_of_query_for_test(qid(0xA)), 1);

    auto t2 = q.take(0); // serves C despite A still having a runnable task
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qc);

    q.close();
}

// After a running query accumulates attained service, a fresh zero-attained query takes
// slot 0 on the next settled pass and the other one drops behind it.
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

    EXPECT_EQ(q.slot_of_query_for_test(qid(0xB)), 0);
    EXPECT_EQ(q.slot_of_query_for_test(qid(0xA)), 1);

    auto t2 = q.take(0);
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qb);

    // The higher-attained query is still drained, just after B.
    auto t3 = q.take(0);
    ASSERT_NE(t3, nullptr);
    EXPECT_EQ(t3->query_ctx_raw(), qa);

    q.close();
}

// Terminate drops the query from the array immediately, but reclaiming its state waits
// until nothing of it is pending or in flight.
TEST_F(PushBasedTaskQueueTest, TerminateDropsSlotAndReclaimsOnceDrained) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA);

    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    q.wait_scheduler_settled_for_test();
    ASSERT_EQ(q.slot_of_query_for_test(qid(0xA)), 0);

    auto t = q.take(0);
    ASSERT_NE(t, nullptr);

    // The task is still in flight, so the state survives the terminate.
    q.notify_query_terminated(qid(0xA));
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.slot_of_query_for_test(qid(0xA)), -1);
    EXPECT_EQ(q.registry_size_for_test(), 1);

    q.update_statistics(t.get(), 1000);
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

// A worker that cached the array while the query was ranked does not hold reclamation
// back, and its next take through the stale snapshot is safe: the state it still
// references is kept alive by that snapshot alone.
TEST_F(PushBasedTaskQueueTest, ReclaimWithCachedSnapshotIsSafe) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA);

    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    q.wait_scheduler_settled_for_test();

    // Worker 0 caches the array that lists A in slot 0.
    auto t = q.take(0);
    ASSERT_NE(t, nullptr);
    q.update_statistics(t.get(), 1000);

    q.notify_query_terminated(qid(0xA));
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.registry_size_for_test(), 0);

    EXPECT_EQ(q.take(0), nullptr);

    q.close();
}

// Work returning after a temporary drain keeps the same QueryState; an empty query is
// never reclaimed while it is alive.
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
    EXPECT_EQ(q.slot_of_query_for_test(qid(0xA)), 0);

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

// "Inelastic first": a single-task pipeline's task outranks a pre-existing backlog from
// another query, including whatever the slot array says. Even though slot 0 holds query
// A, the inelastic task from the higher-attained query B is served first.
TEST_F(PushBasedTaskQueueTest, InelasticFirstBeatsBacklog) {
    TestTaskQueue q(1);
    auto* qa = qkey(0xA); // elastic backlog
    auto* qb = qkey(0xB); // inelastic, higher attained — still served first

    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    }
    q.wait_scheduler_settled_for_test(); // A takes slot 0

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

// Inelastic tasks keep full per-query accounting: an empty query is not reclaimed,
// terminate after draining is. A later inelastic push recreates the state.
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

// Tasks with no QueryContext share one sentinel bucket, which is ranked like any other
// query. Emptiness does not reclaim it (no QUERY_TERMINATED will ever arrive); the state
// lives until queue close.
TEST_F(PushBasedTaskQueueTest, SentinelSurvivesIdle) {
    TestTaskQueue q(1);
    ASSERT_TRUE(q.push_back(make_task(nullptr, 0)).ok());
    q.wait_scheduler_settled_for_test();
    EXPECT_EQ(q.registry_size_for_test(), 1);
    EXPECT_EQ(q.slot_of_query_for_test(qid(0)), 0);

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

} // namespace doris
