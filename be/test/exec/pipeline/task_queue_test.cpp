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

#include <atomic>
#include <memory>

#include "exec/pipeline/pipeline_task.h"

namespace doris {

// Level 0 is the inelastic-first level. Runtime levels (for elastic fragments)
// are 1..6, derived from the fragment runtime (ns):
//   <=1s -> L1, <=3s -> L2, <=10s -> L3, <=60s -> L4, <=300s -> L5, else -> L6
static constexpr uint64_t kSecondNs = 1'000'000'000ULL;

// Lightweight PipelineTask whose fragment runtime and inelastic signal can be set
// directly, so the fragment-granular MLFQ can be exercised without building a full
// operator tree. `fragment_runtime_ns()` and `fragment_is_inelastic()` are virtual
// only under BE_TEST (via MOCK_FUNCTION).
class MockPipelineTask : public PipelineTask {
public:
    MockPipelineTask() : PipelineTask() {}
    ~MockPipelineTask() override = default;

    void set_fragment_runtime_ns(uint64_t value) { _mock_runtime.store(value); }
    uint64_t fragment_runtime_ns() const override { return _mock_runtime.load(); }

    void set_inelastic(bool value) { _mock_inelastic.store(value); }
    bool fragment_is_inelastic() const override { return _mock_inelastic.load(); }

private:
    std::atomic<uint64_t> _mock_runtime {0};
    std::atomic<bool> _mock_inelastic {false};
};

static std::shared_ptr<MockPipelineTask> make_task(uint64_t fragment_runtime_ns) {
    auto task = std::make_shared<MockPipelineTask>();
    task->set_fragment_runtime_ns(fragment_runtime_ns);
    return task;
}

class TaskQueueTest : public testing::Test {};

// Workers always drain the lowest non-empty level before a deeper one, regardless
// of push order.
TEST_F(TaskQueueTest, AbsolutePriorityAcrossLevels) {
    PriorityTaskQueue q;
    auto deep = make_task(120 * kSecondNs); // level 3
    auto mid = make_task(5 * kSecondNs);    // level 2
    auto shallow = make_task(0);            // level 0

    ASSERT_TRUE(q.push(deep).ok());
    ASSERT_TRUE(q.push(mid).ok());
    ASSERT_TRUE(q.push(shallow).ok());

    EXPECT_EQ(q.try_take(false).get(), shallow.get());
    EXPECT_EQ(q.try_take(false).get(), mid.get());
    EXPECT_EQ(q.try_take(false).get(), deep.get());
    EXPECT_EQ(q.try_take(false), nullptr);
}

// Within a single level ordering is FIFO.
TEST_F(TaskQueueTest, FifoWithinLevel) {
    PriorityTaskQueue q;
    auto a = make_task(0);
    auto b = make_task(0);
    auto c = make_task(0);

    ASSERT_TRUE(q.push(a).ok());
    ASSERT_TRUE(q.push(b).ok());
    ASSERT_TRUE(q.push(c).ok());

    EXPECT_EQ(q.try_take(false).get(), a.get());
    EXPECT_EQ(q.try_take(false).get(), b.get());
    EXPECT_EQ(q.try_take(false).get(), c.get());
}

// The level is derived from the fragment counter, so a fragment that has burned a
// lot of CPU lands deep even though the task object itself is new.
TEST_F(TaskQueueTest, LevelDrivenByFragmentRuntime) {
    PriorityTaskQueue q;
    auto a = make_task(0);              // level 0
    auto b = make_task(600 * kSecondNs); // level 5

    ASSERT_TRUE(q.push(b).ok());
    ASSERT_TRUE(q.push(a).ok());

    // `a` is served first because its fragment is lighter, even though `b` was
    // pushed first.
    EXPECT_EQ(q.try_take(false).get(), a.get());
    EXPECT_EQ(q.try_take(false).get(), b.get());
}

// A task enqueued at a high-priority level whose fragment subsequently crosses a
// threshold is deferred to the correct deeper level at dequeue time, instead of
// being run at its stale level.
TEST_F(TaskQueueTest, LazyDemotionAtDequeue) {
    PriorityTaskQueue q;
    auto heavy = make_task(0);          // enqueued at level 0
    auto light = make_task(2 * kSecondNs); // level 1

    ASSERT_TRUE(q.push(heavy).ok());
    ASSERT_TRUE(q.push(light).ok());

    // The fragment of `heavy` consumed a lot of CPU (on some other core) after the
    // task was enqueued; it should now belong in level 5.
    heavy->set_fragment_runtime_ns(600 * kSecondNs);

    // Even though `heavy` physically sits in level 0, lazy demotion defers it and
    // `light` (level 1) is served first.
    EXPECT_EQ(q.try_take(false).get(), light.get());
    EXPECT_EQ(q.try_take(false).get(), heavy.get());
    EXPECT_EQ(q.try_take(false), nullptr);
}

// A worker with an empty local queue steals the victim's highest-priority task.
TEST_F(TaskQueueTest, StealReturnsHighestPriorityTask) {
    MultiCoreTaskQueue mc(2);
    auto deep = make_task(120 * kSecondNs); // level 3
    auto shallow = make_task(0);            // level 0

    // Pin both tasks to core 0.
    ASSERT_TRUE(mc.push_back(deep, 0).ok());
    ASSERT_TRUE(mc.push_back(shallow, 0).ok());

    // Worker on core 1 steals from core 0 and must get the lowest-level task first.
    EXPECT_EQ(mc.take(1).get(), shallow.get());
    EXPECT_EQ(mc.take(1).get(), deep.get());

    mc.close();
}

// An inelastic fragment's task is served before every elastic task, even ones with
// lighter runtime that were pushed first.
TEST_F(TaskQueueTest, InelasticFirstBeatsAllRuntimeLevels) {
    PriorityTaskQueue q;
    auto light = make_task(0);              // elastic, runtime level 1
    auto inelastic = make_task(600 * kSecondNs); // heavy, but inelastic -> level 0
    inelastic->set_inelastic(true);

    ASSERT_TRUE(q.push(light).ok());
    ASSERT_TRUE(q.push(inelastic).ok());

    EXPECT_EQ(q.try_take(false).get(), inelastic.get());
    EXPECT_EQ(q.try_take(false).get(), light.get());
    EXPECT_EQ(q.try_take(false), nullptr);
}

// Multiple inelastic tasks share level 0 and are served FIFO among themselves,
// ahead of any elastic task.
TEST_F(TaskQueueTest, InelasticTasksFifoWithinTopLevel) {
    PriorityTaskQueue q;
    auto elastic = make_task(0);
    auto in1 = make_task(0);
    auto in2 = make_task(0);
    in1->set_inelastic(true);
    in2->set_inelastic(true);

    ASSERT_TRUE(q.push(elastic).ok());
    ASSERT_TRUE(q.push(in1).ok());
    ASSERT_TRUE(q.push(in2).ok());

    EXPECT_EQ(q.try_take(false).get(), in1.get());
    EXPECT_EQ(q.try_take(false).get(), in2.get());
    EXPECT_EQ(q.try_take(false).get(), elastic.get());
}

// A task enqueued at level 0 while its fragment was inelastic is lazily moved to
// its runtime level at dequeue once the fragment becomes elastic again.
TEST_F(TaskQueueTest, LazyDeboostWhenFragmentBecomesElastic) {
    PriorityTaskQueue q;
    auto boosted = make_task(600 * kSecondNs); // heavy runtime -> level 6 when elastic
    boosted->set_inelastic(true);
    auto mid = make_task(5 * kSecondNs); // elastic, runtime level 3

    ASSERT_TRUE(q.push(boosted).ok()); // enters level 0
    ASSERT_TRUE(q.push(mid).ok());     // enters level 3

    // The fragment gains parallelism and is no longer inelastic before dequeue.
    boosted->set_inelastic(false);

    // `boosted` now belongs in its heavy runtime level (6), so `mid` is served first
    // and `boosted` is deferred and served last.
    EXPECT_EQ(q.try_take(false).get(), mid.get());
    EXPECT_EQ(q.try_take(false).get(), boosted.get());
    EXPECT_EQ(q.try_take(false), nullptr);
}

} // namespace doris
