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

#include "exec/scan/task_executor/time_sharing/multilevel_split_queue.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include "exec/scan/task_executor/split_runner.h"
#include "exec/scan/task_executor/task_id.h"
#include "exec/scan/task_executor/ticker.h"
#include "exec/scan/task_executor/time_sharing/prioritized_split_runner.h"
#include "exec/scan/task_executor/time_sharing/time_sharing_task_handle.h"

namespace doris {

// Scan-side fragment-runtime absolute-priority MLFQ tests. The level thresholds are
// MultilevelSplitQueue::LEVEL_THRESHOLD_SECONDS = {0, 1, 10, 60, 300}, so a fragment
// runtime maps to: <1s -> L0, [1s,10s) -> L1, [10s,60s) -> L2, [60s,300s) -> L3,
// >=300s -> L4.
static constexpr uint64_t kSecondNs = 1'000'000'000ULL;

// Minimal SplitRunner: the queue tests only exercise offer/take/priority, never
// process(), so the runner body is a no-op.
class NoopSplitRunner : public SplitRunner {
public:
    Status init() override { return Status::OK(); }
    Result<SharedListenableFuture<Void>> process_for(std::chrono::nanoseconds) override {
        return SharedListenableFuture<Void>::create_ready();
    }
    void close(const Status&) override {}
    bool is_finished() override { return false; }
    Status finished_status() override { return Status::OK(); }
    std::string get_info() const override { return ""; }
    bool is_auto_reschedule() const override { return false; }
};

class MultilevelSplitQueueTest : public testing::Test {
protected:
    std::shared_ptr<MultilevelSplitQueue> queue = std::make_shared<MultilevelSplitQueue>(2);
    std::shared_ptr<Ticker> ticker = std::make_shared<SystemTicker>();
    int _next_id = 0;

    std::shared_ptr<TimeSharingTaskHandle> make_handle(const std::string& id,
                                                       std::atomic<uint64_t>* frag) {
        return std::make_shared<TimeSharingTaskHandle>(
                TaskId(id), queue, []() { return 0.0; }, 1, std::chrono::seconds(1), std::nullopt,
                frag);
    }

    // The PrioritizedSplitRunner constructor calls update_level_priority(), which reads
    // the handle's fragment-runtime-derived level, so callers must set the fragment
    // counter before building the split to get the intended initial level.
    std::shared_ptr<PrioritizedSplitRunner> make_split(
            const std::shared_ptr<TimeSharingTaskHandle>& handle) {
        auto runner = std::make_shared<NoopSplitRunner>();
        return std::make_shared<PrioritizedSplitRunner>(handle, _next_id++, runner, ticker);
    }
};

// The split's level is derived from its fragment's cumulative runtime.
TEST_F(MultilevelSplitQueueTest, LevelDerivedFromFragmentRuntime) {
    std::atomic<uint64_t> frag_l0 {0};
    std::atomic<uint64_t> frag_l1 {5 * kSecondNs};
    std::atomic<uint64_t> frag_l3 {120 * kSecondNs};

    EXPECT_EQ(make_handle("l0", &frag_l0)->priority().level(), 0);
    EXPECT_EQ(make_handle("l1", &frag_l1)->priority().level(), 1);
    EXPECT_EQ(make_handle("l3", &frag_l3)->priority().level(), 3);
}

// Workers drain the lowest non-empty level fully before any deeper one, regardless of
// offer order.
TEST_F(MultilevelSplitQueueTest, AbsolutePriorityAcrossLevels) {
    std::atomic<uint64_t> frag_shallow {0};            // L0
    std::atomic<uint64_t> frag_mid {5 * kSecondNs};    // L1
    std::atomic<uint64_t> frag_deep {120 * kSecondNs}; // L3

    auto shallow = make_split(make_handle("shallow", &frag_shallow));
    auto mid = make_split(make_handle("mid", &frag_mid));
    auto deep = make_split(make_handle("deep", &frag_deep));

    queue->offer(deep);
    queue->offer(mid);
    queue->offer(shallow);

    EXPECT_EQ(queue->take().get(), shallow.get());
    EXPECT_EQ(queue->take().get(), mid.get());
    EXPECT_EQ(queue->take().get(), deep.get());
    EXPECT_EQ(queue->take(), nullptr);
}

// Within a single level, the split whose fragment has consumed less CPU is served
// first (SplitRunnerComparator orders by level priority == fragment runtime).
TEST_F(MultilevelSplitQueueTest, WithinLevelOrderedByFragmentRuntime) {
    std::atomic<uint64_t> frag_light {1'000'000};  // 1ms -> L0
    std::atomic<uint64_t> frag_heavy {2'000'000};  // 2ms -> L0

    auto light = make_split(make_handle("light", &frag_light));
    auto heavy = make_split(make_handle("heavy", &frag_heavy));

    queue->offer(heavy);
    queue->offer(light);

    EXPECT_EQ(queue->take().get(), light.get());
    EXPECT_EQ(queue->take().get(), heavy.get());
}

// A split enqueued at a high-priority level whose fragment subsequently consumes a lot
// of CPU (on any core) is lazily demoted to the correct deeper level at take() time,
// instead of running at its stale level.
TEST_F(MultilevelSplitQueueTest, LazyDemotionWhenFragmentGrows) {
    std::atomic<uint64_t> frag_boosted {0};         // starts at L0
    std::atomic<uint64_t> frag_mid {5 * kSecondNs}; // L1

    auto boosted = make_split(make_handle("boosted", &frag_boosted));
    auto mid = make_split(make_handle("mid", &frag_mid));

    queue->offer(boosted); // enters L0
    queue->offer(mid);     // enters L1

    // The boosted split's fragment burns a lot of CPU after it was enqueued.
    frag_boosted.store(120 * kSecondNs); // now belongs in L3

    // Even though `boosted` physically sits in L0, lazy re-leveling defers it and `mid`
    // (L1) is served first.
    EXPECT_EQ(queue->take().get(), mid.get());
    EXPECT_EQ(queue->take().get(), boosted.get());
    EXPECT_EQ(queue->take(), nullptr);
}

} // namespace doris
