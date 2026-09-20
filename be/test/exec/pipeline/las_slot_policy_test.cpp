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

#include "exec/pipeline/las_slot_policy.h"

#include <gtest/gtest.h>

#include <vector>

namespace doris {

// The names the `pipeline_las_slot_policy` config accepts, and nothing else.
TEST(LasSlotPolicyTest, PolicyNames) {
    EXPECT_TRUE(is_las_slot_policy_name("ordered"));
    EXPECT_TRUE(is_las_slot_policy_name("fixed"));
    EXPECT_FALSE(is_las_slot_policy_name(""));
    EXPECT_FALSE(is_las_slot_policy_name("Ordered"));
    EXPECT_FALSE(is_las_slot_policy_name("round_robin"));

    EXPECT_EQ(parse_las_slot_policy("ordered"), LasSlotPolicy::ORDERED);
    EXPECT_EQ(parse_las_slot_policy("fixed"), LasSlotPolicy::FIXED);
    // Anything the validator would have rejected still has to yield a usable policy.
    EXPECT_EQ(parse_las_slot_policy("nonsense"), LasSlotPolicy::ORDERED);
}

// The array always has at least one slot, so the most-LAS query is staffed whatever the
// knob says.
TEST(LasSlotPolicyTest, SlotCountIsClamped) {
    EXPECT_EQ(clamp_las_slot_count(8), 8);
    EXPECT_EQ(clamp_las_slot_count(1), 1);
    EXPECT_EQ(clamp_las_slot_count(0), 1);
    EXPECT_EQ(clamp_las_slot_count(-7), 1);
}

// Contiguous groups of equal size when the worker count divides the slot count.
TEST(LasSlotPolicyTest, WorkersSplitEvenlyWhenDivisible) {
    const std::vector<int> expected {0, 0, 1, 1, 2, 2, 3, 3};
    for (int worker = 0; worker < 8; ++worker) {
        EXPECT_EQ(las_slot_of_worker(worker, /*slot_count=*/4, /*worker_count=*/8),
                  expected[static_cast<size_t>(worker)])
                << "worker " << worker;
    }

    // One worker per slot is the exact case of the same rule.
    for (int worker = 0; worker < 4; ++worker) {
        EXPECT_EQ(las_slot_of_worker(worker, 4, 4), worker) << "worker " << worker;
    }
}

// The remainder lands on the low slots, i.e. the least-attained queries get the extra
// worker rather than the tail of the array.
TEST(LasSlotPolicyTest, RemainderGoesToLowSlots) {
    const std::vector<int> expected {0, 0, 1, 2};
    for (int worker = 0; worker < 4; ++worker) {
        EXPECT_EQ(las_slot_of_worker(worker, /*slot_count=*/3, /*worker_count=*/4),
                  expected[static_cast<size_t>(worker)])
                << "worker " << worker;
    }

    // Every slot keeps at least one worker as long as there are enough workers.
    const std::vector<int> five_over_two {0, 0, 0, 1, 1};
    for (int worker = 0; worker < 5; ++worker) {
        EXPECT_EQ(las_slot_of_worker(worker, 2, 5), five_over_two[static_cast<size_t>(worker)])
                << "worker " << worker;
    }
}

// Fewer workers than slots: the low slots are served one worker each and the tail goes
// unserved, instead of striding over the array and skipping slot 1.
TEST(LasSlotPolicyTest, FewerWorkersThanSlotsServeTheLowSlots) {
    for (int worker = 0; worker < 3; ++worker) {
        EXPECT_EQ(las_slot_of_worker(worker, /*slot_count=*/8, /*worker_count=*/3), worker)
                << "worker " << worker;
    }
}

// Out-of-range asks have no slot rather than a wrapped one.
TEST(LasSlotPolicyTest, OutOfRangeHasNoSlot) {
    EXPECT_EQ(las_slot_of_worker(-1, 4, 8), -1);
    EXPECT_EQ(las_slot_of_worker(8, 4, 8), -1);
    EXPECT_EQ(las_slot_of_worker(0, 0, 8), -1);
    EXPECT_EQ(las_slot_of_worker(0, 4, 0), -1);
}

} // namespace doris
