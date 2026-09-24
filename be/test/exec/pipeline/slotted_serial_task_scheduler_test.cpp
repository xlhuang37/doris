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

#include "exec/pipeline/slotted_serial_task_scheduler.h"

#include <gtest/gtest.h>

#include <vector>

namespace doris {

namespace {

TUniqueId make_qid(int64_t lo) {
    TUniqueId id;
    id.hi = 0;
    id.lo = lo;
    return id;
}

SerialFragmentInfo make_fragment(int64_t query_lo, int64_t arrival_ns, int fragment_id) {
    SerialFragmentInfo info;
    info.query_id = make_qid(query_lo);
    info.arrival_ns = arrival_ns;
    info.fragment_id = fragment_id;
    info.pipelines = {{0, false, false}};
    return info;
}

using Router = BasicSlotRouter<int>;

} // namespace

TEST(SlotRouterTest, SplitsWorkersEvenlyWhenDivisible) {
    Router router(32, 4);
    ASSERT_EQ(router.slot_count(), 4);
    for (int slot = 0; slot < 4; ++slot) {
        EXPECT_EQ(router.workers_of_slot(slot), 8);
    }
    EXPECT_EQ(router.slot_of_worker(0), 0);
    EXPECT_EQ(router.slot_of_worker(7), 0);
    EXPECT_EQ(router.slot_of_worker(8), 1);
    EXPECT_EQ(router.slot_of_worker(31), 3);
    EXPECT_EQ(router.slot_of_worker(32), -1);
    EXPECT_EQ(router.slot_of_worker(-1), -1);
}

TEST(SlotRouterTest, SpreadsTheRemainderAndClamps) {
    Router uneven(10, 3);
    int total = 0;
    for (int slot = 0; slot < 3; ++slot) {
        EXPECT_GE(uneven.workers_of_slot(slot), 3);
        EXPECT_LE(uneven.workers_of_slot(slot), 4);
        total += uneven.workers_of_slot(slot);
    }
    EXPECT_EQ(total, 10);
    EXPECT_EQ(Router(8, 0).slot_count(), 1);
    EXPECT_EQ(Router(8, 100).slot_count(), 8);
}

TEST(SlotRouterTest, QueriesWaitUntilAdmitted) {
    Router router(8, 2);
    EXPECT_EQ(router.on_register(make_fragment(1, 100, 0)), -1);
    EXPECT_EQ(router.on_push(make_qid(1), 11), -1);
    EXPECT_EQ(router.slot_of_query(make_qid(1)), -1);
    EXPECT_EQ(router.waiting_size(), 1U);
    EXPECT_TRUE(router.can_admit());

    auto admission = router.admit(router.free_slot());
    ASSERT_TRUE(admission.has_value());
    EXPECT_EQ(admission->query_id.lo, 1);
    EXPECT_EQ(router.slot_of_query(make_qid(1)), 0);
    EXPECT_EQ(router.waiting_size(), 0U);
    // Once bound, registrations and pushes go straight to the slot.
    EXPECT_EQ(router.on_register(make_fragment(1, 100, 1)), 0);
    EXPECT_EQ(router.on_push(make_qid(1), 12), 0);
}

TEST(SlotRouterTest, AdmitsByArrivalOnlyIntoFreeSlots) {
    Router router(8, 2);
    // Registered in the order 3, 1, 2, but query 1 arrived first and query 2 second.
    router.on_register(make_fragment(3, 300, 0));
    router.on_register(make_fragment(1, 100, 0));
    router.on_register(make_fragment(2, 200, 0));

    auto first = router.admit(router.free_slot());
    auto second = router.admit(router.free_slot());
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(first->query_id.lo, 1);
    EXPECT_EQ(second->query_id.lo, 2);
    EXPECT_EQ(router.slot_of_query(make_qid(1)), 0);
    EXPECT_EQ(router.slot_of_query(make_qid(2)), 1);

    EXPECT_EQ(router.free_slot(), -1);
    EXPECT_FALSE(router.can_admit());
    EXPECT_FALSE(router.admit(0).has_value());
    EXPECT_EQ(router.slot_of_query(make_qid(3)), -1);

    EXPECT_EQ(router.on_finish(make_qid(2)), 1);
    EXPECT_EQ(router.free_slot(), 1);
    auto third = router.admit(router.free_slot());
    ASSERT_TRUE(third.has_value());
    EXPECT_EQ(third->query_id.lo, 3);
    EXPECT_EQ(router.slot_of_query(make_qid(3)), 1);
    EXPECT_EQ(router.slot_of_query(make_qid(1)), 0);
}

TEST(SlotRouterTest, EqualArrivalTimesAdmitInQueryIdOrder) {
    Router router(4, 1);
    router.on_register(make_fragment(2, 100, 0));
    router.on_register(make_fragment(1, 100, 0));
    auto admission = router.admit(0);
    ASSERT_TRUE(admission.has_value());
    EXPECT_EQ(admission->query_id.lo, 1);
}

TEST(SlotRouterTest, AdmissionReplaysBufferedWorkInOrder) {
    Router router(4, 1);
    router.on_register(make_fragment(1, 100, 2));
    router.on_push(make_qid(1), 21);
    router.on_register(make_fragment(1, 100, 0));
    router.on_push(make_qid(1), 22);
    router.on_push(make_qid(1), 23);

    auto admission = router.admit(0);
    ASSERT_TRUE(admission.has_value());
    ASSERT_EQ(admission->fragments.size(), 2U);
    EXPECT_EQ(admission->fragments[0].fragment_id, 2);
    EXPECT_EQ(admission->fragments[1].fragment_id, 0);
    EXPECT_EQ(admission->tasks, (std::vector<int> {21, 22, 23}));
}

TEST(SlotRouterTest, FinishingAWaitingQueryDropsItsBufferedWork) {
    Router router(4, 1);
    router.on_register(make_fragment(1, 100, 0));
    router.on_register(make_fragment(2, 200, 0));
    router.on_push(make_qid(2), 7);
    ASSERT_TRUE(router.admit(0).has_value());

    EXPECT_EQ(router.on_finish(make_qid(2)), -1);
    EXPECT_EQ(router.waiting_size(), 0U);
    EXPECT_EQ(router.on_finish(make_qid(1)), 0);
    EXPECT_FALSE(router.can_admit());
    EXPECT_EQ(router.free_slot(), 0);
}

TEST(SlotRouterTest, UnknownAndFinishedQueriesNeverTakeASlot) {
    Router router(4, 2);
    // A push for a query that never registered parks in slot 0 unscheduled.
    EXPECT_EQ(router.on_push(make_qid(9), 1), 0);
    EXPECT_EQ(router.waiting_size(), 0U);

    router.on_register(make_fragment(1, 100, 0));
    ASSERT_TRUE(router.admit(0).has_value());
    EXPECT_EQ(router.on_finish(make_qid(1)), 0);
    // Late work of a finished query is neither re-queued nor registered anywhere.
    EXPECT_EQ(router.on_register(make_fragment(1, 100, 1)), -1);
    EXPECT_EQ(router.waiting_size(), 0U);
    EXPECT_EQ(router.on_push(make_qid(1), 2), 0);
    EXPECT_FALSE(router.can_admit());
    // Finishing a query that was never seen changes nothing.
    EXPECT_EQ(router.on_finish(make_qid(42)), -1);
    EXPECT_EQ(router.free_slot(), 0);
}

} // namespace doris
