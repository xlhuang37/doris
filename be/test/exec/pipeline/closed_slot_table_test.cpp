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

#include "exec/pipeline/closed_slot_table.h"

#include <gtest/gtest.h>

#include <vector>

namespace doris {

TEST(ClosedSlotTableTest, SplitsWorkersEvenlyWhenDivisible) {
    ClosedSlotTable<int> table(32);
    std::vector<int> changed;
    for (int query = 1; query <= 4; ++query) {
        table.add(query);
    }
    table.rebind(4, &changed);
    ASSERT_EQ(table.slot_count(), 4);
    for (int slot = 0; slot < 4; ++slot) {
        EXPECT_EQ(table.workers_of_slot(slot), 8);
    }
    EXPECT_EQ(table.slot_of_worker(0), 0);
    EXPECT_EQ(table.slot_of_worker(8), 1);
    EXPECT_EQ(table.slot_of_worker(31), 3);
    EXPECT_EQ(table.slot_of_worker(32), -1);
    EXPECT_EQ(table.slot_of_worker(-1), -1);
}

TEST(ClosedSlotTableTest, SpreadsTheRemainderWhenNotDivisible) {
    ClosedSlotTable<int> table(10);
    std::vector<int> changed;
    for (int query = 1; query <= 3; ++query) {
        table.add(query);
    }
    table.rebind(3, &changed);
    int total = 0;
    for (int slot = 0; slot < 3; ++slot) {
        const int workers = table.workers_of_slot(slot);
        EXPECT_GE(workers, 3);
        EXPECT_LE(workers, 4);
        total += workers;
    }
    EXPECT_EQ(total, 10); // every worker still belongs to exactly one slot
}

TEST(ClosedSlotTableTest, AdmitsInArrivalOrderAndIsIdempotent) {
    ClosedSlotTable<int> table(8);
    std::vector<int> changed;
    for (int query = 1; query <= 4; ++query) {
        table.add(query);
        table.add(query); // repeated pushes must not re-queue a query
    }
    table.rebind(2, &changed);
    EXPECT_EQ(table.slot_of(1), 0);
    EXPECT_EQ(table.slot_of(2), 1);
    EXPECT_EQ(table.slot_of(3), -1);
    EXPECT_EQ(table.waiting_size(), 2U);

    changed.clear();
    EXPECT_EQ(table.remove(1), 0);
    table.rebind(2, &changed);
    EXPECT_EQ(changed, std::vector<int> {0});
    EXPECT_EQ(table.slot_of(3), 0);

    changed.clear();
    EXPECT_EQ(table.remove(4), -1); // still waiting, so it vacated no slot
    table.rebind(2, &changed);
    EXPECT_TRUE(changed.empty());
    EXPECT_EQ(table.waiting_size(), 0U);
}

TEST(ClosedSlotTableTest, ShrinkReturnsOwnersToTheFrontOfTheQueue) {
    ClosedSlotTable<int> table(8);
    std::vector<int> changed;
    for (int query = 1; query <= 4; ++query) {
        table.add(query);
    }
    table.rebind(4, &changed);

    changed.clear();
    table.rebind(2, &changed);
    EXPECT_EQ(table.slot_count(), 2);
    EXPECT_EQ(table.waiting_size(), 2U);
    EXPECT_EQ(changed.size(), 2U);
    EXPECT_EQ(table.workers_of_slot(0), 4);

    changed.clear();
    table.rebind(4, &changed);
    EXPECT_EQ(table.slot_of(3), 2); // evicted queries return ahead of newer arrivals
    EXPECT_EQ(table.slot_of(4), 3);
    EXPECT_EQ(table.waiting_size(), 0U);
}

TEST(ClosedSlotTableTest, ClampsTheRequestedSlotCount) {
    ClosedSlotTable<int> table(8);
    std::vector<int> changed;
    table.add(1);
    table.rebind(0, &changed);
    EXPECT_EQ(table.slot_count(), 1);
    table.rebind(-5, &changed);
    EXPECT_EQ(table.slot_count(), 1);
    table.rebind(1000, &changed);
    EXPECT_EQ(table.slot_count(), 8);
    EXPECT_FALSE(table.owner_of(-1).has_value());
    EXPECT_FALSE(table.owner_of(99).has_value());
}

} // namespace doris
