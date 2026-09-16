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
#include <map>
#include <memory>
#include <set>
#include <vector>

#include "common/config.h"
#include "exec/pipeline/closed_slot_table.h"
#include "exec/pipeline/pipeline_task.h"

namespace doris {

// Tests for the closed-system pipeline task queue: the pool's workers are cut into
// `pipeline_closed_system_slots` equal groups, each group serves exactly one query, and a
// query that finds every slot taken waits its turn. Registry keys are TUniqueId values
// from query_id(); tests encode a fake QueryContext* into that id and never dereference it.
//
// The queue reads the slot count when it is constructed and on every admission event, so
// tests set the config before constructing the queue.

class MockPipelineTask : public PipelineTask {
public:
    explicit MockPipelineTask(QueryContext* key) : _key(key) {}

    QueryContext* query_ctx_raw() const override { return _key; }
    TUniqueId query_id() const override {
        TUniqueId id;
        id.lo = static_cast<int64_t>(reinterpret_cast<uintptr_t>(_key));
        return id;
    }

private:
    QueryContext* _key;
};

// Single dequeue attempt with no parking, so tests are deterministic and never sleep.
class TestTaskQueue final : public MultiCoreTaskQueue {
public:
    explicit TestTaskQueue(int core_size, Mode mode = Mode::CLOSED)
            : MultiCoreTaskQueue(core_size, mode) {}
    PipelineTaskSPtr take(int core_id) override { return _take(core_id, 0); }
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
PipelineTaskSPtr make_task(QueryContext* key) {
    return std::make_shared<MockPipelineTask>(key);
}
void push_tasks(TestTaskQueue& q, QueryContext* key, int count) {
    for (int i = 0; i < count; ++i) {
        ASSERT_TRUE(q.push_back(make_task(key)).ok());
    }
}
// Sets the slot count for the duration of a test and restores it afterwards.
struct ScopedSlotCount {
    explicit ScopedSlotCount(int32_t slots) : _previous(config::pipeline_closed_system_slots) {
        config::pipeline_closed_system_slots = slots;
    }
    ~ScopedSlotCount() { config::pipeline_closed_system_slots = _previous; }

private:
    const int32_t _previous;
};
} // namespace

// Two queries, eight workers, two slots: each query owns half the pool, and the halves are
// contiguous ranges of worker ids.
TEST(ClosedTaskQueueTest, PartitionsWorkersAcrossQueries) {
    ScopedSlotCount slots {2};
    TestTaskQueue q(8);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);

    push_tasks(q, qa, 1);
    push_tasks(q, qb, 1);

    EXPECT_EQ(q.slot_count_for_test(), 2);
    EXPECT_EQ(q.slot_of_query_for_test(qid(0xA)), 0);
    EXPECT_EQ(q.slot_of_query_for_test(qid(0xB)), 1);
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xA)), 4);
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xB)), 4);
    for (int worker = 0; worker < 4; ++worker) {
        EXPECT_EQ(q.slot_of_worker_for_test(worker), 0) << "worker " << worker;
    }
    for (int worker = 4; worker < 8; ++worker) {
        EXPECT_EQ(q.slot_of_worker_for_test(worker), 1) << "worker " << worker;
    }

    // A worker only ever gets tasks of the query its slot points at.
    auto from_a = q.take(0);
    ASSERT_NE(from_a, nullptr);
    EXPECT_EQ(from_a->query_ctx_raw(), qa);
    q.update_statistics(from_a.get(), 1000);

    auto from_b = q.take(7);
    ASSERT_NE(from_b, nullptr);
    EXPECT_EQ(from_b->query_ctx_raw(), qb);
    q.update_statistics(from_b.get(), 1000);

    q.close();
}

// The experiment this mode exists for: 32 workers over 1, 2, 4, ... queries, each query
// getting an equal share and never seeing another query's worker.
TEST(ClosedTaskQueueTest, PartitionMatrixOver32Workers) {
    for (int32_t slots : {1, 2, 4, 8, 16, 32}) {
        ScopedSlotCount scoped {slots};
        TestTaskQueue q(32);
        for (int32_t query = 1; query <= slots; ++query) {
            push_tasks(q, qkey(static_cast<uintptr_t>(query)), 2);
        }
        ASSERT_EQ(q.slot_count_for_test(), slots);
        for (int32_t query = 1; query <= slots; ++query) {
            EXPECT_EQ(q.assigned_workers_for_test(qid(static_cast<uintptr_t>(query))), 32 / slots)
                    << "slots " << slots << " query " << query;
        }

        // Drain twice over: no worker ever sees a second query.
        std::map<int, std::set<QueryContext*>> seen;
        for (int round = 0; round < 2; ++round) {
            for (int worker = 0; worker < 32; ++worker) {
                auto task = q.take(worker);
                if (task != nullptr) {
                    seen[worker].insert(task->query_ctx_raw());
                    q.update_statistics(task.get(), 1000);
                }
            }
        }
        for (const auto& [worker, queries] : seen) {
            EXPECT_EQ(queries.size(), 1U) << "slots " << slots << " worker " << worker;
        }
        q.close();
    }
}

// No stealing and no fallback queue: a worker whose query has nothing runnable idles even
// when another query has a deep backlog.
TEST(ClosedTaskQueueTest, WorkerNeverServesAnotherSlot) {
    ScopedSlotCount slots {2};
    TestTaskQueue q(8);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);

    push_tasks(q, qa, 1);
    push_tasks(q, qb, 1);
    auto drain_b = q.take(4);
    ASSERT_NE(drain_b, nullptr);
    q.update_statistics(drain_b.get(), 1000);

    push_tasks(q, qa, 16);
    for (int worker = 4; worker < 8; ++worker) {
        EXPECT_EQ(q.take(worker), nullptr) << "worker " << worker << " must not serve A";
    }
    auto from_a = q.take(1);
    ASSERT_NE(from_a, nullptr);
    EXPECT_EQ(from_a->query_ctx_raw(), qa);
    q.update_statistics(from_a.get(), 1000);

    q.close();
}

// A query that arrives when every slot is taken makes no progress at all until one frees,
// and slots are handed out in arrival order.
TEST(ClosedTaskQueueTest, AdmissionIsFifoAndWaitsForASlot) {
    ScopedSlotCount slots {2};
    TestTaskQueue q(8);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);
    auto* qc = qkey(0xC);

    push_tasks(q, qa, 2);
    push_tasks(q, qb, 2);
    push_tasks(q, qc, 2);

    EXPECT_EQ(q.slot_of_query_for_test(qid(0xC)), -1);
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xC)), 0);
    for (int worker = 0; worker < 8; ++worker) {
        auto task = q.take(worker);
        if (task != nullptr) {
            EXPECT_NE(task->query_ctx_raw(), qc) << "C holds no slot and must not run";
            q.update_statistics(task.get(), 1000);
        }
    }

    // A is gone: C inherits its slot, and therefore its workers.
    q.notify_query_terminated(qid(0xA));
    EXPECT_EQ(q.slot_of_query_for_test(qid(0xC)), 0);
    EXPECT_EQ(q.assigned_workers_for_test(qid(0xC)), 4);
    EXPECT_EQ(q.slot_of_query_for_test(qid(0xB)), 1);

    auto from_c = q.take(0);
    ASSERT_NE(from_c, nullptr);
    EXPECT_EQ(from_c->query_ctx_raw(), qc);
    q.update_statistics(from_c.get(), 1000);

    q.close();
}

// Retuning the slot count at runtime re-partitions the workers at the next admission
// event. Queries evicted by a shrink go back to the front of the waiting queue.
TEST(ClosedTaskQueueTest, SlotCountChangeRepartitions) {
    ScopedSlotCount slots {4};
    TestTaskQueue q(8);
    for (uintptr_t query = 1; query <= 4; ++query) {
        push_tasks(q, qkey(query), 1);
    }
    EXPECT_EQ(q.assigned_workers_for_test(qid(1)), 2);
    EXPECT_EQ(q.slot_of_query_for_test(qid(4)), 3);

    config::pipeline_closed_system_slots = 2;
    push_tasks(q, qkey(1), 1); // any push re-checks the configured slot count
    EXPECT_EQ(q.slot_count_for_test(), 2);
    EXPECT_EQ(q.assigned_workers_for_test(qid(1)), 4);
    EXPECT_EQ(q.slot_of_query_for_test(qid(3)), -1);
    EXPECT_EQ(q.slot_of_query_for_test(qid(4)), -1);
    for (int worker = 4; worker < 8; ++worker) {
        auto task = q.take(worker);
        if (task != nullptr) {
            EXPECT_EQ(task->query_ctx_raw(), qkey(2)) << "slot 1 serves query 2 only";
            q.update_statistics(task.get(), 1000);
        }
    }

    config::pipeline_closed_system_slots = 4;
    push_tasks(q, qkey(1), 1);
    EXPECT_EQ(q.slot_count_for_test(), 4);
    EXPECT_EQ(q.slot_of_query_for_test(qid(3)), 2);
    EXPECT_EQ(q.slot_of_query_for_test(qid(4)), 3);

    q.close();
}

// A terminated query is reclaimed once the pool has nothing of it left, not before, and a
// later push starts it over as a fresh query waiting for a slot.
TEST(ClosedTaskQueueTest, TerminateReclaimsWhenPoolIsDone) {
    ScopedSlotCount slots {1};
    TestTaskQueue q(4);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);

    push_tasks(q, qa, 1);
    EXPECT_EQ(q.registry_size_for_test(), 1);

    auto task = q.take(0);
    ASSERT_NE(task, nullptr);
    q.notify_query_terminated(qid(0xA));
    // Still in flight on this worker, so the state has to stay alive.
    EXPECT_EQ(q.registry_size_for_test(), 1);
    q.update_statistics(task.get(), 1000);

    // The next admission event reaps A and gives its slot to B.
    push_tasks(q, qb, 1);
    EXPECT_EQ(q.registry_size_for_test(), 1);
    EXPECT_EQ(q.slot_of_query_for_test(qid(0xB)), 0);

    // A push after reclaim recreates A, which now waits for a slot.
    push_tasks(q, qa, 1);
    EXPECT_EQ(q.registry_size_for_test(), 2);
    EXPECT_EQ(q.slot_of_query_for_test(qid(0xA)), -1);

    q.close();
}

// Tasks with no QueryContext (e.g. RevokableTask) share the all-zero id and are admitted
// like any other query rather than getting a private fast path.
TEST(ClosedTaskQueueTest, SentinelBucketIsAdmittedLikeAQuery) {
    ScopedSlotCount slots {1};
    TestTaskQueue q(4);
    auto* sentinel = qkey(0);

    push_tasks(q, sentinel, 1);
    EXPECT_EQ(q.slot_of_query_for_test(qid(0)), 0);
    EXPECT_EQ(q.assigned_workers_for_test(qid(0)), 4);

    auto task = q.take(2);
    ASSERT_NE(task, nullptr);
    EXPECT_EQ(task->query_ctx_raw(), sentinel);
    q.update_statistics(task.get(), 1000);

    q.close();
}

// release_task (the "task already running elsewhere" re-queue dance) releases the
// in-flight reference without charging runtime, and the re-queued task stays with its
// query rather than becoming visible to other slots.
TEST(ClosedTaskQueueTest, ReleaseAndRequeue) {
    ScopedSlotCount slots {2};
    TestTaskQueue q(8);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);

    push_tasks(q, qa, 1);
    push_tasks(q, qb, 1);
    auto drain_b = q.take(4);
    ASSERT_NE(drain_b, nullptr);
    q.update_statistics(drain_b.get(), 1000);

    auto task = q.take(0);
    ASSERT_NE(task, nullptr);
    q.release_task(task.get());
    ASSERT_TRUE(q.push_back(task, 0).ok());

    EXPECT_EQ(q.take(5), nullptr); // B's group still cannot see it
    auto again = q.take(1);
    ASSERT_NE(again, nullptr);
    EXPECT_EQ(again.get(), task.get());
    q.update_statistics(again.get(), 1000);

    q.close();
}

// Degenerate mode (blocking pool): one plain shared queue, no partition, no per-query
// state; produce/consume and accounting calls are safe.
TEST(ClosedTaskQueueTest, GeneralOnlyMode) {
    TestTaskQueue q(4, MultiCoreTaskQueue::Mode::GENERAL_ONLY);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);

    ASSERT_TRUE(q.push_back(make_task(qa)).ok());
    ASSERT_TRUE(q.push_back(make_task(qb)).ok());
    EXPECT_EQ(q.registry_size_for_test(), 0);

    // Any worker may run anything here.
    auto t1 = q.take(3);
    ASSERT_NE(t1, nullptr);
    q.update_statistics(t1.get(), 1000);
    auto t2 = q.take(3);
    ASSERT_NE(t2, nullptr);
    q.release_task(t2.get());
    EXPECT_EQ(q.take(0), nullptr);

    q.notify_query_terminated(qid(0xA)); // no-op in this mode

    q.close();
}

TEST(ClosedTaskQueueTest, CloseRejectsWork) {
    ScopedSlotCount slots {1};
    TestTaskQueue q(4);
    auto* qa = qkey(0xA);

    push_tasks(q, qa, 1);
    q.close();
    EXPECT_FALSE(q.push_back(make_task(qa)).ok());
    EXPECT_EQ(q.take(0), nullptr);
}

// The partition policy on its own, for the cases that are awkward to reach through the
// queue: uneven worker counts, clamping, and the order evicted queries come back in.
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
