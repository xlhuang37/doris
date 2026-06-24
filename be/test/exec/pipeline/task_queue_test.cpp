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

#include <cstdint>
#include <memory>

#include <map>

#include "common/config.h"
#include "exec/pipeline/pipeline_task.h"
#include "runtime/query_cpu_lease.h"
#include "runtime/workload_group/cpu_lease_grantor.h"

namespace doris {

// Tests for the global, query-granular absolute-priority pipeline MLFQ
// (MultiCoreTaskQueue). Level thresholds are {1s,3s,10s,60s,300s} of query runtime,
// so: <=1s -> L0, (1s,3s] -> L1, ... A query's level is read from its tasks'
// query_runtime_ns(); the bucket key is query_ctx_raw() (compared, never dereferenced,
// so opaque fake pointers are fine here).
static constexpr uint64_t kSecondNs = 1'000'000'000ULL;

class MockPipelineTask : public PipelineTask {
public:
    MockPipelineTask(QueryContext* key, uint64_t runtime_ns, QueryCpuLease* lease = nullptr)
            : _key(key), _runtime_ns(runtime_ns), _lease(lease) {}

    uint64_t query_runtime_ns() const override { return _runtime_ns; }
    QueryContext* query_ctx_raw() const override { return _key; }
    QueryCpuLease* cpu_lease() const override { return _lease; }

    void set_runtime_ns(uint64_t runtime_ns) { _runtime_ns = runtime_ns; }

private:
    QueryContext* _key;
    uint64_t _runtime_ns;
    QueryCpuLease* _lease;
};

namespace {
QueryContext* qkey(uintptr_t id) {
    return reinterpret_cast<QueryContext*>(id);
}
PipelineTaskSPtr make_task(QueryContext* key, uint64_t runtime_ns) {
    return std::make_shared<MockPipelineTask>(key, runtime_ns);
}
} // namespace

// A lower-runtime query's tasks are served before a higher-runtime query's, globally,
// regardless of push order or which worker asks (the H1 per-core-sharding regression).
TEST(GlobalPipelineMLFQTest, AbsolutePriorityAcrossQueries) {
    MultiCoreTaskQueue q(1);
    auto* qa = qkey(0xA); // runtime 0 -> L0
    auto* qb = qkey(0xB); // runtime 5s -> L1

    // Push the lower-priority query first.
    ASSERT_TRUE(q.push_back(make_task(qb, 5 * kSecondNs)).ok());
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());

    auto t1 = q.take(0);
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qa);

    auto t2 = q.take(0);
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qb);

    q.close();
}

// Within one level, distinct workers are spread across co-resident queries
// round-robin rather than dogpiling one query.
TEST(GlobalPipelineMLFQTest, WithinLevelRoundRobinSpread) {
    MultiCoreTaskQueue q(3);
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);
    auto* qc = qkey(0xC);

    // All at L0, two tasks each; insertion order A, B, C.
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qb, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qb, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qc, 0)).ok());
    ASSERT_TRUE(q.push_back(make_task(qc, 0)).ok());

    // Three different workers each take once: should land on three different queries.
    auto t0 = q.take(0);
    auto t1 = q.take(1);
    auto t2 = q.take(2);
    ASSERT_NE(t0, nullptr);
    ASSERT_NE(t1, nullptr);
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t0->query_ctx_raw(), qa);
    EXPECT_EQ(t1->query_ctx_raw(), qb);
    EXPECT_EQ(t2->query_ctx_raw(), qc);

    q.close();
}

// A worker keeps serving the same query across takes while that query remains the
// highest priority (locality).
TEST(GlobalPipelineMLFQTest, QueryLocality) {
    MultiCoreTaskQueue q(2);
    auto* qa = qkey(0xA); // L0
    auto* qb = qkey(0xB); // L1

    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    }
    ASSERT_TRUE(q.push_back(make_task(qb, 5 * kSecondNs)).ok());

    // Worker 0 should stick to query A for all three of its tasks before touching B.
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

// A newly arrived higher-priority query pulls a worker off its current (lower
// priority) query at the next take (cooperative preemption).
TEST(GlobalPipelineMLFQTest, PreemptionByHigherPriorityQuery) {
    MultiCoreTaskQueue q(1);
    auto* qa = qkey(0xA); // L1 (5s)
    auto* qc = qkey(0xC); // L0 (0)

    ASSERT_TRUE(q.push_back(make_task(qa, 5 * kSecondNs)).ok());
    ASSERT_TRUE(q.push_back(make_task(qa, 5 * kSecondNs)).ok());

    auto t1 = q.take(0); // worker latches onto A (only query present)
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qa);

    // Higher-priority query C arrives.
    ASSERT_TRUE(q.push_back(make_task(qc, 0)).ok());

    auto t2 = q.take(0); // should preempt to C despite A still having a runnable task
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qc);

    q.close();
}

// With the soft per-query worker cap set, workers beyond the cap spill to a
// lower-priority query even though the top query still has runnable tasks.
TEST(GlobalPipelineMLFQTest, LeaseSpillWhenCapped) {
    int saved = config::pipeline_query_worker_cap;
    config::pipeline_query_worker_cap = 1;

    MultiCoreTaskQueue q(2);
    auto* qa = qkey(0xA); // L0, three tasks
    auto* qb = qkey(0xB); // L1, one task

    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(q.push_back(make_task(qa, 0)).ok());
    }
    ASSERT_TRUE(q.push_back(make_task(qb, 5 * kSecondNs)).ok());

    auto t0 = q.take(0); // A takes its single allowed worker slot
    ASSERT_NE(t0, nullptr);
    EXPECT_EQ(t0->query_ctx_raw(), qa);

    // A is at its cap (1 in-flight, not released), so worker 1 spills to B.
    auto t1 = q.take(1);
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qb);

    q.close();
    config::pipeline_query_worker_cap = saved;
}

// ---------------------------------------------------------------------------
// CPU-lease scheduling mode (config::enable_cpu_lease_scheduling).
// ---------------------------------------------------------------------------

// Exposes the zero-timeout single-attempt take so admission/denial can be asserted
// without the 100ms wait of the public take().
class LeaseTestQueue : public MultiCoreTaskQueue {
public:
    explicit LeaseTestQueue(int cores) : MultiCoreTaskQueue(cores) {}
    PipelineTaskSPtr try_take(int worker = 0) { return _take(worker, 0); }
};

// RAII guard that flips lease-scheduling config for the duration of a test.
class LeaseConfigGuard {
public:
    LeaseConfigGuard(int max_active, int leveling_slots = 8) {
        _saved_enable = config::enable_cpu_lease_scheduling;
        _saved_active = config::max_active_queries_per_group;
        _saved_leveling = config::cpu_lease_leveling_slots;
        _saved_cap = config::cpu_lease_max_threads_per_query;
        config::enable_cpu_lease_scheduling = true;
        config::max_active_queries_per_group = max_active;
        config::cpu_lease_leveling_slots = leveling_slots;
        config::cpu_lease_max_threads_per_query = 0; // unbounded per-query
    }
    ~LeaseConfigGuard() {
        config::enable_cpu_lease_scheduling = _saved_enable;
        config::max_active_queries_per_group = _saved_active;
        config::cpu_lease_leveling_slots = _saved_leveling;
        config::cpu_lease_max_threads_per_query = _saved_cap;
    }

private:
    bool _saved_enable;
    int _saved_active;
    int _saved_leveling;
    int _saved_cap;
};

// QueryCpuLease unit: running-slot accounting (drives scanner bundling) and the
// layer + CPU-band priority computation.
TEST(QueryCpuLeaseTest, SlotAccountingAndBandPriority) {
    QueryCpuLease lease;
    EXPECT_EQ(lease.running_slots(), 0);
    lease.acquire_slot();
    lease.acquire_slot();
    EXPECT_EQ(lease.running_slots(), 2);
    lease.release_slot();
    EXPECT_EQ(lease.running_slots(), 1);
    // Underflow guard: extra releases never drive it negative.
    lease.release_slot();
    lease.release_slot();
    EXPECT_EQ(lease.running_slots(), 0);

    // CPU bands follow the original Doris demotion quantum: <=1s -> 0, <=3s -> 1,
    // <=10s -> 2, <=60s -> 3, <=300s -> 4, else -> 5.
    EXPECT_EQ(QueryCpuLease::pick_cpu_band(0), 0);
    EXPECT_EQ(QueryCpuLease::pick_cpu_band(2 * kSecondNs), 1);
    EXPECT_EQ(QueryCpuLease::pick_cpu_band(5 * kSecondNs), 2);
    EXPECT_EQ(QueryCpuLease::pick_cpu_band(10 * kSecondNs), 2);
    EXPECT_EQ(QueryCpuLease::pick_cpu_band(30 * kSecondNs), 3);
    EXPECT_EQ(QueryCpuLease::pick_cpu_band(100 * kSecondNs), 4);
    EXPECT_EQ(QueryCpuLease::pick_cpu_band(500 * kSecondNs), 5);

    // With few slots the parallelism layer is 0, so priority == band.
    EXPECT_EQ(lease.compute_priority(0), 0);
    EXPECT_EQ(lease.compute_priority(2 * kSecondNs), 1);
    EXPECT_EQ(lease.compute_priority(30 * kSecondNs), 3);
}

// Parallelism layer dominates the CPU band: a query holding many slots is demoted below
// a query with higher CPU but fewer slots.
TEST(QueryCpuLeaseTest, ParallelismLayerDominatesBand) {
    config::cpu_lease_leveling_slots = 4; // one layer per 4 slots
    QueryCpuLease wide;
    for (int i = 0; i < 8; ++i) {
        wide.acquire_slot(); // layer = min(8/4, kNumLayers-1) = 1
    }
    QueryCpuLease narrow; // layer 0, but lots of CPU -> deeper band
    // wide: layer 1, band 0 -> 1*kLayerWidth + 0 = 6
    // narrow: layer 0, band 3 (30s) -> 3
    EXPECT_GT(wide.compute_priority(0), narrow.compute_priority(30 * kSecondNs));
    config::cpu_lease_leveling_slots = 8;
}

// The grantor admits at most max_active_queries_per_group queries; a query beyond the
// cap is not served until an admitted query frees its slot.
TEST(CpuLeaseSchedulingTest, AdmissionCapLimitsConcurrentQueries) {
    LeaseConfigGuard guard(/*max_active=*/2);
    CpuLeaseGrantor grantor;
    LeaseTestQueue q(1);
    q.set_grantor(&grantor);

    std::map<uintptr_t, std::unique_ptr<QueryCpuLease>> leases;
    auto lease_for = [&](uintptr_t id) {
        auto& l = leases[id];
        if (!l) {
            l = std::make_unique<QueryCpuLease>();
        }
        return l.get();
    };

    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);
    auto* qc = qkey(0xC);
    ASSERT_TRUE(q.push_back(std::make_shared<MockPipelineTask>(qa, 0, lease_for(0xA))).ok());
    ASSERT_TRUE(q.push_back(std::make_shared<MockPipelineTask>(qb, 0, lease_for(0xB))).ok());
    ASSERT_TRUE(q.push_back(std::make_shared<MockPipelineTask>(qc, 0, lease_for(0xC))).ok());

    auto t0 = q.try_take(); // admits A
    ASSERT_NE(t0, nullptr);
    EXPECT_EQ(t0->query_ctx_raw(), qa);
    auto t1 = q.try_take(); // admits B (cap now full: {A,B})
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qb);

    // C cannot be admitted (cap reached, A and B still hold admission via in-flight).
    auto denied = q.try_take();
    EXPECT_EQ(denied, nullptr);
    EXPECT_EQ(grantor.admitted_count(), 2);

    // A finishes -> frees its admission slot; C can now be admitted.
    q.update_statistics(t0.get(), 0);
    auto t2 = q.try_take();
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qc);

    q.close();
}

// In lease mode, queries are served in layer+band priority order across CPU bands.
TEST(CpuLeaseSchedulingTest, BandPriorityOrdering) {
    LeaseConfigGuard guard(/*max_active=*/8);
    CpuLeaseGrantor grantor;
    LeaseTestQueue q(1);
    q.set_grantor(&grantor);

    QueryCpuLease la, lb, lc;
    auto* qa = qkey(0xA); // 0s   -> band 0
    auto* qb = qkey(0xB); // 10s  -> band 2
    auto* qc = qkey(0xC); // 30s  -> band 3

    // Push lowest-priority first to prove ordering is by priority, not insertion.
    ASSERT_TRUE(q.push_back(std::make_shared<MockPipelineTask>(qc, 30 * kSecondNs, &lc)).ok());
    ASSERT_TRUE(q.push_back(std::make_shared<MockPipelineTask>(qb, 10 * kSecondNs, &lb)).ok());
    ASSERT_TRUE(q.push_back(std::make_shared<MockPipelineTask>(qa, 0, &la)).ok());

    auto t0 = q.try_take();
    ASSERT_NE(t0, nullptr);
    EXPECT_EQ(t0->query_ctx_raw(), qa);
    auto t1 = q.try_take();
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qb);
    auto t2 = q.try_take();
    ASSERT_NE(t2, nullptr);
    EXPECT_EQ(t2->query_ctx_raw(), qc);

    q.close();
}

// A query that consumes more CPU is re-leveled into a higher band and yields the worker
// to a fresher (lower-CPU) query at the next take (cooperative preemption).
TEST(CpuLeaseSchedulingTest, CooperativePreemptionOnCpuOverrun) {
    LeaseConfigGuard guard(/*max_active=*/8);
    CpuLeaseGrantor grantor;
    LeaseTestQueue q(1);
    q.set_grantor(&grantor);

    QueryCpuLease la, lb;
    auto* qa = qkey(0xA);
    auto* qb = qkey(0xB);

    // A starts cheap (band 0) with two tasks; one will be mutated to band 2.
    auto a_task1 = std::make_shared<MockPipelineTask>(qa, 0, &la);
    auto a_task2 = std::make_shared<MockPipelineTask>(qa, 0, &la);
    ASSERT_TRUE(q.push_back(a_task1).ok());
    ASSERT_TRUE(q.push_back(a_task2).ok());

    auto t0 = q.try_take(); // worker latches onto A
    ASSERT_NE(t0, nullptr);
    EXPECT_EQ(t0->query_ctx_raw(), qa);

    // A's remaining task now reflects heavy CPU consumption (band 2), and a fresh
    // band-0 query B arrives.
    a_task2->set_runtime_ns(30 * kSecondNs);
    ASSERT_TRUE(q.push_back(std::make_shared<MockPipelineTask>(qb, 0, &lb)).ok());

    auto t1 = q.try_take(); // should preempt to B (A demoted to a deeper band)
    ASSERT_NE(t1, nullptr);
    EXPECT_EQ(t1->query_ctx_raw(), qb);

    q.close();
}

} // namespace doris
