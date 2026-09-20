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

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>

#include "exec/operator/mock_scan_operator.h"
#include "exec/scan/scanner.h"
#include "runtime/query_context.h"
#include "runtime/runtime_profile.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/stopwatch.hpp"

namespace doris {

// Scanner threads are a large share of a query's CPU, so `Scanner::update_scan_cpu_timer`
// charges each scan slice into the query-global runtime counter that the pipeline task
// scheduler ranks queries by. `ScannerScheduler::_scanner_scan` calls it exactly once per
// slice (the yield defer, or the eos block), for both the plain thread pool and the
// time-sharing task executor, so this is the whole of the scan-side accounting.
namespace {

// Minimal concrete Scanner so the abstract base can be instantiated. The behavior under
// test lives entirely in the base class and never calls `_get_block_impl`.
class TestScanner final : public Scanner {
public:
    TestScanner(RuntimeState* state, ScanLocalStateBase* local_state, RuntimeProfile* profile)
            : Scanner(state, local_state, -1 /*limit*/, profile) {}

protected:
    Status _get_block_impl(RuntimeState* /*state*/, Block* /*block*/, bool* eof) override {
        *eof = true;
        return Status::OK();
    }
};

// Burn thread CPU without sleeping, so the slice really has CPU time to charge however
// loaded the machine running the test is.
void burn_cpu(int64_t nanos) {
    MonotonicStopWatch watch;
    watch.start();
    volatile uint64_t sink = 0;
    while (watch.elapsed_time() < nanos) {
        for (int i = 0; i < 1024; ++i) {
            sink = sink + static_cast<uint64_t>(i);
        }
    }
}

constexpr int64_t kBurnNanos = 2 * 1000 * 1000; // 2ms

} // namespace

class ScannerCpuAccountingTest : public testing::Test {
protected:
    void SetUp() override {
        _op = std::make_shared<MockScanOperatorX>();
        _local_state = std::make_shared<MockScanLocalState>(&_state, _op.get());
        _scanner = std::make_unique<TestScanner>(&_state, _local_state.get(), &_profile);
    }

    std::atomic<uint64_t>* query_counter() {
        return _state.get_query_ctx()->query_runtime_counter();
    }

    MockRuntimeState _state;
    RuntimeProfile _profile {"ScannerCpuAccountingTest"};
    std::shared_ptr<MockScanOperatorX> _op;
    std::shared_ptr<MockScanLocalState> _local_state;
    std::unique_ptr<TestScanner> _scanner;
};

// One slice: the ranking counter is fed the same delta as the scanner's own profile
// timer, which is exact because both come from one read of the thread CPU watch.
TEST_F(ScannerCpuAccountingTest, charges_one_slice_into_the_query_counter) {
    const uint64_t before = query_counter()->load();

    _scanner->start_scan_cpu_timer();
    burn_cpu(kBurnNanos);
    _scanner->update_scan_cpu_timer();

    const uint64_t charged = query_counter()->load() - before;
    EXPECT_GT(charged, 0);
    EXPECT_EQ(charged, static_cast<uint64_t>(_scanner->_scan_cpu_timer));
}

// `start_scan_cpu_timer` resets the watch, so consecutive slices each contribute only
// their own CPU: the counter ends up at the sum, with nothing counted twice.
TEST_F(ScannerCpuAccountingTest, accumulates_across_slices_without_double_counting) {
    const uint64_t before = query_counter()->load();

    _scanner->start_scan_cpu_timer();
    burn_cpu(kBurnNanos);
    _scanner->update_scan_cpu_timer();
    const uint64_t after_first = query_counter()->load();

    _scanner->start_scan_cpu_timer();
    burn_cpu(kBurnNanos);
    _scanner->update_scan_cpu_timer();
    const uint64_t after_second = query_counter()->load();

    EXPECT_GT(after_first, before);
    EXPECT_GT(after_second, after_first);
    EXPECT_EQ(after_second - before, static_cast<uint64_t>(_scanner->_scan_cpu_timer));
}

// A scanner whose state has no query context (nothing to rank) charges nobody, and the
// scanner's own profile timer still advances.
TEST_F(ScannerCpuAccountingTest, charges_nothing_without_a_query_context) {
    QueryContext* query_ctx = _state._query_ctx;
    const uint64_t before = query_ctx->query_runtime_counter()->load();
    _state._query_ctx = nullptr;

    _scanner->start_scan_cpu_timer();
    burn_cpu(kBurnNanos);
    _scanner->update_scan_cpu_timer();

    // Restored before teardown so the state is destroyed exactly as the other tests
    // leave it.
    _state._query_ctx = query_ctx;

    EXPECT_EQ(query_ctx->query_runtime_counter()->load(), before);
    EXPECT_GT(_scanner->_scan_cpu_timer, 0);
}

} // namespace doris
