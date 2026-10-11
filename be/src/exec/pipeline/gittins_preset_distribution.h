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
#pragma once

#include <stddef.h>
#include <stdint.h>

#include <array>
#include <memory>

#include "exec/pipeline/gittins_histogram.h"

namespace doris {
#include "common/compile_check_begin.h"

// Pre-installed final query CPU time distribution for Gittins ranking: the measured
// max CPU time (ms) of each benchmark query file, one sample per file. The scheduler
// builds its index table from this once and never updates it.
inline constexpr std::array<uint64_t, 88> kGittinsPresetCpuTimeMs = {
        61038,  // 100_q1.sql
        43303,  // 100_q10.sql
        6843,   // 100_q11.sql
        7597,   // 100_q12.sql
        39074,  // 100_q13.sql
        2566,   // 100_q14.sql
        7109,   // 100_q15.sql
        7223,   // 100_q16.sql
        9886,   // 100_q17.sql
        64075,  // 100_q18.sql
        22021,  // 100_q19.sql
        1150,   // 100_q2.sql
        4787,   // 100_q20.sql
        39736,  // 100_q21.sql
        6452,   // 100_q22.sql
        8612,   // 100_q3.sql
        7171,   // 100_q4.sql
        21830,  // 100_q5.sql
        2016,   // 100_q6.sql
        16067,  // 100_q7.sql
        15745,  // 100_q8.sql
        47254,  // 100_q9.sql
        20280,  // 20_q1 copy.sql
        12449,  // 20_q1.sql
        7876,   // 20_q10 copy.sql
        5690,   // 20_q10.sql
        1766,   // 20_q11 copy.sql
        953,    // 20_q11.sql
        1293,   // 20_q12 copy.sql
        797,    // 20_q12.sql
        7304,   // 20_q13 copy.sql
        4579,   // 20_q13.sql
        907,    // 20_q14 copy.sql
        913,    // 20_q14.sql
        1424,   // 20_q15 copy.sql
        1354,   // 20_q15.sql
        1894,   // 20_q16 copy.sql
        1768,   // 20_q16.sql
        1660,   // 20_q17 copy.sql
        805,    // 20_q17.sql
        9198,   // 20_q18 copy.sql
        8785,   // 20_q18.sql
        2344,   // 20_q19 copy.sql
        974,    // 20_q19.sql
        562,    // 20_q2 copy.sql
        553,    // 20_q2.sql
        1232,   // 20_q20 copy.sql
        832,    // 20_q20.sql
        6148,   // 20_q21 copy.sql
        4230,   // 20_q21.sql
        1286,   // 20_q22 copy.sql
        1220,   // 20_q22.sql
        1541,   // 20_q3 copy.sql
        1230,   // 20_q3.sql
        1698,   // 20_q4 copy.sql
        1504,   // 20_q4.sql
        3616,   // 20_q5 copy.sql
        3493,   // 20_q5.sql
        380,    // 20_q6 copy.sql
        318,    // 20_q6.sql
        2026,   // 20_q7 copy.sql
        1428,   // 20_q7.sql
        4193,   // 20_q8 copy.sql
        2580,   // 20_q8.sql
        7891,   // 20_q9 copy.sql
        6804,   // 20_q9.sql
        410643, // 500_q1.sql
        167465, // 500_q10.sql
        38404,  // 500_q11.sql
        35265,  // 500_q12.sql
        226298, // 500_q13.sql
        13889,  // 500_q14.sql
        34509,  // 500_q15.sql
        48008,  // 500_q16.sql
        93191,  // 500_q17.sql
        393519, // 500_q18.sql
        103724, // 500_q19.sql
        18275,  // 500_q2.sql
        29947,  // 500_q20.sql
        261473, // 500_q21.sql
        31781,  // 500_q22.sql
        108804, // 500_q3.sql
        65118,  // 500_q4.sql
        170111, // 500_q5.sql
        21854,  // 500_q6.sql
        69650,  // 500_q7.sql
        141943, // 500_q8.sql
        340747, // 500_q9.sql
};

// Index table over `num_slots` 1-second slots. Must cover the largest sample
// (~411s) or the tail is clamped into the last slot.
inline std::shared_ptr<const GittinsIndexTable> build_preset_gittins_table(int num_slots) {
    GittinsHistogram histogram(num_slots);
    for (uint64_t ms : kGittinsPresetCpuTimeMs) {
        histogram.record(ms * 1'000'000ULL);
    }
    return histogram.build();
}

#include "common/compile_check_end.h"
} // namespace doris
