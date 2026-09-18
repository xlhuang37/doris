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

package org.apache.doris.common.profile;

import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.SafeStringBuilder;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPipelineWorkerScheduleRecord;
import org.apache.doris.thrift.TQueryProfile;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class PipelineWorkerTimelineTest {

    private static TPipelineWorkerScheduleRecord record(String scheduler, int workerIndex, int fragmentId,
            int pipelineId, String taskName, long takeUs, long releaseUs, String releaseReason) {
        TPipelineWorkerScheduleRecord record = new TPipelineWorkerScheduleRecord();
        record.setScheduler(scheduler);
        record.setWorkerIndex(workerIndex);
        record.setFragmentId(fragmentId);
        record.setPipelineId(pipelineId);
        record.setTaskName(taskName);
        record.setTakeUs(takeUs);
        record.setReleaseUs(releaseUs);
        record.setReleaseReason(releaseReason);
        return record;
    }

    private static TQueryProfile queryProfile(TUniqueId queryId, List<TPipelineWorkerScheduleRecord> records,
            Long dropped) {
        TQueryProfile profile = new TQueryProfile();
        profile.setQueryId(queryId);
        // updateProfile() rejects a report without this field, an empty map is enough here.
        profile.setFragmentIdToProfile(Maps.newHashMap());
        if (records != null) {
            profile.setWorkerScheduleRecords(records);
        }
        if (dropped != null) {
            profile.setDroppedWorkerScheduleRecords(dropped);
        }
        return profile;
    }

    @Test
    public void testRecordsAreGroupedByWorkerAndOrderedInTime() {
        TUniqueId queryId = new TUniqueId(1L, 2L);
        ExecutionProfile executionProfile = new ExecutionProfile(queryId, Lists.newArrayList(0));
        TNetworkAddress be = new TNetworkAddress("127.0.0.1", 9050);

        // Deliberately out of order, both across workers and within one worker.
        List<TPipelineWorkerScheduleRecord> records = Lists.newArrayList(
                record("simple", 1, 0, 2, "task1(PIPELINE_2)", 300, 340, "EXECUTED"),
                record("simple", 0, 0, 2, "task0(PIPELINE_2)", 200, 250, "CLOSED"),
                record("simple", 0, 0, 2, "task0(PIPELINE_2)", 100, 150, "EXECUTED"),
                record("blocking", 0, 0, 1, "task0(PIPELINE_1)", 400, 410, "EXECUTED"));
        executionProfile.updateProfile(queryProfile(queryId, records, null), be, true);

        Assert.assertTrue(executionProfile.hasWorkerScheduleRecords());

        SafeStringBuilder builder = new SafeStringBuilder();
        executionProfile.appendWorkerTimeline(builder);
        String[] lines = builder.toString().split("\n");

        String prefix = DebugUtil.printId(queryId) + "|127.0.0.1:9050|";
        Assert.assertEquals(4, lines.length);
        Assert.assertEquals(prefix + "blocking|0|0|1|task0(PIPELINE_1)|400|410|10|EXECUTED", lines[0]);
        Assert.assertEquals(prefix + "simple|0|0|2|task0(PIPELINE_2)|100|150|50|EXECUTED", lines[1]);
        Assert.assertEquals(prefix + "simple|0|0|2|task0(PIPELINE_2)|200|250|50|CLOSED", lines[2]);
        Assert.assertEquals(prefix + "simple|1|0|2|task1(PIPELINE_2)|300|340|40|EXECUTED", lines[3]);
    }

    @Test
    public void testRecordsFromMultipleBackendsAreKeptSeparate() {
        TUniqueId queryId = new TUniqueId(3L, 4L);
        ExecutionProfile executionProfile = new ExecutionProfile(queryId, Lists.newArrayList(0));

        executionProfile.updateProfile(queryProfile(queryId, Lists.newArrayList(
                record("simple", 0, 0, 1, "task0(PIPELINE_1)", 100, 110, "EXECUTED")), null),
                new TNetworkAddress("127.0.0.2", 9050), true);
        executionProfile.updateProfile(queryProfile(queryId, Lists.newArrayList(
                record("simple", 0, 0, 1, "task0(PIPELINE_1)", 100, 120, "EXECUTED")), 7L),
                new TNetworkAddress("127.0.0.1", 9050), true);

        SafeStringBuilder builder = new SafeStringBuilder();
        executionProfile.appendWorkerTimeline(builder);
        String[] lines = builder.toString().split("\n");

        Assert.assertEquals(3, lines.length);
        Assert.assertEquals("# dropped|" + DebugUtil.printId(queryId) + "|127.0.0.1:9050|7", lines[0]);
        Assert.assertTrue(lines[1].contains("|127.0.0.1:9050|"));
        Assert.assertTrue(lines[1].endsWith("|100|120|20|EXECUTED"));
        Assert.assertTrue(lines[2].contains("|127.0.0.2:9050|"));
        Assert.assertTrue(lines[2].endsWith("|100|110|10|EXECUTED"));
    }

    @Test
    public void testIterationWithoutTaskIdentityRendersPlaceholder() {
        TUniqueId queryId = new TUniqueId(5L, 6L);
        ExecutionProfile executionProfile = new ExecutionProfile(queryId, Lists.newArrayList(0));

        // A task handed straight back is recorded without pipeline or task identity.
        TPipelineWorkerScheduleRecord putBack = new TPipelineWorkerScheduleRecord();
        putBack.setScheduler("simple");
        putBack.setWorkerIndex(2);
        putBack.setFragmentId(0);
        putBack.setPipelineId(-1);
        putBack.setTakeUs(10);
        putBack.setReleaseUs(11);
        putBack.setReleaseReason("PUT_BACK");
        executionProfile.updateProfile(queryProfile(queryId, Lists.newArrayList(putBack), null),
                new TNetworkAddress("127.0.0.1", 9050), true);

        SafeStringBuilder builder = new SafeStringBuilder();
        executionProfile.appendWorkerTimeline(builder);
        Assert.assertEquals(DebugUtil.printId(queryId) + "|127.0.0.1:9050|simple|2|0|-1"
                + "|-|10|11|1|PUT_BACK", builder.toString().trim());
    }

    @Test
    public void testNoRecordsWhenBackendReportsNone() {
        TUniqueId queryId = new TUniqueId(7L, 8L);
        ExecutionProfile executionProfile = new ExecutionProfile(queryId, Lists.newArrayList(0));
        executionProfile.updateProfile(queryProfile(queryId, null, null),
                new TNetworkAddress("127.0.0.1", 9050), true);

        Assert.assertFalse(executionProfile.hasWorkerScheduleRecords());

        SafeStringBuilder builder = new SafeStringBuilder();
        executionProfile.appendWorkerTimeline(builder);
        Assert.assertEquals("", builder.toString());
    }

    @Test
    public void testSectionIsLastAndOnlyEmittedFromProfileLevelTwo() {
        TUniqueId queryId = new TUniqueId(9L, 10L);
        List<TPipelineWorkerScheduleRecord> records = Lists.newArrayList(
                record("simple", 0, 0, 1, "task0(PIPELINE_1)", 100, 150, "EXECUTED"));

        Profile detailed = new Profile(true, 2, -1);
        ExecutionProfile detailedExecution = new ExecutionProfile(queryId, Lists.newArrayList(0));
        detailedExecution.updateProfile(queryProfile(queryId, records, null),
                new TNetworkAddress("127.0.0.1", 9050), true);
        detailed.addExecutionProfile(detailedExecution);

        SafeStringBuilder builder = new SafeStringBuilder();
        detailed.getExecutionProfileContent(builder);
        String content = builder.toString();

        int sectionStart = content.indexOf("\nPipelineWorkerTimeline:\n");
        Assert.assertTrue("timeline section should be present", sectionStart >= 0);
        Assert.assertTrue("timeline section should come after MergedProfile",
                sectionStart > content.indexOf("MergedProfile:"));
        Assert.assertTrue("timeline section should come after Appendix",
                sectionStart > content.indexOf("\nAppendix:\n"));
        Assert.assertTrue("timeline section should be the last section",
                content.trim().endsWith("|100|150|50|EXECUTED"));

        Profile merged = new Profile(true, 1, -1);
        ExecutionProfile mergedExecution = new ExecutionProfile(queryId, Lists.newArrayList(0));
        mergedExecution.updateProfile(queryProfile(queryId, records, null),
                new TNetworkAddress("127.0.0.1", 9050), true);
        merged.addExecutionProfile(mergedExecution);

        SafeStringBuilder mergedBuilder = new SafeStringBuilder();
        merged.getExecutionProfileContent(mergedBuilder);
        Assert.assertFalse("timeline section must not be emitted at profile level 1",
                mergedBuilder.toString().contains("PipelineWorkerTimeline:"));
    }
}
