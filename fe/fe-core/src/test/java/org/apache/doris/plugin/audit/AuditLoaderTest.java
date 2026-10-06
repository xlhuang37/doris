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

package org.apache.doris.plugin.audit;

import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.catalog.InternalSchema;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.plugin.AuditEvent;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class AuditLoaderTest {

    // The stream load maps buffer fields to AUDIT_SCHEMA columns by position, so the
    // loader must write exactly one field per column, in schema order.
    @Test
    public void testLogBufferMatchesAuditSchema() {
        AuditEvent event = new AuditEvent.AuditEventBuilder()
                .setTimestamp(1741760376000L)
                .setCpuTimeMs(1234L)
                .setAttainedServiceMs(5678L)
                .setPeakMemoryBytes(4321L)
                .setStmt("select 1")
                .build();
        StringBuilder buffer = new StringBuilder();
        Deencapsulation.invoke(new AuditLoader(), "fillLogBuffer", event, buffer);

        String line = buffer.toString();
        Assert.assertEquals(AuditLoader.AUDIT_TABLE_LINE_DELIMITER, line.charAt(line.length() - 1));
        String[] fields = line.substring(0, line.length() - 1)
                .split(String.valueOf(AuditLoader.AUDIT_TABLE_COL_SEPARATOR), -1);
        List<String> columns = InternalSchema.AUDIT_SCHEMA.stream()
                .map(ColumnDef::getName)
                .collect(Collectors.toList());
        Assert.assertEquals(columns.size(), fields.length);

        Assert.assertEquals("1234", fields[columns.indexOf("cpu_time_ms")]);
        Assert.assertEquals("5678", fields[columns.indexOf("attained_service_ms")]);
        Assert.assertEquals("4321", fields[columns.indexOf("peak_memory_bytes")]);
        Assert.assertEquals("select 1", fields[columns.indexOf("stmt")]);
    }
}
