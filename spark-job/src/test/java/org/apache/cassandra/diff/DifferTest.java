/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.diff;

import java.math.BigInteger;
import java.util.Map;
import java.util.function.Function;

import com.google.common.base.VerifyException;
import org.junit.Test;

import com.google.common.collect.Lists;

import org.apache.cassandra.diff.DiffJob;
import org.apache.cassandra.diff.Differ;
import org.apache.cassandra.diff.RangeStats;
import org.apache.cassandra.diff.TokenHelper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DifferTest {

    @Test(expected = VerifyException.class)
    public void rejectNullStartOfRange() {
        Differ.validateRange(null, BigInteger.TEN, TokenHelper.MURMUR3);
    }

    @Test(expected = VerifyException.class)
    public void rejectNullEndOfRange() {
        Differ.validateRange(BigInteger.TEN, null, TokenHelper.MURMUR3);
    }

    @Test(expected = VerifyException.class)
    public void rejectWrappingRange() {
        Differ.validateRange(BigInteger.TEN, BigInteger.ONE, TokenHelper.MURMUR3);
    }

    @Test(expected = VerifyException.class)
    public void rejectRangeWithStartLessThanMinMurmurToken() {
        Differ.validateRange(TokenHelper.MURMUR3.min().subtract(BigInteger.ONE),
                             BigInteger.TEN,
                             TokenHelper.MURMUR3);
    }

    @Test(expected = VerifyException.class)
    public void rejectRangeWithEndGreaterThanMaxMurmurToken() {
        Differ.validateRange(BigInteger.ONE,
                             TokenHelper.MURMUR3.max().add(BigInteger.ONE),
                             TokenHelper.MURMUR3);
    }

    @Test
    public void filterTaskStatusForTables() {
        // according to the journal:
        // * t1 is already completed
        // * t2 is started and has reported some progress, but has not completed
        // * t3 has not reported any progress
        DiffJob.Split split = new DiffJob.Split(1, 1, BigInteger.ONE, BigInteger.TEN);
        Iterable<String> tables = Lists.newArrayList("t1", "t2", "t3");
        Function<String, DiffJob.TaskStatus> journal = (table) -> {
            switch (table) {
                case "t1":
                    return new DiffJob.TaskStatus(split.end, RangeStats.withValues(6, 6, 6, 6, 6, 6, 6, 6, 6));
                case "t2":
                    return new DiffJob.TaskStatus(BigInteger.valueOf(5L), RangeStats.withValues(5, 5, 5, 5, 5, 5, 5, 5, 5));
                case "t3":
                    return DiffJob.TaskStatus.EMPTY;
                default:
                    throw new AssertionError();
            }
        };

        Map<String, DiffJob.TaskStatus> filtered = Differ.filterTables(tables, split, journal, false);
        assertEquals(2, filtered.keySet().size());
        assertEquals(RangeStats.withValues(5, 5, 5, 5, 5, 5, 5, 5, 5), filtered.get("t2").stats);
        assertEquals(BigInteger.valueOf(5L), filtered.get("t2").lastToken);
        assertEquals(RangeStats.newStats(), filtered.get("t3").stats);
        assertNull(filtered.get("t3").lastToken);

        // if re-running (part of) a job because of failures or problematic partitions, we want to
        // ignore the status of completed tasks and re-run them anyway as only specified tokens will
        // be processed - so t1 should be included now
        filtered = Differ.filterTables(tables, split, journal, true);
        assertEquals(3, filtered.keySet().size());
        assertEquals(RangeStats.withValues(6, 6, 6, 6, 6, 6, 6, 6, 6), filtered.get("t1").stats);
        assertEquals(split.end, filtered.get("t1").lastToken);
        assertEquals(RangeStats.withValues(5, 5, 5, 5, 5, 5, 5, 5, 5), filtered.get("t2").stats);
        assertEquals(BigInteger.valueOf(5L), filtered.get("t2").lastToken);
        assertEquals(RangeStats.newStats(), filtered.get("t3").stats);
        assertNull(filtered.get("t3").lastToken);
    }

}
