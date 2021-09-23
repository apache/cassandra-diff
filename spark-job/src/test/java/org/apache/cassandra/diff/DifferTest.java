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
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.common.base.VerifyException;
import com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DifferTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testIncludeAllPartitions() {
        final PartitionKey testKey = new RangeComparatorTest.TestPartitionKey(0);
        final UUID uuid = UUID.fromString("cde3b15d-2363-4028-885a-52de58bad64e");
        assertTrue(Differ.shouldIncludePartition(uuid, 1).test(testKey));
    }

    @Test
    public void shouldIncludePartitionWithProbabilityInvalidProbability() {
        final PartitionKey testKey = new RangeComparatorTest.TestPartitionKey(0);
        final UUID uuid = UUID.fromString("cde3b15d-2363-4028-885a-52de58bad64e");
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid partition sampling property, it should be between 0 and 1");
        Differ.shouldIncludePartition(uuid, -1).test(testKey);
    }

    @Test
    public void shouldIncludePartitionWithProbabilityHalf() {
        final PartitionKey testKey = new RangeComparatorTest.TestPartitionKey(0);
        int count = 0;
        final UUID uuid = UUID.fromString("cde3b15d-2363-4028-885a-52de58bad64e");
        final Predicate<PartitionKey> partitionSampler = Differ.shouldIncludePartition(uuid, 0.5);
        for (int i = 0; i < 20; i++) {
            if (partitionSampler.test(testKey)) {
                count++;
            }
        }
        assertTrue(count <= 15);
        assertTrue(count >= 5);
    }

    @Test
    public void shouldIncludePartitionShouldGenerateSameSequenceForGivenJobId() {
        final UUID uuid = UUID.fromString("cde3b15d-2363-4028-885a-52de58bad64e");
        final PartitionKey testKey = new RangeComparatorTest.TestPartitionKey(0);
        final Predicate<PartitionKey> partitionSampler1 = Differ.shouldIncludePartition(uuid, 0.5);
        final Predicate<PartitionKey> partitionSampler2 = Differ.shouldIncludePartition(uuid, 0.5);
        for (int i = 0; i < 10; i++) {
            assertEquals(partitionSampler2.test(testKey), partitionSampler1.test(testKey));
        }
    }

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
        Iterable<KeyspaceTablePair> tables = Lists.newArrayList(ksTbl("t1"), ksTbl("t2"), ksTbl("t3"));
        Function<KeyspaceTablePair, DiffJob.TaskStatus> journal = (keyspaceTable) -> {
            switch (keyspaceTable.table) {
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

        Map<KeyspaceTablePair, DiffJob.TaskStatus> filtered = Differ.filterTables(tables, split, journal, false);
        assertEquals(2, filtered.keySet().size());
        assertEquals(RangeStats.withValues(5, 5, 5, 5, 5, 5, 5, 5, 5), filtered.get(ksTbl("t2")).stats);
        assertEquals(BigInteger.valueOf(5L), filtered.get(ksTbl("t2")).lastToken);
        assertEquals(RangeStats.newStats(), filtered.get(ksTbl("t3")).stats);
        assertNull(filtered.get(ksTbl("t3")).lastToken);

        // if re-running (part of) a job because of failures or problematic partitions, we want to
        // ignore the status of completed tasks and re-run them anyway as only specified tokens will
        // be processed - so t1 should be included now
        filtered = Differ.filterTables(tables, split, journal, true);
        assertEquals(3, filtered.keySet().size());
        assertEquals(RangeStats.withValues(6, 6, 6, 6, 6, 6, 6, 6, 6), filtered.get(ksTbl("t1")).stats);
        assertEquals(split.end, filtered.get(ksTbl("t1")).lastToken);
        assertEquals(RangeStats.withValues(5, 5, 5, 5, 5, 5, 5, 5, 5), filtered.get(ksTbl("t2")).stats);
        assertEquals(BigInteger.valueOf(5L), filtered.get(ksTbl("t2")).lastToken);
        assertEquals(RangeStats.newStats(), filtered.get(ksTbl("t3")).stats);
        assertNull(filtered.get(ksTbl("t3")).lastToken);
    }

    private KeyspaceTablePair ksTbl(String table) {
        return new KeyspaceTablePair("ks", table);
    }
}
