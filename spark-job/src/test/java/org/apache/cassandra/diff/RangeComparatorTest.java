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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.google.common.collect.*;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Token;
import org.jetbrains.annotations.NotNull;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.apache.cassandra.diff.TestUtils.assertThreadWaits;

public class RangeComparatorTest {

    private Multimap<BigInteger, Throwable> errors = HashMultimap.create();
    private BiConsumer<Throwable, BigInteger> errorReporter = (e, t) -> errors.put(t, e);
    private Multimap<BigInteger, MismatchType> mismatches = HashMultimap.create();
    private BiConsumer<MismatchType, BigInteger> mismatchReporter = (m, t) -> mismatches.put(t, m);
    private Multimap<BigInteger, RangeStats> journal= HashMultimap.create();
    private BiConsumer<RangeStats, BigInteger> progressReporter = (r, t) -> journal.put(t, copyOf(r));
    private Set<BigInteger> comparedPartitions = new HashSet<>();
    private ComparisonExecutor executor = ComparisonExecutor.newExecutor(1, new MetricRegistry());
    private RetryStrategyFactory mockRetryStrategyFactory = new RetryStrategyFactory(null);

    @Test
    public void emptyRange() {
        RangeComparator comparator = comparator(context(100L, 100L));
        RangeStats stats = comparator.compare(keys(), keys(), this::alwaysMatch);
        assertTrue(stats.isEmpty());
        assertNothingReported(errors, mismatches, journal);
        assertCompared();
    }

    @Test
    public void sourceAndTargetKeysAreEmpty() {
        RangeComparator comparator = comparator(context(0L, 100L));
        RangeStats stats = comparator.compare(keys(), keys(), this::alwaysMatch);
        assertTrue(stats.isEmpty());
        assertNothingReported(errors, mismatches, journal);
        assertCompared();
    }

    @Test
    public void partitionPresentOnlyInSource() {
        RangeComparator comparator = comparator(context(0L, 100L));
        RangeStats stats = comparator.compare(keys(0, 1, 2), keys(0, 1), this::alwaysMatch);
        assertFalse(stats.isEmpty());
        assertEquals(1, stats.getOnlyInSource());
        assertEquals(0, stats.getOnlyInTarget());
        assertEquals(2, stats.getMatchedPartitions());
        assertReported(2, MismatchType.ONLY_IN_SOURCE, mismatches);
        assertNothingReported(errors, journal);
        assertCompared(0, 1);
    }

    @Test
    public void partitionPresentOnlyInTarget() {
        RangeComparator comparator = comparator(context(0L, 100L));
        RangeStats stats = comparator.compare(keys(7, 8), keys(7, 8, 9), this::alwaysMatch);
        assertFalse(stats.isEmpty());
        assertEquals(0, stats.getOnlyInSource());
        assertEquals(1, stats.getOnlyInTarget());
        assertEquals(2, stats.getMatchedPartitions());
        assertReported(9, MismatchType.ONLY_IN_TARGET, mismatches);
        assertNothingReported(errors, journal);
        assertCompared(7, 8);
    }

    @Test
    public void multipleOnlyInSource() {
        RangeComparator comparator = comparator(context(0L, 100L));
        RangeStats stats = comparator.compare(keys(4, 5, 6, 7, 8), keys(4, 5), this::alwaysMatch);
        assertFalse(stats.isEmpty());
        assertEquals(3, stats.getOnlyInSource());
        assertEquals(0, stats.getOnlyInTarget());
        assertEquals(2, stats.getMatchedPartitions());
        assertReported(6, MismatchType.ONLY_IN_SOURCE, mismatches);
        assertReported(7, MismatchType.ONLY_IN_SOURCE, mismatches);
        assertReported(8, MismatchType.ONLY_IN_SOURCE, mismatches);
        assertNothingReported(errors, journal);
        assertCompared(4, 5);
    }

    @Test
    public void multipleOnlyInTarget() {
        RangeComparator comparator = comparator(context(0L, 100L));
        RangeStats stats = comparator.compare(keys(4, 5), keys(4, 5, 6, 7, 8), this::alwaysMatch);
        assertFalse(stats.isEmpty());
        assertEquals(0, stats.getOnlyInSource());
        assertEquals(3, stats.getOnlyInTarget());
        assertEquals(2, stats.getMatchedPartitions());
        assertReported(6, MismatchType.ONLY_IN_TARGET, mismatches);
        assertReported(7, MismatchType.ONLY_IN_TARGET, mismatches);
        assertReported(8, MismatchType.ONLY_IN_TARGET, mismatches);
        assertNothingReported(errors, journal);
        assertCompared(4, 5);
    }

    @Test
    public void multipleOnlyInBoth() {
        RangeComparator comparator = comparator(context(0L, 100L));
        RangeStats stats = comparator.compare(keys(0, 1, 3, 5, 7, 9), keys(0, 2, 4, 6, 8, 9), this::alwaysMatch);
        assertFalse(stats.isEmpty());
        assertEquals(4, stats.getOnlyInSource());
        assertEquals(4, stats.getOnlyInTarget());
        assertEquals(2, stats.getMatchedPartitions());
        assertReported(1, MismatchType.ONLY_IN_SOURCE, mismatches);
        assertReported(3, MismatchType.ONLY_IN_SOURCE, mismatches);
        assertReported(5, MismatchType.ONLY_IN_SOURCE, mismatches);
        assertReported(7, MismatchType.ONLY_IN_SOURCE, mismatches);
        assertReported(2, MismatchType.ONLY_IN_TARGET, mismatches);
        assertReported(4, MismatchType.ONLY_IN_TARGET, mismatches);
        assertReported(6, MismatchType.ONLY_IN_TARGET, mismatches);
        assertReported(8, MismatchType.ONLY_IN_TARGET, mismatches);
        assertNothingReported(errors, journal);
        assertCompared(0, 9);
    }

    @Test
    public void sourceKeysIsEmpty() {
        RangeComparator comparator = comparator(context(0L, 100L));
        RangeStats stats = comparator.compare(keys(), keys(4, 5), this::alwaysMatch);
        assertFalse(stats.isEmpty());
        assertEquals(0, stats.getOnlyInSource());
        assertEquals(2, stats.getOnlyInTarget());
        assertEquals(0, stats.getMatchedPartitions());
        assertReported(4, MismatchType.ONLY_IN_TARGET, mismatches);
        assertReported(5, MismatchType.ONLY_IN_TARGET, mismatches);
        assertNothingReported(errors, journal);
        assertCompared();
    }

    @Test
    public void targetKeysIsEmpty() {
        RangeComparator comparator = comparator(context(0L, 100L));
        RangeStats stats = comparator.compare(keys(4, 5), keys(), this::alwaysMatch);
        assertFalse(stats.isEmpty());
        assertEquals(2, stats.getOnlyInSource());
        assertEquals(0, stats.getOnlyInTarget());
        assertEquals(0, stats.getMatchedPartitions());
        assertReported(4, MismatchType.ONLY_IN_SOURCE, mismatches);
        assertReported(5, MismatchType.ONLY_IN_SOURCE, mismatches);
        assertNothingReported(errors, journal);
        assertCompared();
    }

    @Test
    public void skipComparisonOfDisallowedTokens() {
        RangeComparator comparator = comparator(context(0L, 100L, 2, 3, 4));
        RangeStats stats = comparator.compare(keys(0, 1, 2, 3, 4, 5, 6),
                                              keys(0, 1, 2, 3, 4, 5, 6),
                                              this::alwaysMatch);
        assertFalse(stats.isEmpty());
        assertEquals(0, stats.getOnlyInSource());
        assertEquals(0, stats.getOnlyInTarget());
        assertEquals(4, stats.getMatchedPartitions());
        assertEquals(3, stats.getSkippedPartitions());
        assertNothingReported(errors,  mismatches, journal);
        assertCompared(0, 1, 5 , 6);
    }

    @Test
    public void handleErrorReadingFirstSourceKey() {
        RuntimeException toThrow = new RuntimeException("Test");
        testErrorReadingKey(keys(0, toThrow, 0, 1, 2), keys(0, 1, 2), toThrow);
        assertCompared();
    }

    @Test
    public void handleErrorReadingFirstTargetKey() {
        RuntimeException toThrow = new RuntimeException("Test");
        testErrorReadingKey(keys(0, 1, 2), keys(0, toThrow, 0, 1, 2), toThrow);
        assertCompared();
    }

    @Test
    public void handleErrorReadingSourceKey() {
        RuntimeException toThrow = new RuntimeException("Test");
        testErrorReadingKey(keys(1, toThrow, 0, 1, 2), keys(0, 1, 2), toThrow);
        assertCompared(0);
    }

    @Test
    public void handleErrorReadingTargetKey() {
        RuntimeException toThrow = new RuntimeException("Test");
        testErrorReadingKey(keys(0, 1, 2), keys(1, toThrow, 0, 1, 2), toThrow);
        assertCompared(0);
    }

    @Test
    public void handleReadingLastSourceKey() {
        RuntimeException toThrow = new RuntimeException("Test");
        testErrorReadingKey(keys(2, toThrow, 0, 1, 2), keys(0, 1, 2), toThrow);
        assertCompared(0, 1);
    }

    @Test
    public void handleReadingLastTargetKey() {
        RuntimeException toThrow = new RuntimeException("Test");
        testErrorReadingKey(keys(0, 1, 2), keys(2, toThrow, 0, 1, 2), toThrow);
        assertCompared(0, 1);
    }

    @Test
    public void handleErrorConstructingFirstTask() {
        RuntimeException expected = new RuntimeException("Test");
        RangeStats stats = testTaskError(throwDuringConstruction(0, expected));
        assertEquals(1, stats.getErrorPartitions());
        assertCompared(1, 2);
        assertReported(0, expected, errors);
    }

    @Test
    public void handleErrorConstructingTask() {
        RuntimeException expected = new RuntimeException("Test");
        RangeStats stats = testTaskError(throwDuringConstruction(1, expected));
        assertEquals(1, stats.getErrorPartitions());
        assertCompared(0, 2);
        assertReported(1, expected, errors);
    }

    @Test
    public void handleErrorConstructingLastTask() {
        RuntimeException expected = new RuntimeException("Test");
        RangeStats stats = testTaskError(throwDuringConstruction(2, expected));
        assertEquals(1, stats.getErrorPartitions());
        assertCompared(0, 1);
        assertReported(2, expected, errors);
    }

    @Test
    public void handleTaskErrorOnFirstPartition() {
        RuntimeException expected = new RuntimeException("Test");
        RangeStats stats = testTaskError(throwDuringExecution(expected, 0));
        assertEquals(1, stats.getErrorPartitions());
        assertCompared(1, 2);
        assertReported(0, expected, errors);
    }

    @Test
    public void handleTaskErrorOnPartition() {
        RuntimeException expected = new RuntimeException("Test");
        RangeStats stats = testTaskError(throwDuringExecution(expected, 1));
        assertEquals(1, stats.getErrorPartitions());
        assertCompared(0, 2);
        assertReported(1, expected, errors);
    }

    @Test
    public void handleTaskErrorOnLastPartition() {
        RuntimeException expected = new RuntimeException("Test");
        RangeStats stats = testTaskError(throwDuringExecution(expected, 2));
        assertEquals(1, stats.getErrorPartitions());
        assertCompared(0, 1);
        assertReported(2, expected, errors);
    }

    @Test
    public void checkpointEveryTenPartitions() {
        RangeComparator comparator = comparator(context(0L, 100L));
        comparator.compare(keys(LongStream.range(0, 25).toArray()),
                           keys(LongStream.range(0, 25).toArray()),
                           this::alwaysMatch);
        assertReported(9, RangeStats.withValues(10, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L), journal);
        assertReported(19, RangeStats.withValues(20, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L), journal);
        assertEquals(2, journal.keySet().size());
    }

    @Test
    public void recordHighestSeenPartitionWhenTasksCompleteOutOfOrder() {
        // every 10 partitions, the highest seen token is reported to the journal. Here,
        // randomise the iteration of the keys to simulate tasks completing out of order
        RangeComparator comparator = comparator(context(0L, 100L));
        long[] tokens = new long[] {2, 8, 1, 4, 100, 3, 5, 7, 6, 9};
        comparator.compare(keys(tokens),
                           keys(tokens),
                           this::alwaysMatch);
        assertReported(100, RangeStats.withValues(10, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L), journal);
        assertEquals(1, journal.keySet().size());
        assertNothingReported(errors, mismatches);
        assertCompared(tokens);
    }

    @Test
    public void recordHighestSeenPartitionWhenTasksCompleteOutOfOrderWithErrors() {
        // 2 partitions will error during comparison, but after 10 successful comparisons
        // we'll still report progress to the journal
        RuntimeException toThrow = new RuntimeException("Test");
        RangeComparator comparator = comparator(context(0L, 100L));
        long[] tokens = new long[] {2, 8, 1, 11, 4, 100, 3, 5, 7, 6, 9, 0};
        comparator.compare(keys(tokens), keys(tokens), throwDuringExecution(toThrow, 3, 2));
        assertReported(100, RangeStats.withValues(10, 0L, 2L, 0L, 0L, 0L, 0L, 0L, 0L), journal);
        assertEquals(1, journal.keySet().size());
        assertReported(2, toThrow, errors);
        assertReported(3, toThrow, errors);
        assertEquals(2, errors.keySet().size());
        assertNothingReported(mismatches);
        // the erroring tasks don't get counted in the test as compared
        assertCompared(8, 1, 11, 4, 100, 5, 7, 6, 9, 0);
    }

    @Test
    public void rowLevelMismatchIncrementsPartitionMismatches() {
        RangeComparator comparator = comparator(context(0L, 100L));
        RangeStats stats = comparator.compare(keys(0, 1, 2), keys(0, 1 ,2), this::rowMismatch);
        assertEquals(0, stats.getMatchedPartitions());
        assertEquals(3, stats.getMismatchedPartitions());
        assertEquals(0, stats.getMatchedValues());
        assertEquals(0, stats.getMismatchedValues());
        assertNothingReported(errors, journal);
        assertReported(0, MismatchType.PARTITION_MISMATCH, mismatches);
        assertReported(1, MismatchType.PARTITION_MISMATCH, mismatches);
        assertReported(2, MismatchType.PARTITION_MISMATCH, mismatches);
        assertCompared(0, 1, 2);
    }

    @Test
    public void valueMismatchIncrementsPartitionMismatches() {
        RangeComparator comparator = comparator(context(0L, 100L));
        RangeStats stats = comparator.compare(keys(0, 1, 2), keys(0, 1 ,2), this::valuesMismatch);
        assertEquals(0, stats.getMatchedPartitions());
        assertEquals(3, stats.getMismatchedPartitions());
        assertEquals(0, stats.getMatchedValues());
        assertEquals(30, stats.getMismatchedValues());
        assertNothingReported(errors, journal);
        assertReported(0, MismatchType.PARTITION_MISMATCH, mismatches);
        assertReported(1, MismatchType.PARTITION_MISMATCH, mismatches);
        assertReported(2, MismatchType.PARTITION_MISMATCH, mismatches);
        assertCompared(0, 1, 2);
    }

    @Test
    public void matchingPartitionIncrementsCount() {
        RangeComparator comparator = comparator(context(0L, 100L));
        RangeStats stats = comparator.compare(keys(0, 1, 2), keys(0, 1 ,2), this::alwaysMatch);
        assertEquals(3, stats.getMatchedPartitions());
        assertEquals(0, stats.getMismatchedPartitions());
        assertEquals(0, stats.getMatchedValues());
        assertEquals(0, stats.getMismatchedValues());
        assertNothingReported(errors, mismatches, journal);
        assertCompared(0, 1, 2);
    }

    @Test
    public void waitForAllInFlightTasksToComplete() throws InterruptedException {
        CountDownLatch taskSubmissions = new CountDownLatch(2);
        List<CountDownLatch> taskGates = Lists.newArrayList(new CountDownLatch(1), new CountDownLatch(1));
        RangeComparator comparator = comparator(context(0L, 100L));

        Thread t = new Thread(() -> comparator.compare(keys(0, 1),
                                                       keys(0, 1),
                                                       waitUntilNotified(taskSubmissions, taskGates)),
                              "CallingThread");
        t.setDaemon(true);
        t.start();

        // wait for both tasks to be submitted then check that the calling thread enters a waiting state
        taskSubmissions.await();
        assertThreadWaits(t);

        // let the first task complete and check that the calling thread is still waiting
        taskGates.get(0).countDown();
        assertThreadWaits(t);

        // let the second task run and wait for the caller to terminate
        taskGates.get(1).countDown();
        t.join();

        assertNothingReported(errors, mismatches, journal);
        assertCompared(0, 1);
    }

    private RangeStats testTaskError(Function<PartitionKey, PartitionComparator> taskSupplier) {
        RangeComparator comparator = comparator(context(0L, 100L));
        Iterator<PartitionKey> sourceKeys = keys(0, 1, 2);
        Iterator<PartitionKey> targetKeys = keys(0, 1, 2);
        RangeStats stats = comparator.compare(sourceKeys, targetKeys, taskSupplier);
        assertNothingReported(mismatches, journal);
        return stats;
    }

    private void testErrorReadingKey(Iterator<PartitionKey> sourceKeys,
                                     Iterator<PartitionKey> targetKeys,
                                     Exception expected) {

        RangeComparator comparator = comparator(context(0L, 100L));
        try {
            comparator.compare(sourceKeys, targetKeys, this::alwaysMatch);
            fail("Expected exception " + expected.getLocalizedMessage());
        } catch (Exception e) {
            assertEquals(expected, e.getCause());
        }
        assertNothingReported(errors,  mismatches, journal);
    }

    private void assertCompared(long...tokens) {
        assertEquals(comparedPartitions.size(), tokens.length);
        for(long t : tokens)
            assertTrue(comparedPartitions.contains(BigInteger.valueOf(t)));
    }

    private void assertNothingReported(Multimap...reported) {
        for (Multimap m : reported)
            assertTrue(m.isEmpty());
    }

    private <T> void assertReported(long token, T expected, Multimap<BigInteger, T> reported) {
        Collection<T> values = reported.get(BigInteger.valueOf(token));
        assertEquals(1, values.size());
        assertEquals(expected, values.iterator().next());
    }

    Iterator<PartitionKey> keys(long throwAtToken, RuntimeException e, long...tokens) {
        return new AbstractIterator<PartitionKey>() {
            int i = 0;
            protected PartitionKey computeNext() {
                if (i < tokens.length) {
                    long t = tokens[i++];
                    if (t == throwAtToken)
                        throw e;

                    return key(t);
                }
                return endOfData();
            }
        };
    }

    Iterator<PartitionKey> keys(long...tokens) {
        return new AbstractIterator<PartitionKey>() {
            int i = 0;
            protected PartitionKey computeNext() {
                if (i < tokens.length)
                    return key(tokens[i++]);
                return endOfData();
            }
        };
    }

    // yield a PartitionComparator which always concludes that partitions being compared are identical
    PartitionComparator alwaysMatch(PartitionKey key) {
        return new PartitionComparator(null, null, null, mockRetryStrategyFactory) {
            public PartitionStats call() {
                comparedPartitions.add(key.getTokenAsBigInteger());
                return new PartitionStats();
            }
        };
    }

    // yield a PartitionComparator which always determines that the partitions have a row-level mismatch
    PartitionComparator rowMismatch(PartitionKey key) {
        return new PartitionComparator(null, null,  null, mockRetryStrategyFactory) {
            public PartitionStats call() {
                comparedPartitions.add(key.getTokenAsBigInteger());
                PartitionStats stats = new PartitionStats();
                stats.allClusteringsMatch = false;
                return stats;
            }
        };
    }

    // yield a PartitionComparator which always determines that the partitions have a 10 mismatching values
    PartitionComparator valuesMismatch(PartitionKey key) {
        return new PartitionComparator(null, null,  null, mockRetryStrategyFactory) {
            public PartitionStats call() {
                comparedPartitions.add(key.getTokenAsBigInteger());
                PartitionStats stats = new PartitionStats();
                stats.mismatchedValues = 10;
                return stats;
            }
        };
    }

    // yield a function which throws when creating a PartitionComparator for a give token
    // simulates an error reading the source or target partition from the source or target
    // cluster when constructing the task
    Function<PartitionKey, PartitionComparator> throwDuringConstruction(long throwAt, RuntimeException toThrow) {
        return (key) -> {
            BigInteger t = key.getTokenAsBigInteger();
            if (t.longValue() == throwAt)
                throw toThrow;

            return alwaysMatch(key);
        };
    }

    // yields a PartitionComparator which throws the supplied exception when the token matches
    // the one specified to simulate an error when processing the comparison
    Function<PartitionKey, PartitionComparator> throwDuringExecution(RuntimeException toThrow, long...throwAt) {
        return (key) -> {
            BigInteger t = key.getTokenAsBigInteger();
            return new PartitionComparator(null, null, null, mockRetryStrategyFactory) {
                public PartitionStats call() {
                    for (long shouldThrow : throwAt)
                        if (t.longValue() == shouldThrow)
                           throw toThrow;

                    comparedPartitions.add(t);
                    return new PartitionStats();
                }
            };
        };
    }

    // yields a PartitionComparator which waits on a CountDownLatch before returning from call(). The latches
    // are supplied in an iterator and each successive task yielded uses the next supplied latch so that callers
    // can control the rate of task progress
    // The other supplied latch, firstTaskStarted, is used to signal to the caller that execution of the first
    // task has started, so the test doesn't complete before this happens
    Function<PartitionKey, PartitionComparator> waitUntilNotified(final CountDownLatch taskSubmissions,
                                                                  final List<CountDownLatch> taskGates) {
        final Iterator<CountDownLatch> gateIter = taskGates.iterator();
        return (key) -> {

            BigInteger t = key.getTokenAsBigInteger();
            taskSubmissions.countDown();

            return new PartitionComparator(null, null, null, mockRetryStrategyFactory) {
                public PartitionStats call() {
                    if (!gateIter.hasNext())
                        fail("Expected a latch to control task progress");

                    try {
                        gateIter.next().await();
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                        fail("Interrupted");
                    }
                    comparedPartitions.add(t);
                    return new PartitionStats();
                }
            };
        };
    }

    PartitionKey key(long token) {
        return new TestPartitionKey(token);
    }

    RangeComparator comparator(DiffContext context) {
        return new RangeComparator(context, errorReporter, mismatchReporter, progressReporter, executor);
    }

    DiffContext context(long startToken, long endToken, long...disallowedTokens) {
        return new TestContext(BigInteger.valueOf(startToken),
                               BigInteger.valueOf(endToken),
                               new SpecificTokens(Arrays.stream(disallowedTokens)
                                                        .mapToObj(BigInteger::valueOf)
                                                        .collect(Collectors.toSet()),
                                                  SpecificTokens.Modifier.REJECT));
    }

    DiffContext context(long startToken, long endToken) {
        return new TestContext(BigInteger.valueOf(startToken), BigInteger.valueOf(endToken), SpecificTokens.NONE);
    }

    RangeStats copyOf(RangeStats stats) {
        return RangeStats.withValues(stats.getMatchedPartitions(),
                                     stats.getMismatchedPartitions(),
                                     stats.getErrorPartitions(),
                                     stats.getSkippedPartitions(),
                                     stats.getOnlyInSource(),
                                     stats.getOnlyInTarget(),
                                     stats.getMatchedRows(),
                                     stats.getMatchedValues(),
                                     stats.getMismatchedValues());
    }

    static class TestPartitionKey extends PartitionKey {
        final Token token;

        TestPartitionKey(final long tokenValue) {
            super(null);
            token = new Token() {

                public DataType getType() {
                    return DataType.bigint();
                }

                public Object getValue() {
                    return tokenValue;
                }

                public ByteBuffer serialize(ProtocolVersion protocolVersion) {
                    return null;
                }

                public int compareTo(@NotNull Token o) {
                    assert o.getValue() instanceof Long;
                    return Long.compare(tokenValue, (long)o.getValue());
                }
            };
        }

        protected Token getToken() {
            return token;
        }
    }

    static class TestContext extends DiffContext {

        public TestContext(BigInteger startToken,
                           BigInteger endToken,
                           SpecificTokens specificTokens) {
            super(null, null, null, null, startToken, endToken, specificTokens, 0.0);
        }
    }
}
