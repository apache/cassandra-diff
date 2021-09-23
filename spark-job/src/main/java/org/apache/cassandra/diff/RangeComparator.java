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
import java.util.Iterator;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.common.base.Verify;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangeComparator {

    private static final Logger logger = LoggerFactory.getLogger(RangeComparator.class);

    private final DiffContext context;
    private final BiConsumer<Throwable, BigInteger> errorReporter;
    private final BiConsumer<MismatchType, BigInteger> mismatchReporter;
    private final BiConsumer<RangeStats, BigInteger> journal;
    private final ComparisonExecutor comparisonExecutor;

    public RangeComparator(DiffContext context,
                           BiConsumer<Throwable, BigInteger> errorReporter,
                           BiConsumer<MismatchType,BigInteger> mismatchReporter,
                           BiConsumer<RangeStats, BigInteger> journal,
                           ComparisonExecutor comparisonExecutor) {
        this.context = context;
        this.errorReporter = errorReporter;
        this.mismatchReporter = mismatchReporter;
        this.journal = journal;
        this.comparisonExecutor = comparisonExecutor;
    }

    public RangeStats compare(Iterator<PartitionKey> sourceKeys,
                              Iterator<PartitionKey> targetKeys,
                              Function<PartitionKey, PartitionComparator> partitionTaskProvider) {
        return compare(sourceKeys,targetKeys,partitionTaskProvider, partitionKey -> true);
    }

    /**
     * Compares partitions in src and target clusters.
     *
     * @param sourceKeys partition keys in the source cluster
     * @param targetKeys partition keys in the target cluster
     * @param partitionTaskProvider comparision task
     * @param partitionSampler samples partitions based on the probability for probabilistic diff
     * @return stats about the diff
     */
    public RangeStats compare(Iterator<PartitionKey> sourceKeys,
                              Iterator<PartitionKey> targetKeys,
                              Function<PartitionKey, PartitionComparator> partitionTaskProvider,
                              Predicate<PartitionKey> partitionSampler) {

        final RangeStats rangeStats = RangeStats.newStats();
        // We can catch this condition earlier, but it doesn't hurt to also check here
        if (context.startToken.equals(context.endToken))
            return rangeStats;

        Phaser phaser = new Phaser(1);
        AtomicLong partitionCount = new AtomicLong(0);
        AtomicReference<BigInteger> highestTokenSeen = new AtomicReference<>(context.startToken);

        logger.info("Comparing range [{},{}]", context.startToken, context.endToken);
        try {
            PartitionKey sourceKey = nextKey(sourceKeys);
            PartitionKey targetKey = nextKey(targetKeys);

            // special case for start of range - handles one cluster supplying an empty range
            if ((sourceKey == null) != (targetKey == null)) {
                if (sourceKey == null) {
                    logger.info("First in range, source iter is empty {}", context);
                    onlyInTarget(rangeStats, targetKey);
                    targetKeys.forEachRemaining(key -> onlyInTarget(rangeStats, key));
                } else {
                    logger.info("First in range, target iter is empty {}", context);
                    onlyInSource(rangeStats, sourceKey);
                    sourceKeys.forEachRemaining(key -> onlyInSource(rangeStats, key));
                }
                return rangeStats;
            }

            while (sourceKey != null && targetKey != null) {

                int ret = sourceKey.compareTo(targetKey);
                if (ret > 0) {
                    onlyInTarget(rangeStats, targetKey);
                    targetKey = nextKey(targetKeys);
                } else if (ret < 0) {
                    onlyInSource(rangeStats, sourceKey);
                    sourceKey = nextKey(sourceKeys);
                } else {

                    Verify.verify(sourceKey.equals(targetKey),
                                  "Can only compare partitions with identical keys: (%s, %s)",
                                  sourceKey, targetKey);

                    // For results where the key exists in both, we'll fire off an async task to walk the
                    // partition and compare all the rows. The result of that comparison is added to the
                    // totals for the range and the highest seen token updated in the onSuccess callback

                    if (!context.isTokenAllowed(sourceKey.getTokenAsBigInteger())) {
                        logger.debug("Skipping disallowed token {}", sourceKey.getTokenAsBigInteger());
                        rangeStats.skipPartition();
                        sourceKey = nextKey(sourceKeys);
                        targetKey = nextKey(targetKeys);
                        continue;
                    }

                    BigInteger token = sourceKey.getTokenAsBigInteger();
                    try {
                        // Use probabilisticPartitionSampler for sampling partitions, skip partition
                        // if the sampler returns false otherwise run diff on that partition
                        if (partitionSampler.test(sourceKey)) {
                            PartitionComparator comparisonTask = partitionTaskProvider.apply(sourceKey);
                            comparisonExecutor.submit(comparisonTask,
                                                      onSuccess(rangeStats, partitionCount, token, highestTokenSeen, mismatchReporter, journal),
                                                      onError(rangeStats, token, errorReporter),
                                                      phaser);
                        }

                    } catch (Throwable t) {
                        // Handle errors thrown when creating the comparison task. This should trap timeouts and
                        // unavailables occurring when performing the initial query to read the full partition.
                        // Errors thrown when paging through the partition in comparisonTask will be handled by
                        // the onError callback.
                        recordError(rangeStats, token, errorReporter, t);
                    } finally {
                        // if the cluster has been shutdown because the task failed the underlying iterators
                        // of partition keys will return hasNext == false
                        sourceKey = nextKey(sourceKeys);
                        targetKey = nextKey(targetKeys);
                    }
                }
            }

            // handle case where only one iterator is exhausted
            if (sourceKey != null)
                onlyInSource(rangeStats, sourceKey);
            else if (targetKey != null)
                onlyInTarget(rangeStats, targetKey);

            drain(sourceKeys, targetKeys, rangeStats);

        } catch (Exception e) {
            // Handles errors thrown by iteration of underlying resultsets of partition keys by
            // calls to nextKey(). Such errors should cause the overall range comparison to fail,
            // but we must ensure that any in-flight partition comparisons complete so that either
            // the onSuccess or onError callback is fired for each one. This is necessary to ensure
            // that we record the highest seen token and any failed partitions and can safely re-run.
            logger.debug("Waiting for {} in flight tasks before propagating error", phaser.getUnarrivedParties());
            phaser.arriveAndAwaitAdvance();
            throw new RuntimeException(String.format("Error encountered during range comparison for [%s:%s]",
                                       context.startToken, context.endToken), e);
        }

        logger.debug("Waiting for {} in flight tasks before returning", phaser.getUnarrivedParties());
        phaser.arriveAndAwaitAdvance();

        if (!rangeStats.allMatches())
            logger.info("Segment [{}:{}] stats - ({})", context.startToken, context.endToken, rangeStats);

        return rangeStats;
    }

    private void drain(Iterator<PartitionKey> sourceKeys,
                             Iterator<PartitionKey> targetKeys,
                             RangeStats rangeStats) {
        if (sourceKeys.hasNext()) {
            logger.info("Source keys not exhausted {}", context);
            sourceKeys.forEachRemaining(key -> onlyInSource(rangeStats, key));
        } else if (targetKeys.hasNext()) {
            logger.info("Target keys not exhausted: {}", context);
            targetKeys.forEachRemaining(key -> onlyInTarget(rangeStats, key));
        }
    }

    private void onlyInTarget(RangeStats stats, PartitionKey key) {
        stats.onlyInTarget();
        mismatchReporter.accept(MismatchType.ONLY_IN_TARGET, key.getTokenAsBigInteger());
    }

    private void onlyInSource(RangeStats stats, PartitionKey key) {
        stats.onlyInSource();
        mismatchReporter.accept(MismatchType.ONLY_IN_SOURCE, key.getTokenAsBigInteger());
    }

    private PartitionKey nextKey(Iterator<PartitionKey> keys) {
        return keys.hasNext() ? keys.next() : null;
    }

    private Consumer<PartitionStats> onSuccess(final RangeStats rangeStats,
                                               final AtomicLong partitionCount,
                                               final BigInteger currentToken,
                                               final AtomicReference<BigInteger> highestSeenToken,
                                               final BiConsumer<MismatchType, BigInteger> mismatchReporter,
                                               final BiConsumer<RangeStats, BigInteger> journal) {
        return (result) -> {

            rangeStats.accumulate(result);
            if (!result.allClusteringsMatch || result.mismatchedValues > 0) {
                mismatchReporter.accept(MismatchType.PARTITION_MISMATCH, currentToken);
                rangeStats.mismatchedPartition();
            } else {
                rangeStats.matchedPartition();
            }

            BigInteger highest = highestSeenToken.get();
            while (currentToken.compareTo(highest) > 0) {
                if (highestSeenToken.compareAndSet(highest, currentToken))
                    break;

                highest = highestSeenToken.get();
            }

            // checkpoint ever 10 partitions
            if (partitionCount.incrementAndGet() % 10 == 0)
                journal.accept(rangeStats, highestSeenToken.get());
        };
    }

    private Consumer<Throwable> onError(final RangeStats rangeStats,
                                        final BigInteger currentToken,
                                        final BiConsumer<Throwable, BigInteger> errorReporter) {
        return (error) -> recordError(rangeStats, currentToken, errorReporter, error);
    }

    private void recordError(final RangeStats rangeStats,
                             final BigInteger currentToken,
                             final BiConsumer<Throwable, BigInteger> errorReporter,
                             final Throwable error) {
        rangeStats.partitionError();
        errorReporter.accept(error, currentToken);
    }
}


