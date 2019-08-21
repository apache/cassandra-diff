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

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.math.BigInteger;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.function.BiConsumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Verify;
import com.google.common.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public class Differ implements Serializable
{
    private static final Logger logger = LoggerFactory.getLogger(Differ.class);

    private static final MetricRegistry metrics = new MetricRegistry();

    private static final int COMPARISON_THREADS = 8;
    private static final ComparisonExecutor COMPARISON_EXECUTOR = ComparisonExecutor.newExecutor(COMPARISON_THREADS, metrics);

    private final UUID jobId;
    private final DiffJob.Split split;
    private final TokenHelper tokenHelper;
    private final String keyspace;
    private final List<String> tables;
    private final RateLimiter rateLimiter;
    private final DiffJob.TrackerProvider trackerProvider;
    private final double reverseReadProbability;
    private final SpecificTokens specificTokens;

    private static DiffCluster srcDiffCluster;
    private static DiffCluster targetDiffCluster;
    private static Session journalSession;

    static
    {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            StringWriter stackTrace = new StringWriter();
            e.printStackTrace(new PrintWriter(stackTrace));
            System.out.println("UNCAUGHT EXCEPTION: " + stackTrace.toString());
            throw new RuntimeException(e);
        });
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("In shutdown hook");
            shutdown();
        }));
    }

    public Differ(JobConfiguration config,
                  DiffJob.Params params,
                  int perExecutorRateLimit,
                  DiffJob.Split split,
                  TokenHelper tokenHelper,
                  ClusterProvider sourceProvider,
                  ClusterProvider targetProvider,
                  ClusterProvider metadataProvider,
                  DiffJob.TrackerProvider trackerProvider)
    {
        logger.info("Creating Differ for {}", split);
        this.jobId = params.jobId;
        this.split = split;
        this.tokenHelper = tokenHelper;
        this.keyspace = params.keyspace;
        this.tables = params.tables;
        this.trackerProvider = trackerProvider;
        rateLimiter = RateLimiter.create(perExecutorRateLimit);
        this.reverseReadProbability = config.reverseReadProbability();
        this.specificTokens = config.specificTokens();
        synchronized (Differ.class)
        {
            /*
            Spark runs jobs on each worker in the same JVM, we need to initialize these only once, otherwise
            we run OOM with health checker threads
             */
            // yes we could have JobConfiguration return this directly, but snakeyaml doesn't like relocated classes and the driver has to be shaded
            ConsistencyLevel cl = ConsistencyLevel.valueOf(config.consistencyLevel());
            if (srcDiffCluster == null)
            {
                srcDiffCluster = new DiffCluster(DiffCluster.Type.SOURCE,
                                                 sourceProvider.getCluster(),
                                                 params.keyspace,
                                                 cl,
                                                 rateLimiter,
                                                 config.tokenScanFetchSize(),
                                                 config.partitionReadFetchSize(),
                                                 config.readTimeoutMillis());
            }

            if (targetDiffCluster == null)
            {
                targetDiffCluster = new DiffCluster(DiffCluster.Type.TARGET,
                                                    targetProvider.getCluster(),
                                                    params.keyspace,
                                                    cl,
                                                    rateLimiter,
                                                    config.tokenScanFetchSize(),
                                                    config.partitionReadFetchSize(),
                                                    config.readTimeoutMillis());
            }

            if (journalSession == null)
            {
                journalSession = metadataProvider.getCluster().connect();
                trackerProvider.initializeStatements(journalSession);
            }
        }
    }

    public Map<String, RangeStats> run() {
        JobMetadataDb.ProgressTracker journal = trackerProvider.getTracker(journalSession, jobId, split);
        Map<String, DiffJob.TaskStatus> tablesToDiff = filterTables(tables,
                                                                    split,
                                                                    journal::getLastStatus,
                                                                    !specificTokens.isEmpty());

        String metricsPrefix = String.format("%s.%s", srcDiffCluster.clusterId.name(), srcDiffCluster.keyspace);
        logger.info("Diffing {} for tables {}", split, tablesToDiff);

        for (Map.Entry<String, DiffJob.TaskStatus> tableStatus : tablesToDiff.entrySet()) {
            final String table = tableStatus.getKey();
            DiffJob.TaskStatus status = tableStatus.getValue();
            RangeStats diffStats = status.stats;

            // if this split has already been fully processed, it's being re-run to check
            // partitions with errors. In that case, we don't want to adjust the split
            // start and we don't want to update the completed count when we're finished.
            boolean isRerun = split.end.equals(status.lastToken);
            BigInteger startToken = status.lastToken == null || isRerun ? split.start : status.lastToken;
            validateRange(startToken, split.end, tokenHelper);

            TableSpec sourceTable = TableSpec.make(table, srcDiffCluster);
            TableSpec targetTable = TableSpec.make(table, targetDiffCluster);
            validateTableSpecs(sourceTable, targetTable);

            DiffContext ctx = new DiffContext(srcDiffCluster,
                                              targetDiffCluster,
                                              keyspace,
                                              sourceTable,
                                              startToken,
                                              split.end,
                                              specificTokens,
                                              reverseReadProbability);

            String timerName = String.format("%s.%s.split_times", metricsPrefix, table);
            try (@SuppressWarnings("unused") Timer.Context timer = metrics.timer(timerName).time()) {
                diffStats.accumulate(diffTable(ctx,
                                               (error, token) -> journal.recordError(table, token, error),
                                               (type, token) -> journal.recordMismatch(table, type, token),
                                               (stats, token) -> journal.updateStatus(table, stats, token)));

                // update the journal with the final state for the table. Use the split's ending token
                // as the last seen token (even though we may not have actually read any partition for
                // that token) as this effectively marks the split as done.
                journal.finishTable(table, diffStats, !isRerun);
            }
        }

        Map<String, RangeStats> statsByTable = tablesToDiff.entrySet()
                                                           .stream()
                                                           .collect(Collectors.toMap(Map.Entry::getKey,
                                                                                     e -> e.getValue().stats));
        updateMetrics(metricsPrefix, statsByTable);
        return statsByTable;
    }

    public RangeStats diffTable(final DiffContext context,
                                final BiConsumer<Throwable, BigInteger> partitionErrorReporter,
                                final BiConsumer<MismatchType, BigInteger> mismatchReporter,
                                final BiConsumer<RangeStats, BigInteger> journal) {

        final Iterator<PartitionKey> sourceKeys = context.source.getPartitionKeys(context.table.getTable(),
                                                                                  context.startToken,
                                                                                  context.endToken);
        final Iterator<PartitionKey> targetKeys = context.target.getPartitionKeys(context.table.getTable(),
                                                                                  context.startToken,
                                                                                  context.endToken);
        final Function<PartitionKey, PartitionComparator> partitionTaskProvider =
            (key) -> {
                boolean reverse = context.shouldReverse();
                return new PartitionComparator(context.table,
                                               context.source.getPartition(context.table, key, reverse),
                                               context.target.getPartition(context.table, key, reverse));
            };

        RangeComparator rangeComparator = new RangeComparator(context,
                                                              partitionErrorReporter,
                                                              mismatchReporter,
                                                              journal,
                                                              COMPARISON_EXECUTOR);

        final RangeStats tableStats = rangeComparator.compare(sourceKeys, targetKeys, partitionTaskProvider);
        logger.debug("Table [{}] stats - ({})", context.table.getTable(), tableStats);
        return tableStats;
    }

    @VisibleForTesting
    static Map<String, DiffJob.TaskStatus> filterTables(Iterable<String> tables,
                                                        DiffJob.Split split,
                                                        Function<String, DiffJob.TaskStatus> journal,
                                                        boolean includeCompleted) {
        Map<String, DiffJob.TaskStatus> tablesToProcess = new HashMap<>();
        for (String table : tables) {
            DiffJob.TaskStatus taskStatus = journal.apply(table);
            RangeStats diffStats = taskStatus.stats;
            BigInteger lastToken = taskStatus.lastToken;

            // When we finish processing a split for a given table, we update the task status in journal
            // to set the last seen token to the split's end token, to indicate that the split is complete.
            if (!includeCompleted && lastToken != null && lastToken.equals(split.end)) {
                logger.info("Found finished table {} for split {}", table, split);
            }
            else {
                tablesToProcess.put(table, diffStats != null
                                            ? taskStatus
                                            : new DiffJob.TaskStatus(taskStatus.lastToken, RangeStats.newStats()));
            }
        }
        return tablesToProcess;
    }

    static void validateTableSpecs(TableSpec source, TableSpec target) {
        Verify.verify(source.equalsNamesOnly(target),
                      "Source and target table definitions do not match (Source: %s Target: %s)",
                      source, target);
    }

    @VisibleForTesting
    static void validateRange(BigInteger start, BigInteger end, TokenHelper tokens) {

        Verify.verify(start != null && end != null, "Invalid token range [%s,%s]", start, end);

        Verify.verify(start.compareTo(tokens.min()) >= 0 && end.compareTo(tokens.max()) <= 0 && start.compareTo(end) < 0,
                      "Invalid token range [%s,%s] for partitioner range [%s,%s]",
                       start, end, tokens.min(), tokens.max());
    }

    @VisibleForTesting
    static Map<String, RangeStats> accumulate(Map<String, RangeStats> stats, Map<String, RangeStats> otherStats)
    {
        for (Map.Entry<String, RangeStats> otherEntry : otherStats.entrySet())
        {
            if (stats.containsKey(otherEntry.getKey()))
                stats.get(otherEntry.getKey()).accumulate(otherEntry.getValue());
            else
                stats.put(otherEntry.getKey(), otherEntry.getValue());
        }
        return stats;
    }

    private static void updateMetrics(String prefix, Map<String, RangeStats> statsMap)
    {
        for (Map.Entry<String, RangeStats> entry : statsMap.entrySet())
        {
            String qualifier = String.format("%s.%s", prefix, entry.getKey());
            RangeStats stats = entry.getValue();

            metrics.meter(qualifier + ".partitions_read").mark(stats.getMatchedPartitions() + stats.getOnlyInSource() + stats.getOnlyInTarget() + stats.getMismatchedPartitions());
            metrics.counter(qualifier + ".matched_partitions").inc(stats.getMatchedPartitions());
            metrics.counter(qualifier + ".mismatched_partitions").inc(stats.getMismatchedPartitions());

            metrics.counter(qualifier + ".partitions_only_in_source").inc(stats.getOnlyInSource());
            metrics.counter(qualifier + ".partitions_only_in_target").inc(stats.getOnlyInTarget());
            metrics.counter(qualifier + ".skipped_partitions").inc(stats.getSkippedPartitions());

            metrics.counter(qualifier + ".matched_rows").inc(stats.getMatchedRows());
            metrics.counter(qualifier + ".matched_values").inc(stats.getMatchedValues());
            metrics.counter(qualifier + ".mismatched_values").inc(stats.getMismatchedValues());
        }
    }

    public static void shutdown()
    {
        try
        {
            if (srcDiffCluster != null) {
                srcDiffCluster.stop();
                srcDiffCluster.close();
            }
            if (targetDiffCluster != null) {
                targetDiffCluster.stop();
                targetDiffCluster.close();
            }
            if (journalSession != null) {
                journalSession.close();
                journalSession.getCluster().close();
            }
            COMPARISON_EXECUTOR.shutdown();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
