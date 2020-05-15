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

package org.apache.cassandra.diff.api.services;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.diff.ClusterProvider;
import org.apache.cassandra.diff.JobConfiguration;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;

// TODO cache jobsummary
// TODO fix exception handling
public class DBService implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(DBService.class);
    private static final long QUERY_TIMEOUT_MS = 3000;

    private final Cluster cluster;
    private final Session session;
    private final String diffKeyspace;

    private final PreparedStatement runningJobsStatement;
    private final PreparedStatement jobSummaryStatement;
    private final PreparedStatement jobResultStatement;
    private final PreparedStatement jobStatusStatement;
    private final PreparedStatement jobMismatchesStatement;
    private final PreparedStatement jobErrorSummaryStatement;
    private final PreparedStatement jobErrorRangesStatement;
    private final PreparedStatement jobErrorDetailStatement;
    private final PreparedStatement jobsStartDateStatement;
    private final PreparedStatement jobsForSourceStatement;
    private final PreparedStatement jobsForTargetStatement;
    private final PreparedStatement jobsForKeyspaceStatement;

    public DBService(JobConfiguration config) {
        logger.info("Initializing DBService");
        ClusterProvider provider = ClusterProvider.getProvider(config.clusterConfig("metadata"), "metadata");
        cluster = provider.getCluster();

        session = cluster.connect();
        diffKeyspace = config.metadataOptions().keyspace;
        runningJobsStatement = session.prepare(String.format(
            " SELECT job_id " +
            " FROM %s.running_jobs", diffKeyspace));
        jobSummaryStatement = session.prepare(String.format(
            " SELECT " +
            "   job_id," +
            "   job_start_time," +
            "   buckets," +
            "   qualified_table_names, " +
            "   source_cluster_name," +
            "   source_cluster_desc," +
            "   target_cluster_name," +
            "   target_cluster_desc," +
            "   total_tasks " +
            " FROM %s.job_summary" +
            " WHERE job_id = ?", diffKeyspace));
        jobResultStatement = session.prepare(String.format(
            " SELECT " +
            "   job_id," +
            "   qualified_table_name, " +
            "   matched_partitions," +
            "   mismatched_partitions,"   +
            "   matched_rows,"   +
            "   matched_values,"   +
            "   mismatched_values,"   +
            "   partitions_only_in_source,"   +
            "   partitions_only_in_target,"   +
            "   skipped_partitions"   +
            " FROM %s.job_results" +
            " WHERE job_id = ? AND qualified_table_name = ?", diffKeyspace));
        jobStatusStatement = session.prepare(String.format(
            " SELECT " +
            "   job_id,"   +
            "   bucket,"   +
            "   qualified_table_name,"   +
            "   completed "   +
            " FROM %s.job_status" +
            " WHERE job_id = ? AND bucket = ?", diffKeyspace));
        jobMismatchesStatement = session.prepare(String.format(
            " SELECT " +
            "   job_id," +
            "   bucket," +
            "   qualified_table_name," +
            "   mismatching_token," +
            "   mismatch_type" +
            " FROM %s.mismatches" +
            " WHERE job_id = ? AND bucket = ?", diffKeyspace));
        jobErrorSummaryStatement = session.prepare(String.format(
            " SELECT " +
            "   count(start_token) AS error_count," +
            "   qualified_table_name" +
            " FROM %s.task_errors" +
            " WHERE job_id = ? AND bucket = ?",
            diffKeyspace));
        jobErrorRangesStatement = session.prepare(String.format(
            " SELECT " +
            "   bucket,"   +
            "   qualified_table_name,"   +
            "   start_token,"   +
            "   end_token"   +
            " FROM %s.task_errors" +
            " WHERE job_id = ? AND bucket = ?",
            diffKeyspace));
        jobErrorDetailStatement = session.prepare(String.format(
            " SELECT " +
            "   qualified_table_name,"   +
            "   error_token"   +
            " FROM %s.partition_errors" +
            " WHERE job_id = ? AND bucket = ? AND qualified_table_name = ? AND start_token = ? AND end_token = ?", diffKeyspace));
        jobsStartDateStatement = session.prepare(String.format(
            " SELECT " +
            "   job_id" +
            " FROM %s.job_start_index" +
            " WHERE job_start_date = ? AND job_start_hour = ?", diffKeyspace));
        jobsForSourceStatement = session.prepare(String.format(
            " SELECT " +
            "   job_id" +
            " FROM %s.source_cluster_index" +
            " WHERE source_cluster_name = ?", diffKeyspace));
        jobsForTargetStatement = session.prepare(String.format(
            " SELECT " +
            "   job_id" +
            " FROM %s.target_cluster_index" +
            " WHERE target_cluster_name = ?", diffKeyspace));
        jobsForKeyspaceStatement = session.prepare(String.format(
            " SELECT " +
            "   job_id" +
            " FROM %s.keyspace_index" +
            " WHERE keyspace_name = ?", diffKeyspace));
    }

    public List<UUID> fetchRunningJobs() {
        List<UUID> jobs = new ArrayList<>();
        ResultSet rs = session.execute(runningJobsStatement.bind());
        rs.forEach(row -> jobs.add(row.getUUID("job_id")));
        return jobs;
    }

    public Collection<JobSummary> fetchJobSummaries(List<UUID> jobIds) {
        List<ResultSetFuture> futures = Lists.newArrayListWithCapacity(jobIds.size());
        jobIds.forEach(id -> futures.add(session.executeAsync(jobSummaryStatement.bind(id))));

        // Oldest first
        SortedSet<JobSummary> summaries = Sets.newTreeSet(JobSummary.COMPARATOR);
        processFutures(futures, JobSummary::fromRow, summaries::add);
        return summaries;
    }

    public JobSummary fetchJobSummary(UUID jobId) {
        Row row = session.execute(jobSummaryStatement.bind(jobId)).one();
        if (row == null)
            throw new RuntimeException(String.format("Job %s not found", jobId));
        return JobSummary.fromRow(row);
    }

    public Collection<JobResult> fetchJobResults(UUID jobId) {
        JobSummary summary = fetchJobSummary(jobId);
        List<ResultSetFuture> futures = Lists.newArrayListWithCapacity(summary.keyspaceTables.size());
        for (String table : summary.keyspaceTables)
            futures.add(session.executeAsync(jobResultStatement.bind(jobId, table)));

        SortedSet<JobResult> results = Sets.newTreeSet();
        processFutures(futures, JobResult::fromRow, results::add);
        return results;
    }

    public JobStatus fetchJobStatus(UUID jobId) {
        JobSummary summary = fetchJobSummary(jobId);
        List<ResultSetFuture> futures = Lists.newArrayListWithCapacity(summary.buckets);

        for (int i = 0; i < summary.buckets; i++)
            futures.add(session.executeAsync(jobStatusStatement.bind(jobId, i)));

        Map<String, Long> completedByTable = Maps.newHashMapWithExpectedSize(summary.keyspaceTables.size());
        processFutures(futures, row -> completedByTable.merge(row.getString("qualified_table_name"),
                                                              row.getLong("completed"),
                                                              Long::sum));
        return new JobStatus(jobId, completedByTable);
    }

    public JobMismatches fetchMismatches(UUID jobId) {
        JobSummary summary = fetchJobSummary(jobId);
        List<ResultSetFuture> futures = Lists.newArrayListWithCapacity(summary.buckets);

        for (int i = 0; i < summary.buckets; i++)
            futures.add(session.executeAsync(jobMismatchesStatement.bind(jobId, i)));

        Map<String, List<Mismatch>> mismatchesByTable = Maps.newHashMapWithExpectedSize(summary.keyspaceTables.size());
        processFutures(futures, row -> mismatchesByTable.merge(row.getString("qualified_table_name"),
                                                               Lists.newArrayList(new Mismatch(row.getString("mismatching_token"),
                                                                                               row.getString("mismatch_type"))),
                                                               (l1, l2) -> { l1.addAll(l2); return l1;}));
        return new JobMismatches(jobId, mismatchesByTable);
    }

    public JobErrorSummary fetchErrorSummary(UUID jobId) {
        JobSummary summary = fetchJobSummary(jobId);
        List<ResultSetFuture> futures = Lists.newArrayListWithCapacity(summary.buckets);

        for (int i = 0; i < summary.buckets; i++)
            futures.add(session.executeAsync(jobErrorSummaryStatement.bind(jobId, i)));

        Map<String, Long> errorCountByTable = Maps.newHashMapWithExpectedSize(summary.keyspaceTables.size());
        processFutures(futures, row -> {
            String table = row.getString("qualified_table_name");
            if (null != table) {
                errorCountByTable.merge(row.getString("qualified_table_name"),
                                        row.getLong("error_count"),
                                        Long::sum);
            }
        });
        return new JobErrorSummary(jobId, errorCountByTable);
    }

    public JobErrorRanges fetchErrorRanges(UUID jobId) {
        JobSummary summary = fetchJobSummary(jobId);
        List<ResultSetFuture> futures = Lists.newArrayListWithCapacity(summary.buckets);

        for (int i = 0; i < summary.buckets; i++)
            futures.add(session.executeAsync(jobErrorRangesStatement.bind(jobId, i)));

        Map<String, List<Range>> errorRangesByTable = Maps.newHashMapWithExpectedSize(summary.keyspaceTables.size());
        processFutures(futures, row -> errorRangesByTable.merge(row.getString("qualified_table_name"),
                                                                Lists.newArrayList(new Range(row.getString("start_token"),
                                                                                             row.getString("end_token"))),
                                                                (l1, l2) -> { l1.addAll(l2); return l1;}));
        return new JobErrorRanges(jobId, errorRangesByTable);
    }

    public JobErrorDetail fetchErrorDetail(UUID jobId) {
        JobSummary summary = fetchJobSummary(jobId);
        List<ResultSetFuture> rangeFutures = Lists.newArrayListWithCapacity(summary.buckets);

        for (int i = 0; i < summary.buckets; i++ )
            rangeFutures.add(session.executeAsync(jobErrorRangesStatement.bind(jobId, i)));

        List<ResultSetFuture> errorFutures = Lists.newArrayList();
        processFutures(rangeFutures,
                       row -> session.executeAsync(jobErrorDetailStatement.bind(jobId,
                                                                                row.getInt("bucket"),
                                                                                row.getString("qualified_table_name"),
                                                                                row.getString("start_token"),
                                                                                row.getString("end_token"))),
                       errorFutures::add);
        Map<String, List<String>> errorsByTable = Maps.newHashMapWithExpectedSize(summary.keyspaceTables.size());
        processFutures(errorFutures,
                       row -> errorsByTable.merge(row.getString("qualified_table_name"),
                                                  Lists.newArrayList(row.getString("error_token")),
                                                  (l1, l2) -> { l1.addAll(l2); return l1;}));
        return new JobErrorDetail(jobId, errorsByTable);
    }

    public Collection<JobSummary> fetchJobsStartedBetween(DateTime start, DateTime end) {
        int days = Days.daysBetween(start, end).getDays();
        List<ResultSetFuture> idFutures = Lists.newArrayListWithCapacity(days * 24);
        for (int i = 0; i <= days; i++) {
            DateTime date = start.plusDays(i);
            LocalDate ld = LocalDate.fromYearMonthDay(date.getYear(), date.getMonthOfYear(), date.getDayOfMonth());
            for (int j = 0; j <= 23; j++) {
                idFutures.add(session.executeAsync(jobsStartDateStatement.bind(ld, j)));
            }
        }

        List<ResultSetFuture> jobFutures = Lists.newArrayList();
        processFutures(idFutures,
                       row -> session.executeAsync(jobSummaryStatement.bind(row.getUUID("job_id"))),
                       jobFutures::add);

        SortedSet<JobSummary> jobs = Sets.newTreeSet(JobSummary.COMPARATOR.reversed());
        processFutures(jobFutures, JobSummary::fromRow, jobs::add);
        return jobs;
    }

    public Collection<JobSummary> fetchJobsForSourceCluster(String sourceClusterName) {
        ResultSet jobIds = session.execute(jobsForSourceStatement.bind(sourceClusterName));
        List<ResultSetFuture> futures = Lists.newArrayList();
        jobIds.forEach(row -> futures.add(session.executeAsync(jobSummaryStatement.bind(row.getUUID("job_id")))));


        SortedSet<JobSummary> jobs = Sets.newTreeSet(JobSummary.COMPARATOR.reversed());
        processFutures(futures, JobSummary::fromRow, jobs::add);
        return jobs;
    }

    public Collection<JobSummary> fetchJobsForTargetCluster(String targetClusterName) {
        ResultSet jobIds = session.execute(jobsForTargetStatement.bind(targetClusterName));
        List<ResultSetFuture> futures = Lists.newArrayList();
        jobIds.forEach(row -> futures.add(session.executeAsync(jobSummaryStatement.bind(row.getUUID("job_id")))));

        // most recent first
        SortedSet<JobSummary> jobs = Sets.newTreeSet(JobSummary.COMPARATOR.reversed());
        processFutures(futures, JobSummary::fromRow, jobs::add);
        return jobs;
    }

    public Collection<JobSummary> fetchJobsForKeyspace(String keyspace) {
        ResultSet jobIds = session.execute(jobsForKeyspaceStatement.bind(keyspace));
        List<ResultSetFuture> futures = Lists.newArrayList();
        jobIds.forEach(row -> futures.add(session.executeAsync(jobSummaryStatement.bind(row.getUUID("job_id")))));

        // most recent first
        SortedSet<JobSummary> jobs = Sets.newTreeSet(JobSummary.COMPARATOR.reversed());
        processFutures(futures, JobSummary::fromRow, jobs::add);
        return jobs;
    }

    private void processFutures(List<ResultSetFuture> futures, Consumer<Row> consumer) {
        processFutures(futures, Function.identity(), consumer);
    }

    private <T> void processFutures(List<ResultSetFuture> futures, Function<Row, T> transform, Consumer<T> consumer) {
        Consumer<ResultSet> resultConsumer = resultSet -> resultSet.forEach(row -> consumer.accept(transform.apply(row)));
        futures.forEach(f -> getResultsSafely(f).ifPresent(resultConsumer));
    }

    private Optional<ResultSet> getResultsSafely(ResultSetFuture future) {
        try {
            return Optional.ofNullable(future.get(QUERY_TIMEOUT_MS, TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            logger.warn("Error getting result from async query", e);
            return Optional.empty();
        }
    }

    public void close()
    {
        session.close();
        cluster.close();
    }

    public static class Range {
        final String start;
        final String end;

        public Range(String start, String end) {
            this.start = start;
            this.end = end;
        }
    }

    public static class JobErrorSummary {
        final UUID jobId;
        final Map<String, Long> errorsByTable;

        public JobErrorSummary(UUID jobId, Map<String, Long> errorsByTable) {
            this.jobId = jobId;
            this.errorsByTable = errorsByTable;
        }
    }

    public static class JobErrorRanges {
        final UUID jobId;
        final Map<String, List<Range>> rangesByTable;

        public JobErrorRanges(UUID jobId, Map<String, List<Range>> rangesByTable) {
            this.jobId = jobId;
            this.rangesByTable = rangesByTable;
        }
    }

    public static class JobErrorDetail {
        final UUID jobId;
        final Map<String, List<String>> errorsByTable;

        public JobErrorDetail(UUID jobId, Map<String, List<String>> errorsByTable) {
            this.jobId = jobId;
            this.errorsByTable = errorsByTable;
        }
    }

    public static class JobMismatches {

        final UUID jobId;
        final Map<String, List<Mismatch>> mismatchesByTable;

        public JobMismatches(UUID jobId, Map<String, List<Mismatch>> mismatchesByTable) {
            this.jobId = jobId;
            this.mismatchesByTable = mismatchesByTable;
        }
    }

    public static class Mismatch {
        final String token;
        final String type;

        public Mismatch(String token, String type) {
            this.token = token;
            this.type = type;
        }
    }

    public static class JobStatus {

        final UUID jobId;
        final Map<String, Long> completedByTable;

        public JobStatus(UUID jobId, Map<String, Long> completedByTable) {
            this.jobId = jobId;
            this.completedByTable = completedByTable;
        }
    }

    public static class JobResult implements Comparable<JobResult> {

        final UUID jobId;
        final String table;
        final long matchedPartitions;
        final long mismatchedPartitions;
        final long matchedRows;
        final long matchedValues;
        final long mismatchedValues;
        final long onlyInSource;
        final long onlyInTarget;
        final long skippedPartitions;

        public JobResult(UUID jobId,
                         String table,
                         long matchedPartitions,
                         long mismatchedPartitions,
                         long matchedRows,
                         long matchedValues,
                         long mismatchedValues,
                         long onlyInSource,
                         long onlyInTarget,
                         long skippedPartitions) {
            this.jobId = jobId;
            this.table = table;
            this.matchedPartitions = matchedPartitions;
            this.mismatchedPartitions = mismatchedPartitions;
            this.matchedRows = matchedRows;
            this.matchedValues = matchedValues;
            this.mismatchedValues = mismatchedValues;
            this.onlyInSource = onlyInSource;
            this.onlyInTarget = onlyInTarget;
            this.skippedPartitions = skippedPartitions;
        }

        static JobResult fromRow(Row row) {
            return new JobResult(row.getUUID("job_id"),
                                 row.getString("qualified_table_name"),
                                 row.getLong("matched_partitions"),
                                 row.getLong("mismatched_partitions"),
                                 row.getLong("matched_rows"),
                                 row.getLong("matched_values"),
                                 row.getLong("mismatched_values"),
                                 row.getLong("partitions_only_in_source"),
                                 row.getLong("partitions_only_in_target"),
                                 row.getLong("skipped_partitions"));
        }

        public int compareTo(@NotNull JobResult other) {
            return this.table.compareTo(other.table);
        }
    }

    public static class JobSummary {

        public static final Comparator<JobSummary> COMPARATOR = Comparator.comparing(j -> j.startTime);

        final UUID jobId;
        final int buckets;
        final List<String> keyspaceTables;
        final String sourceClusterName;
        final String sourceClusterDesc;
        final String targetClusterName;
        final String targetClusterDesc;
        final int tasks;
        final String start;

        // private so it isn't included in json serialization
        private final DateTime startTime;

        private JobSummary(UUID jobId,
                           DateTime startTime,
                           int buckets,
                           List<String> keyspaceTables,
                           String sourceClusterName,
                           String sourceClusterDesc,
                           String targetClusterName,
                           String targetClusterDesc,
                           int tasks)
        {
            this.jobId = jobId;
            this.startTime = startTime;
            this.start = startTime.toString();
            this.buckets = buckets;
            this.keyspaceTables = keyspaceTables;
            this.sourceClusterName = sourceClusterName;
            this.sourceClusterDesc = sourceClusterDesc;
            this.targetClusterName = targetClusterName;
            this.targetClusterDesc = targetClusterDesc;
            this.tasks = tasks;
        }

        static JobSummary fromRow(Row row) {
            return new JobSummary(row.getUUID("job_id"),
                                  new DateTime(UUIDs.unixTimestamp(row.getUUID("job_start_time")), DateTimeZone.UTC),
                                  row.getInt("buckets"),
                                  row.getList("qualified_table_names", String.class),
                                  row.getString("source_cluster_name"),
                                  row.getString("source_cluster_desc"),
                                  row.getString("target_cluster_name"),
                                  row.getString("target_cluster_desc"),
                                  row.getInt("total_tasks"));
        }
    }
}
