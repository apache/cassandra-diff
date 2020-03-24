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

import java.io.Serializable;
import java.math.BigInteger;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * To run this, something like this should be executed for local runs
 *
 * spark-submit --files ./spark-job/localconfig.yaml
 *              --master "local[2]"
 *              --class org.apache.cassandra.DiffJob spark-job/target/spark-job-0.1-SNAPSHOT.jar
 *              localconfig.yaml
 */

public class DiffJob {
    private static final Logger logger = LoggerFactory.getLogger(DiffJob.class);

    public static void main(String ... args) {
        if (args.length == 0) {
            System.exit(-1);
        }
        SparkSession spark = SparkSession.builder().appName("cassandra-diff").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        String configFile = SparkFiles.get(args[0]);
        YamlJobConfiguration configuration = YamlJobConfiguration.load(configFile);
        DiffJob diffJob = new DiffJob();
        diffJob.run(configuration, sc);
        spark.stop();
    }

    // optional code block to run before a job starts
    private Runnable preJobHook;
    // optional code block to run after a job completes successfully; otherwise, it is not executed.
    private Consumer<Map<String, RangeStats>> postJobHook;

    public void addPreJobHook(Runnable preJobHook) {
        this.preJobHook = preJobHook;
    }

    public void addPostJobHook(Consumer<Map<String, RangeStats>> postJobHook) {
        this.postJobHook = postJobHook;
    }

    public void run(JobConfiguration configuration, JavaSparkContext sc) {
        SparkConf conf = sc.getConf();
        // get partitioner from both clusters and verify that they match
        ClusterProvider sourceProvider = ClusterProvider.getProvider(configuration.clusterConfig("source"), "source");
        ClusterProvider targetProvider = ClusterProvider.getProvider(configuration.clusterConfig("target"), "target");
        String sourcePartitioner;
        String targetPartitioner;
        try (Cluster sourceCluster = sourceProvider.getCluster();
             Cluster targetCluster = targetProvider.getCluster()) {
            sourcePartitioner = sourceCluster.getMetadata().getPartitioner();
            targetPartitioner = targetCluster.getMetadata().getPartitioner();
        }
        if (!sourcePartitioner.equals(targetPartitioner)) {
            throw new IllegalStateException(String.format("Cluster partitioners do not match; Source: %s, Target: %s,",
                                                          sourcePartitioner, targetPartitioner));
        }
        TokenHelper tokenHelper = TokenHelper.forPartitioner(sourcePartitioner);

        logger.info("Configuring job metadata store");
        ClusterProvider metadataProvider = ClusterProvider.getProvider(configuration.clusterConfig("metadata"), "metadata");
        JobMetadataDb.JobLifeCycle job = null;
        UUID jobId = null;
        try (Cluster metadataCluster = metadataProvider.getCluster();
             Session metadataSession = metadataCluster.connect()) {

            MetadataKeyspaceOptions metadataOptions = configuration.metadataOptions();
            JobMetadataDb.Schema.maybeInitialize(metadataSession, metadataOptions);

            // Job params, which once a job is created cannot be modified in subsequent re-runs
            logger.info("Creating or retrieving job parameters");
            job = new JobMetadataDb.JobLifeCycle(metadataSession, metadataOptions.keyspace);
            Params params = getJobParams(job, configuration);
            logger.info("Job Params: {}", params);
            if (null == params)
                throw new RuntimeException("Unable to initialize job params");

            jobId = params.jobId;
            List<Split> splits = getSplits(configuration, TokenHelper.forPartitioner(sourcePartitioner));

            // Job options, which may be modified per-run
            int instances = Integer.parseInt(conf.get("spark.executor.instances", "4"));
            int cores = Integer.parseInt(conf.get("spark.executor.cores", "2"));
            int executors = instances * cores;
            // according to https://spark.apache.org/docs/latest/rdd-programming-guide.html#parallelized-collections we should
            // have 2-4 partitions per cpu in the cluster:
            int slices = Math.min(4 * executors, splits.size());
            int perExecutorRateLimit = configuration.rateLimit() / executors;

            // Record the high level job summary info
            job.initializeJob(params,
                              sourceProvider.getClusterName(),
                              sourceProvider.toString(),
                              targetProvider.getClusterName(),
                              targetProvider.toString());

            logger.info("DiffJob {} comparing [{}] in keyspace {} on {} and {}",
                        jobId,
                        String.join(",", params.tables),
                        params.keyspace,
                        sourceProvider,
                        targetProvider);

            if (null != preJobHook)
                preJobHook.run();

            // Run the distributed diff and collate results
            Map<String, RangeStats> diffStats = sc.parallelize(splits, slices)
                                                  .map((split) -> new Differ(configuration,
                                                                             params,
                                                                             perExecutorRateLimit,
                                                                             split,
                                                                             tokenHelper,
                                                                             sourceProvider,
                                                                             targetProvider,
                                                                             metadataProvider,
                                                                             new TrackerProvider(configuration.metadataOptions().keyspace))
                                                                  .run())
                                                  .reduce(Differ::accumulate);
            // Publish results. This also removes the job from the currently running list
            job.finalizeJob(params.jobId, diffStats);
            logger.info("FINISHED: {}", diffStats);
            if (null != postJobHook)
                postJobHook.accept(diffStats);
        } catch (Exception e) {
            // If the job errors out, try and mark the job as not running, so it can be restarted.
            // If the error was thrown from JobMetadataDb.finalizeJob *after* the job had already
            // been marked not running, this will log a warning, but is not fatal.
            if (job != null && jobId != null)
                job.markNotRunning(jobId);
            throw new RuntimeException("Diff job failed", e);
        } finally {
            if (sc.isLocal())
            {
                Differ.shutdown();
                JobMetadataDb.ProgressTracker.resetStatements();
            }
        }
    }

    private static Params getJobParams(JobMetadataDb.JobLifeCycle job, JobConfiguration conf) {
        if (conf.jobId().isPresent()) {
            return job.getJobParams(conf.jobId().get());
        } else {
            return new Params(UUID.randomUUID(),
                              conf.keyspace(),
                              conf.tables(),
                              conf.buckets(),
                              conf.splits());
        }
    }

    private static List<Split> getSplits(JobConfiguration config, TokenHelper tokenHelper) {
        logger.info("Initializing splits");
        List<Split> splits = calculateSplits(config.splits(), config.buckets(), tokenHelper);
        logger.info("All Splits: {}", splits);
        if (!config.specificTokens().isEmpty() && config.specificTokens().modifier == SpecificTokens.Modifier.ACCEPT) {
            splits = getSplitsForTokens(config.specificTokens().tokens, splits);
            logger.info("Splits for specific tokens ONLY: {}", splits);
        }
        // shuffle the splits to make sure the work is spread over the workers,
        // important if it isn't a full cluster is being compared
        Collections.shuffle(splits);
        return splits;
    }

    @VisibleForTesting
    static List<Split> calculateSplits(int numSplits, int numBuckets, TokenHelper tokenHelper) {
        List<Split> splits = new ArrayList<>(numSplits);
        BigInteger minToken = tokenHelper.min();
        BigInteger maxToken = tokenHelper.max();

        BigInteger totalTokens = maxToken.subtract(minToken);
        BigInteger segmentSize = totalTokens.divide(BigInteger.valueOf(numSplits));

        // add the first split starting at minToken without adding BigInt.ONE below
        // Splits are grouped into buckets so we can shard the journal info across
        // C* partitions
        splits.add(new Split(0,  0, minToken, minToken.add(segmentSize)));
        BigInteger prev = minToken.add(segmentSize);
        for (int i = 1; i < numSplits - 1; i++) {
            BigInteger next = prev.add(segmentSize);
            // add ONE to avoid split overlap
            splits.add(new Split(i, i % numBuckets, prev.add(BigInteger.ONE), next));
            prev = next;
        }
        splits.add(new Split(numSplits - 1, (numSplits - 1) % numBuckets,  prev.add(BigInteger.ONE), maxToken)); // make sure we cover the whole range
        return splits;
    }

    @VisibleForTesting
    static List<Split> getSplitsForTokens(Set<BigInteger> tokens, List<Split> splits) {
        return splits.stream().filter(split -> split.containsAny(tokens)).collect(Collectors.toList());
    }

    @VisibleForTesting
    static class Split implements Serializable {
        final int splitNumber;
        final int bucket;
        final BigInteger start;
        final BigInteger end;

        Split(int splitNumber, int bucket, BigInteger start, BigInteger end) {
            this.splitNumber = splitNumber;
            this.bucket = bucket;
            this.start = start;
            this.end = end;
        }

        public String toString() {
            return "Split [" +
                   start +
                   ", " +
                   end +
                   ']';
        }

        public boolean containsAny(Set<BigInteger> specificTokens) {
            for (BigInteger specificToken : specificTokens) {
                if (specificToken.compareTo(start) >= 0 && specificToken.compareTo(end) <= 0)
                    return true;
            }
            return false;
        }
    }

    static class Params implements Serializable {
        public final UUID jobId;
        public final String keyspace;
        public final ImmutableList<String> tables;
        public final int buckets;
        public final int tasks;

        Params(UUID jobId, String keyspace, List<String> tables, int buckets, int tasks) {
            this.jobId = jobId;
            this.keyspace = keyspace;
            this.tables = ImmutableList.copyOf(tables);
            this.buckets = buckets;
            this.tasks = tasks;
        }

        public String toString() {
            return String.format("Params: [jobId: %s, keyspace: %s, tables: %s, buckets: %s, tasks: %s]",
                                 jobId, keyspace, tables.stream().collect(Collectors.joining(",")), buckets, tasks);
        }
    }

    static class TaskStatus {
        public static final TaskStatus EMPTY = new TaskStatus(null, null);
        public final BigInteger lastToken;
        public final RangeStats stats;

        TaskStatus(BigInteger lastToken, RangeStats stats) {
            this.lastToken = lastToken;
            this.stats = stats;
        }

        public String toString() {
            return "TaskStatus{" +
                   "lastToken=" + lastToken +
                   ", stats=" + stats +
                   '}';
        }
    }

    public static class TrackerProvider implements Serializable {
        private final String metadataKeyspace;

        TrackerProvider(String metadataKeyspace) {
            this.metadataKeyspace = metadataKeyspace;
        }

        public void initializeStatements(Session session) {
            JobMetadataDb.ProgressTracker.initializeStatements(session, metadataKeyspace);
        }

        public JobMetadataDb.ProgressTracker getTracker(Session session, UUID jobId, Split split) {
            return new JobMetadataDb.ProgressTracker(jobId, split.bucket, split.start, split.end, metadataKeyspace, session);
        }
    }
 }
