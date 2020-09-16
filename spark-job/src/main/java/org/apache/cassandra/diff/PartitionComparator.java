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

import java.util.Iterator;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Row;
import org.apache.cassandra.diff.DiffCluster.Type;

public class PartitionComparator implements Callable<PartitionStats> {

    private static final Logger logger = LoggerFactory.getLogger(PartitionComparator.class);

    private final TableSpec tableSpec;
    private final Iterator<Row> source;
    private final Iterator<Row> target;
    private final RetryStrategyProvider retryStrategyProvider;

    public PartitionComparator(TableSpec tableSpec,
                               Iterator<Row> source,
                               Iterator<Row> target,
                               RetryStrategyProvider retryStrategyProvider) {
        this.tableSpec = tableSpec;
        this.source = source;
        this.target = target;
        this.retryStrategyProvider = retryStrategyProvider;
    }

    public PartitionStats call() {
        PartitionStats partitionStats = new PartitionStats();

        if (source == null || target == null) {
            logger.error("Skipping partition because one result was null (timeout despite retries)");
            partitionStats.skipped = true;
            return partitionStats;
        }

        while (hasNextRow(Type.SOURCE) && hasNextRow(Type.TARGET)) {

            Row sourceRow = getNextRow(Type.SOURCE);
            Row targetRow = getNextRow(Type.TARGET);

            // if primary keys don't match don't proceed any further, just mark the
            // partition as mismatched and be done
            if (!clusteringsEqual(sourceRow, targetRow)) {
                partitionStats.allClusteringsMatch = false;
                return partitionStats;
            }

            partitionStats.matchedRows++;

            // if the rows match, but there are mismatching values in the regular columns
            // we can continue processing the partition, so just flag it as mismatched and continue
            checkRegularColumnEquality(partitionStats, sourceRow, targetRow);
        }

        // if one of the iterators isn't exhausted, then there's a mismatch at the partition level
        if (hasNextRow(Type.SOURCE) || hasNextRow(Type.TARGET))
            partitionStats.allClusteringsMatch = false;

        return partitionStats;
    }

    private boolean hasNextRow(Type type) {
        Callable<Boolean> hasNext = () -> type == Type.SOURCE
                                          ? source.hasNext()
                                          : target.hasNext();
        RetryStrategy retryStrategy = retryStrategyProvider.get();
        return ClusterSourcedException.catches(type, () -> retryStrategy.retry(hasNext));
    }

    private Row getNextRow(Type type) {
        Callable<Row> next = () -> type == Type.SOURCE
                                   ? source.next()
                                   : target.next();
        RetryStrategy retryStrategy = retryStrategyProvider.get();
        return ClusterSourcedException.catches(type, () -> retryStrategy.retry(next));
    }

    private boolean clusteringsEqual(Row source, Row target) {
        for (ColumnMetadata column : tableSpec.getClusteringColumns()) {
            Object fromSource = source.getObject(column.getName());
            Object fromTarget = target.getObject(column.getName());

            if ((fromSource == null) != (fromTarget == null))
                return false;

            if (fromSource != null && !fromSource.equals(fromTarget))
                return false;
        }
        return true;
    }

    private void checkRegularColumnEquality(PartitionStats stats, Row source, Row target) {
        for (ColumnMetadata column : tableSpec.getRegularColumns()) {
            Object fromSource = source.getObject(column.getName());
            Object fromTarget = target.getObject(column.getName());
            if (fromSource == null) {
                if (fromTarget == null) {
                    stats.matchedValues++;
                } else {
                    stats.mismatchedValues++;
                }
            } else {
                if (fromSource.equals(fromTarget)) {
                    stats.matchedValues++;
                } else {
                    stats.mismatchedValues++;
                }
            }
        }
    }
}
