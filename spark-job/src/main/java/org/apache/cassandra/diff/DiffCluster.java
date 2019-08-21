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
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.*;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.diff.DiffContext.cqlizedString;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class DiffCluster implements AutoCloseable
{
    private final static Logger logger = LoggerFactory.getLogger(DiffCluster.class);

    public enum Type {SOURCE, TARGET}

    private final Map<String, PreparedStatement[]> preparedStatements = new HashMap<>();
    private final ConsistencyLevel consistencyLevel;
    public final Cluster cluster;
    private final Session session;
    private final TokenHelper tokenHelper;
    public final String keyspace;
    public final List<BigInteger> tokenList;

    public final RateLimiter getPartitionRateLimiter;
    public final Type clusterId;
    private final int tokenScanFetchSize;
    private final int partitionReadFetchSize;
    private final int readTimeoutMillis;

    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public DiffCluster(Type clusterId,
                       Cluster cluster,
                       String keyspace,
                       ConsistencyLevel consistencyLevel,
                       RateLimiter getPartitionRateLimiter,
                       int tokenScanFetchSize,
                       int partitionReadFetchSize,
                       int readTimeoutMillis)

    {
        this.keyspace = keyspace;
        this.consistencyLevel = consistencyLevel;
        this.cluster = cluster;
        this.tokenHelper = TokenHelper.forPartitioner(cluster.getMetadata().getPartitioner());
        this.clusterId = clusterId;
        this.tokenList = Collections.emptyList();
        this.getPartitionRateLimiter = getPartitionRateLimiter;
        this.session = cluster.connect();
        this.tokenScanFetchSize = tokenScanFetchSize;
        this.partitionReadFetchSize = partitionReadFetchSize;
        this.readTimeoutMillis = readTimeoutMillis;
    }

    public Iterator<PartitionKey> getPartitionKeys(String table, final BigInteger prevToken, final BigInteger token) {
        try {
            return Uninterruptibles.getUninterruptibly(fetchPartitionKeys(table, prevToken, token));
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private ListenableFuture<Iterator<PartitionKey>> fetchPartitionKeys(String table, final BigInteger prevToken, final BigInteger token) {
        BoundStatement statement = keyReader(table).bind(tokenHelper.forBindParam(prevToken),
                                                         tokenHelper.forBindParam(token));
        statement.setFetchSize(tokenScanFetchSize);
        statement.setReadTimeoutMillis(readTimeoutMillis);
        return Futures.transform(session.executeAsync(statement),
                                 this::toPartitionKeys,
                                 MoreExecutors.directExecutor());
    }

    private AbstractIterator<PartitionKey> toPartitionKeys(ResultSet resultSet) {
        return new AbstractIterator<PartitionKey>() {
            Iterator<Row> rows = resultSet.iterator();

            protected PartitionKey computeNext() {
                if (session.isClosed())
                    throw new RuntimeException("Session was closed, cannot get next partition key");

                if (stopped.get())
                    throw new RuntimeException("Job was stopped, cannot get next partition key");

                return rows.hasNext() ? new PartitionKey(rows.next()) : endOfData();
            }
        };
    }

    public Iterator<Row> getPartition(TableSpec table, PartitionKey key, boolean shouldReverse) {
        return readPartition(table.getTable(), key, shouldReverse)
               .getUninterruptibly()
               .iterator();
    }

    private ResultSetFuture readPartition(String table, final PartitionKey key, boolean shouldReverse) {
        BoundStatement statement = shouldReverse
                                   ? reverseReader(table).bind(key.getComponents().toArray())
                                   : forwardReader(table).bind(key.getComponents().toArray());
        statement.setFetchSize(partitionReadFetchSize);
        statement.setReadTimeoutMillis(readTimeoutMillis);
        getPartitionRateLimiter.acquire();
        return session.executeAsync(statement);
    }

    public void stop() {
        stopped.set(true);
    }

    public void close()
    {
        logger.info("Closing cluster {}", this.clusterId);
        session.closeAsync();
        cluster.closeAsync();
    }

    private PreparedStatement keyReader(String table) {
        return getStatementForTable(table, 0);
    }

    private PreparedStatement forwardReader(String table) {
        return getStatementForTable(table, 1);
    }

    private PreparedStatement reverseReader(String table) {
        return getStatementForTable(table, 2);
    }

    private PreparedStatement getStatementForTable(String table, int index) {
        if (!preparedStatements.containsKey(table)) {
            synchronized (this) {
                if (!preparedStatements.containsKey(table)) {
                    PreparedStatement keyStatement = getKeyStatement(table);
                    PreparedStatement[] partitionReadStmts = getFullStatement(table);
                    preparedStatements.put(table, new PreparedStatement[]{ keyStatement ,
                                                                           partitionReadStmts[0],
                                                                           partitionReadStmts[1] });
                }
            }
        }
        return preparedStatements.get(table)[index];
    }

    private PreparedStatement getKeyStatement(@NotNull String table) {
        final TableMetadata tableMetadata = session.getCluster()
                                                   .getMetadata()
                                                   .getKeyspace(cqlizedString(keyspace))
                                                   .getTable(cqlizedString(table));
        String[] partitionKeyColumns = columnNames(tableMetadata.getPartitionKey());

        Select.Selection selection = QueryBuilder.select().distinct().column(token(partitionKeyColumns));
        for (String column : partitionKeyColumns)
            selection = selection.column(column);

        BuiltStatement select = selection.from(tableMetadata)
                                         .where(gt(token(partitionKeyColumns), bindMarker()))
                                         .and(lte(token(partitionKeyColumns), bindMarker()));

        logger.debug("Partition key/token read CQL : {}", select.toString());
        return session.prepare(select).setConsistencyLevel(consistencyLevel);
    }

    private PreparedStatement[] getFullStatement(@NotNull String table) {
        final TableMetadata tableMetadata = session.getCluster()
                                                   .getMetadata()
                                                   .getKeyspace(cqlizedString(keyspace))
                                                   .getTable(cqlizedString(table));
        String[] partitionKeyColumns = columnNames(tableMetadata.getPartitionKey());
        String[] allColumns = columnNames(tableMetadata.getColumns());

        Select.Selection selection = QueryBuilder.select().column(token(partitionKeyColumns));
        for (String column : allColumns)
            selection = selection.column(column);

        Select select = selection.from(tableMetadata);

        for (String column : partitionKeyColumns)
            select.where().and(eq(column, bindMarker()));

        logger.info("Partition forward read CQL : {}", select.toString());
        PreparedStatement forwardRead = session.prepare(select).setConsistencyLevel(consistencyLevel);

        List<ColumnMetadata> clusteringColumns = tableMetadata.getClusteringColumns();
        // if the table has no clustering columns a reverse read doesn't make sense
        // and will never be executed, so just skip preparing the reverse query
        if (clusteringColumns.isEmpty())
            return new PreparedStatement[] {forwardRead, null};

        // Depending on DiffContext.reverseReadProbability, we may attempt to read the
        // partition in reverse order, so prepare a statement for that
        List<ClusteringOrder> clusteringOrders = tableMetadata.getClusteringOrder();
        Ordering[] reverseOrdering = new Ordering[clusteringColumns.size()];
        for (int i=0; i<clusteringColumns.size(); i++) {
            reverseOrdering[i] = clusteringOrders.get(i) == ClusteringOrder.ASC
                                 ? desc(clusteringColumns.get(i).getName())
                                 : asc(clusteringColumns.get(i).getName());
        }

        select.orderBy(reverseOrdering);
        logger.info("Partition reverse read CQL : {}", select.toString());

        PreparedStatement reverseRead = session.prepare(select).setConsistencyLevel(consistencyLevel);

        return new PreparedStatement[] {forwardRead, reverseRead};
    }

    private static String[] columnNames(List<ColumnMetadata> columns) {
        return columns.stream().map(ColumnMetadata::getName).map(DiffCluster::columnToString).toArray(String[]::new);
    }

    private static String columnToString(String name)
    {
        return '"'+name+'"';
    }
}
