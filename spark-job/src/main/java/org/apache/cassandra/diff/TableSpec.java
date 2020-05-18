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

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;

import static org.apache.cassandra.diff.DiffContext.cqlizedString;

public class TableSpec {

    private final KeyspaceTablePair keyspaceTablePair;
    private ImmutableList<ColumnMetadata> clusteringColumns;
    private ImmutableList<ColumnMetadata> regularColumns;


    public KeyspaceTablePair getTable()
    {
        return keyspaceTablePair;
    }


    public ImmutableList<ColumnMetadata> getClusteringColumns() {
        return clusteringColumns;
    }

    public ImmutableList<ColumnMetadata> getRegularColumns() {
        return regularColumns;
    }


    /**
     * @param table the table to diff
     * @param clusteringColumns the clustering columns, retrieved from cluster using the client
     * @param regularColumns the non-primary key columns, retrieved from cluster using the client
     */
    TableSpec(final KeyspaceTablePair table,
              final List<ColumnMetadata> clusteringColumns,
              final List<ColumnMetadata> regularColumns) {
        this.keyspaceTablePair = table;
        this.clusteringColumns = ImmutableList.copyOf(clusteringColumns);
        this.regularColumns = ImmutableList.copyOf(regularColumns);
    }

    public static TableSpec make(KeyspaceTablePair keyspaceTablePair, DiffCluster diffCluster) {
        final Cluster cluster = diffCluster.cluster;

        final String cqlizedKeyspace = cqlizedString(keyspaceTablePair.keyspace);
        final String cqlizedTable = cqlizedString(keyspaceTablePair.table);

        KeyspaceMetadata ksMetadata = cluster.getMetadata().getKeyspace(cqlizedKeyspace);
        if (ksMetadata == null) {
            throw new IllegalArgumentException(String.format("Keyspace %s not found in %s cluster", keyspaceTablePair.keyspace, diffCluster.clusterId));
        }

        TableMetadata tableMetadata = ksMetadata.getTable(cqlizedTable);
        List<ColumnMetadata> clusteringColumns = tableMetadata.getClusteringColumns();
        List<ColumnMetadata> regularColumns = tableMetadata.getColumns()
                                                           .stream()
                                                           .filter(c -> !(clusteringColumns.contains(c)))
                                                           .collect(Collectors.toList());
        return new TableSpec(KeyspaceTablePair.from(tableMetadata), clusteringColumns, regularColumns);
    }

    public boolean equalsNamesOnly(TableSpec other) {
        return this.keyspaceTablePair.equals(other.keyspaceTablePair)
            && columnNames(this.clusteringColumns).equals(columnNames(other.clusteringColumns))
            && columnNames(this.regularColumns).equals(columnNames(other.regularColumns));
    }

    private static List<String> columnNames(List<ColumnMetadata> columns) {
        return columns.stream().map(ColumnMetadata::getName).collect(Collectors.toList());
    }

    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof TableSpec))
            return false;

        TableSpec other = (TableSpec)o;
        return this.keyspaceTablePair.equals(other.keyspaceTablePair)
               && this.clusteringColumns.equals(other.clusteringColumns)
               && this.regularColumns.equals(other.regularColumns);

    }

    public int hashCode() {
        return Objects.hash(keyspaceTablePair, clusteringColumns, regularColumns);
    }

    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("table", keyspaceTablePair)
                          .add("clusteringColumns", clusteringColumns)
                          .add("regularColumns", regularColumns)
                          .toString();
    }
}
