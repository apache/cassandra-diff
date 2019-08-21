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

import com.datastax.driver.core.*;
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import static org.apache.cassandra.diff.DiffContext.cqlizedString;

public class TableSpec {

    private final String table;
    private ImmutableList<ColumnMetadata> clusteringColumns;
    private ImmutableList<ColumnMetadata> regularColumns;


    public String getTable()
    {
        return table;
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
    TableSpec(final String table,
              final List<ColumnMetadata> clusteringColumns,
              final List<ColumnMetadata> regularColumns) {
        this.table = table;
        this.clusteringColumns = ImmutableList.copyOf(clusteringColumns);
        this.regularColumns = ImmutableList.copyOf(regularColumns);
    }

    public static TableSpec make(String table, DiffCluster diffCluster) {
        final Cluster cluster = diffCluster.cluster;

        final String cqlizedKeyspace = cqlizedString(diffCluster.keyspace);
        final String cqlizedTable = cqlizedString(table);

        KeyspaceMetadata ksMetadata = cluster.getMetadata().getKeyspace(cqlizedKeyspace);
        if (ksMetadata == null) {
            throw new IllegalArgumentException(String.format("Keyspace %s not found in %s cluster", diffCluster.keyspace, diffCluster.clusterId));
        }

        TableMetadata tableMetadata = ksMetadata.getTable(cqlizedTable);
        List<ColumnMetadata> clusteringColumns = tableMetadata.getClusteringColumns();
        List<ColumnMetadata> regularColumns = tableMetadata.getColumns()
                                                           .stream()
                                                           .filter(c -> !(clusteringColumns.contains(c)))
                                                           .collect(Collectors.toList());
        return new TableSpec(tableMetadata.getName(), clusteringColumns, regularColumns);
    }

    public boolean equalsNamesOnly(TableSpec other) {
        return this.table.equals(other.table)
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
        return this.table.equals(other.table)
               && this.clusteringColumns.equals(other.clusteringColumns)
               && this.regularColumns.equals(other.regularColumns);

    }

    public int hashCode() {
        return Objects.hash(table, clusteringColumns, regularColumns);
    }

    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("table", table)
                          .add("clusteringColumns", clusteringColumns)
                          .add("regularColumns", regularColumns)
                          .toString();
    }
}
