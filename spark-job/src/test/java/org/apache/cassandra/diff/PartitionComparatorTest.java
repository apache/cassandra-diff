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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import org.junit.Test;

import com.datastax.driver.core.*;
import org.apache.cassandra.diff.PartitionComparator;
import org.apache.cassandra.diff.PartitionStats;
import org.apache.cassandra.diff.TableSpec;

import static org.junit.Assert.assertEquals;

public class PartitionComparatorTest {

    @Test
    public void sourceIsNull() {
        TableSpec t = spec("table1", names("c1", "c2"), names("v1", "v2"));
        PartitionComparator comparator = comparator(t, null, rows(row(t, 0, 1, 2, 3)));
        PartitionStats stats = comparator.call();
        assertStats(stats, true, true, 0, 0, 0);
    }

    @Test
    public void targetIsNull() {
        TableSpec t = spec("table1", names("c1", "c2"), names("v1", "v2"));
        PartitionComparator comparator = comparator(t, rows(row(t, 0, 1, 2, 3)), null);
        PartitionStats stats = comparator.call();
        assertStats(stats, true, true, 0, 0, 0);
    }

    @Test
    public void sourceIsEmpty() {
        TableSpec t = spec("table1", names("c1", "c2"), names("v1", "v2"));
        PartitionComparator comparator = comparator(t, rows(), rows(row(t, 0, 1, 2, 3)));
        PartitionStats stats = comparator.call();
        assertStats(stats, false, false, 0, 0, 0);
    }

    @Test
    public void targetIsEmpty() {
        TableSpec t = spec("table1", names("c1", "c2"), names("v1", "v2"));
        PartitionComparator comparator = comparator(t, rows(row(t, 0, 1, 2, 3)), rows());
        PartitionStats stats = comparator.call();
        assertStats(stats, false, false, 0, 0, 0);
    }

    @Test
    public void sourceAndTargetAreEmpty() {
        TableSpec t = spec("table1", names("c1", "c2"), names("v1", "v2"));
        PartitionComparator comparator = comparator(t, rows(), rows());
        PartitionStats stats = comparator.call();
        assertStats(stats, false, true, 0, 0, 0);
    }

    @Test
    public void sourceContainsExtraRowsAtStart() {
        TableSpec t = spec("table1", names("c1", "c2"), names("v1", "v2"));
        PartitionComparator comparator = comparator(t,
                                                    rows(row(t, 0, 1, 2, 3),
                                                         row(t, 10, 11, 12, 13)),
                                                    rows(row(t, 10, 11, 12, 13)));
        PartitionStats stats = comparator.call();
        // Comparison fails fast, so bails on the initial mismatch
        assertStats(stats, false, false, 0, 0, 0);
    }

    @Test
    public void targetContainsExtraRowsAtStart() {
        TableSpec t = spec("table1", names("c1", "c2"), names("v1", "v2"));
        PartitionComparator comparator = comparator(t,
                                                    rows(row(t, 10, 11, 12, 13)),
                                                    rows(row(t, 0, 1, 2, 3),
                                                         row(t, 10, 11, 12, 13)));
        PartitionStats stats = comparator.call();
        // Comparison fails fast, so bails on the initial mismatch
        assertStats(stats, false, false, 0, 0, 0);
    }

    @Test
    public void sourceContainsExtraRowsAtEnd() {
        TableSpec t = spec("table1", names("c1", "c2"), names("v1", "v2"));
        PartitionComparator comparator = comparator(t,
                                                    rows(row(t, 0, 1, 2, 3),
                                                         row(t, 10, 11, 12, 13)),
                                                    rows(row(t, 0, 1, 2, 3)));
        PartitionStats stats = comparator.call();
        // The fact that the first row & all its v1 & v2 values match should be reflected in the stats
        assertStats(stats, false, false, 1, 2, 0);
    }

    @Test
    public void targetContainsExtraRowsAtEnd() {
        TableSpec t = spec("table1", names("c1", "c2"), names("v1", "v2"));
        PartitionComparator comparator = comparator(t,
                                                    rows(row(t, 0, 1, 2, 3)),
                                                    rows(row(t, 0, 1, 2, 3),
                                                         row(t, 10, 11, 12, 13)));
        PartitionStats stats = comparator.call();
        // The fact that the first row & all its v1 & v2 values match should be reflected in the stats
        assertStats(stats, false, false, 1, 2, 0);
    }

    @Test
    public void withoutClusteringsAllRowsMatching() {
        TableSpec t = spec("table1", names(), names("v1", "v2"));
        PartitionComparator comparator = comparator(t,
                                                    rows(row(t, 0, 1),
                                                         row(t, 1, 11),
                                                         row(t, 2, 12)),
                                                    rows(row(t, 0, 1),
                                                         row(t, 1, 11),
                                                         row(t, 2, 12)));
        PartitionStats stats = comparator.call();
        assertStats(stats, false, true, 3, 6, 0);
    }

    @Test
    public void singleClusteringAllRowsMatching() {
        TableSpec t = spec("table1", names("c1"), names("v1", "v2"));
        PartitionComparator comparator = comparator(t,
                                                    rows(row(t, 0, 1, 2),
                                                         row(t, 1, 11, 12),
                                                         row(t, 2, 21, 22)),
                                                    rows(row(t, 0, 1, 2),
                                                         row(t, 1, 11, 12),
                                                         row(t, 2, 21, 22)));
        PartitionStats stats = comparator.call();
        assertStats(stats, false, true, 3, 6, 0);
    }

    @Test
    public void multipleClusteringAllRowsMatching() {
        TableSpec t = spec("table1", names("c1", "c2"), names("v1", "v2"));
        PartitionComparator comparator = comparator(t,
                                                    rows(row(t, 0, 1, 2, 3),
                                                         row(t, 1, 11, 12, 13),
                                                         row(t, 2, 21, 22, 23)),
                                                    rows(row(t, 0, 1, 2, 3),
                                                         row(t, 1, 11, 12, 13),
                                                         row(t, 2, 21, 22, 23)));
        PartitionStats stats = comparator.call();
        assertStats(stats, false, true, 3, 6, 0);
    }

    @Test
    public void withoutClusteringsWithMismatches() {
        TableSpec t = spec("table1", names(), names("v1", "v2"));
        PartitionComparator comparator = comparator(t,
                                                    rows(row(t, 0, 1),
                                                         row(t, 1, 11),
                                                         row(t, 2, 12)),
                                                    rows(row(t, 0, 1),
                                                         row(t, 1, 1100),
                                                         row(t, 2, 1200)));
        PartitionStats stats = comparator.call();
        assertStats(stats, false, true, 3, 4, 2);
    }

    @Test
    public void singleClusteringWithMismatches() {
        TableSpec t = spec("table1", names("c1"), names("v1", "v2"));
        PartitionComparator comparator = comparator(t,
                                                    rows(row(t, 0, 1, 2),
                                                         row(t, 1, 11, 21),
                                                         row(t, 2, 12, 22)),
                                                    rows(row(t, 0, 1, 20),
                                                         row(t, 1, 1100, 21),
                                                         row(t, 2, 12, 1200)));
        PartitionStats stats = comparator.call();
        assertStats(stats, false, true, 3, 3, 3);
    }

    private void assertStats(PartitionStats stats,
                             boolean skipped,
                             boolean clusteringsMatch,
                             int matchedRows,
                             int matchedValues,
                             int mismatchedValues) {
        assertEquals(skipped, stats.skipped);
        assertEquals(clusteringsMatch, stats.allClusteringsMatch);
        assertEquals(matchedRows, stats.matchedRows);
        assertEquals(matchedValues, stats.matchedValues);
        assertEquals(mismatchedValues, stats.mismatchedValues);
    }

    PartitionComparator comparator(TableSpec table, Iterator<Row> source, Iterator<Row> target) {
        return new PartitionComparator(table, source, target);
    }

    List<String> names(String...names) {
        return Lists.newArrayList(names);
    }

    TableSpec spec(String table, List<String> clusteringColumns, List<String> regularColumns) {
        return new TableSpec(new KeyspaceTablePair("ks", table), columns(clusteringColumns), columns(regularColumns));
    }

    List<ColumnMetadata> columns(List<String> names) {
        return names.stream().map(ColumnMetadataHelper::column).collect(Collectors.toList());
    }

    Iterator<Row> rows(Row...rows) {
        return new AbstractIterator<Row>() {
            int i = 0;
            protected Row computeNext() {
                if (i < rows.length)
                    return rows[i++];
                return endOfData();
            }
        };
    }

    Row row(TableSpec table, Object...values) {
        return new TestRow(Stream.concat(table.getClusteringColumns().stream(),
                                         table.getRegularColumns().stream())
                                 .map(ColumnMetadata::getName).toArray(String[]::new),
                           values);
    }

    static class TestRow implements Row {
        private final String[] names;
        private final Object[] values;

        TestRow(String[] names, Object[] values) {
            if (names.length != values.length)
                throw new IllegalArgumentException(String.format("Number of column names (%d) doesn't " +
                                                                 "match number of values(%d)",
                                                                 names.length, values.length));
            this.names = names;
            this.values = values;
        }

        // Only getObject(String) is used by PartitionComparator
        public Object getObject(String s) {
            for (int i=0; i < names.length; i++)
                if (names[i].equals(s))
                    return values[i];

            throw new IllegalArgumentException(s + " is not a column defined in this metadata");
        }

        public boolean isNull(String s) {
            throw new UnsupportedOperationException();
        }

        public boolean getBool(String s) {
            throw new UnsupportedOperationException();
        }

        public byte getByte(String s) {
            throw new UnsupportedOperationException();
        }

        public short getShort(String s) {
            throw new UnsupportedOperationException();
        }

        public int getInt(String s) {
            throw new UnsupportedOperationException();
        }

        public long getLong(String s) {
            throw new UnsupportedOperationException();
        }

        public Date getTimestamp(String s) {
            throw new UnsupportedOperationException();
        }

        public LocalDate getDate(String s) {
            throw new UnsupportedOperationException();
        }

        public long getTime(String s) {
            throw new UnsupportedOperationException();
        }

        public float getFloat(String s) {
            throw new UnsupportedOperationException();
        }

        public double getDouble(String s) {
            throw new UnsupportedOperationException();
        }

        public ByteBuffer getBytesUnsafe(String s) {
            throw new UnsupportedOperationException();
        }

        public ByteBuffer getBytes(String s) {
            throw new UnsupportedOperationException();
        }

        public String getString(String s) {
            throw new UnsupportedOperationException();
        }

        public BigInteger getVarint(String s) {
            throw new UnsupportedOperationException();
        }

        public BigDecimal getDecimal(String s) {
            throw new UnsupportedOperationException();
        }

        public UUID getUUID(String s) {
            throw new UnsupportedOperationException();
        }

        public InetAddress getInet(String s) {
            throw new UnsupportedOperationException();
        }

        public <T> List<T> getList(String s, Class<T> aClass) {
            throw new UnsupportedOperationException();
        }

        public <T> List<T> getList(String s, TypeToken<T> typeToken) {
            throw new UnsupportedOperationException();
        }

        public <T> Set<T> getSet(String s, Class<T> aClass) {
            throw new UnsupportedOperationException();
        }

        public <T> Set<T> getSet(String s, TypeToken<T> typeToken) {
            throw new UnsupportedOperationException();
        }

        public <K, V> Map<K, V> getMap(String s, Class<K> aClass, Class<V> aClass1) {
            throw new UnsupportedOperationException();
        }

        public <K, V> Map<K, V> getMap(String s, TypeToken<K> typeToken, TypeToken<V> typeToken1) {
            throw new UnsupportedOperationException();
        }

        public UDTValue getUDTValue(String s) {
            throw new UnsupportedOperationException();
        }

        public TupleValue getTupleValue(String s) {
            throw new UnsupportedOperationException();
        }

        public <T> T get(String s, Class<T> aClass) {
            throw new UnsupportedOperationException();
        }

        public <T> T get(String s, TypeToken<T> typeToken) {
            throw new UnsupportedOperationException();
        }

        public <T> T get(String s, TypeCodec<T> typeCodec) {
            throw new UnsupportedOperationException();
        }

        public ColumnDefinitions getColumnDefinitions() {
            throw new UnsupportedOperationException();
        }

        public Token getToken(int i) {
            throw new UnsupportedOperationException();
        }

        public Token getToken(String s) {
            throw new UnsupportedOperationException();
        }

        public Token getPartitionKeyToken() {
            throw new UnsupportedOperationException();
        }

        public boolean isNull(int i) {
            throw new UnsupportedOperationException();
        }

        public boolean getBool(int i) {
            throw new UnsupportedOperationException();
        }

        public byte getByte(int i) {
            throw new UnsupportedOperationException();
        }

        public short getShort(int i) {
            throw new UnsupportedOperationException();
        }

        public int getInt(int i) {
            throw new UnsupportedOperationException();
        }

        public long getLong(int i) {
            throw new UnsupportedOperationException();
        }

        public Date getTimestamp(int i) {
            throw new UnsupportedOperationException();
        }

        public LocalDate getDate(int i) {
            throw new UnsupportedOperationException();
        }

        public long getTime(int i) {
            throw new UnsupportedOperationException();
        }

        public float getFloat(int i) {
            throw new UnsupportedOperationException();
        }

        public double getDouble(int i) {
            throw new UnsupportedOperationException();
        }

        public ByteBuffer getBytesUnsafe(int i) {
            throw new UnsupportedOperationException();
        }

        public ByteBuffer getBytes(int i) {
            throw new UnsupportedOperationException();
        }

        public String getString(int i) {
            throw new UnsupportedOperationException();
        }

        public BigInteger getVarint(int i) {
            throw new UnsupportedOperationException();
        }

        public BigDecimal getDecimal(int i) {
            throw new UnsupportedOperationException();
        }

        public UUID getUUID(int i) {
            throw new UnsupportedOperationException();
        }

        public InetAddress getInet(int i) {
            throw new UnsupportedOperationException();
        }

        public <T> List<T> getList(int i, Class<T> aClass) {
            throw new UnsupportedOperationException();
        }

        public <T> List<T> getList(int i, TypeToken<T> typeToken) {
            throw new UnsupportedOperationException();
        }

        public <T> Set<T> getSet(int i, Class<T> aClass) {
            throw new UnsupportedOperationException();
        }

        public <T> Set<T> getSet(int i, TypeToken<T> typeToken) {
            throw new UnsupportedOperationException();
        }

        public <K, V> Map<K, V> getMap(int i, Class<K> aClass, Class<V> aClass1) {
            throw new UnsupportedOperationException();
        }

        public <K, V> Map<K, V> getMap(int i, TypeToken<K> typeToken, TypeToken<V> typeToken1) {
            throw new UnsupportedOperationException();
        }

        public UDTValue getUDTValue(int i) {
            throw new UnsupportedOperationException();
        }

        public TupleValue getTupleValue(int i) {
            throw new UnsupportedOperationException();
        }

        public Object getObject(int i) {
            throw new UnsupportedOperationException();
        }

        public <T> T get(int i, Class<T> aClass) {
            throw new UnsupportedOperationException();
        }

        public <T> T get(int i, TypeToken<T> typeToken) {
            throw new UnsupportedOperationException();
        }

        public <T> T get(int i, TypeCodec<T> typeCodec) {
            throw new UnsupportedOperationException();
        }
    }

}
