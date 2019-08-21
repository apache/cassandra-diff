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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;

import com.datastax.driver.core.*;
import org.jetbrains.annotations.NotNull;

public class PartitionKey implements Comparable<PartitionKey> {

    private final Row row;

    public PartitionKey(Row row) {
        this.row = row;
    }

    public BigInteger getTokenAsBigInteger(){
        Token token = getToken();
        if (token.getType() == DataType.bigint()) {
            return BigInteger.valueOf((Long) token.getValue());
        } else {
            return (BigInteger) token.getValue();
        }
    }

    public List<Object> getComponents() {
        int cols = row.getColumnDefinitions().size();
        List<Object> columns = new ArrayList<>(cols);
        // Note we start at index=1, because index=0 is the token
        for (int i = 1; i < cols; i++)
            columns.add(row.getObject(i));
        return columns;
    }

    @VisibleForTesting
    protected Token getToken() {
        return row.getToken(0);
    }

    public int compareTo(@NotNull PartitionKey o) {
        return getToken().compareTo(o.getToken());
    }

    public boolean equals(Object obj) {
        return this == obj || (obj instanceof PartitionKey &&  this.compareTo((PartitionKey)obj) == 0);
    }

    public int hashCode() {
        return Objects.hash(getTokenAsBigInteger());
    }

    public String toString() {
        return StreamSupport.stream(row.getColumnDefinitions().spliterator(), false)
                            .map(ColumnDefinitions.Definition::getName)
                            .map(row::getObject)
                            .filter(Objects::nonNull)
                            .map(Object::toString)
                            .collect(Collectors.joining(":"));
    }
}
