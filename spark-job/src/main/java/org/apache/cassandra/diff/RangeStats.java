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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

public class RangeStats implements Serializable {

    private transient LongAdder matchedPartitions;
    private transient LongAdder mismatchedPartitions;
    private transient LongAdder errorPartitions;
    private transient LongAdder skippedPartitions;
    private transient LongAdder onlyInSource;
    private transient LongAdder onlyInTarget;
    private transient LongAdder matchedRows;
    private transient LongAdder matchedValues;
    private transient LongAdder mismatchedValues;

    public static RangeStats newStats() {
        return new RangeStats(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
    }

    public static RangeStats withValues(long matchedPartitions,
                                        long mismatchedPartitions,
                                        long errorPartitions,
                                        long skippedPartitions,
                                        long onlyInSource,
                                        long onlyInTarget,
                                        long matchedRows,
                                        long matchedValues,
                                        long mismatchedValues) {

        return new RangeStats(matchedPartitions,
                              mismatchedPartitions,
                              errorPartitions,
                              skippedPartitions,
                              onlyInSource,
                              onlyInTarget,
                              matchedRows,
                              matchedValues,
                              mismatchedValues);
    }

    private RangeStats(long matchedPartitions,
                       long mismatchedPartitions,
                       long errorPartitions,
                       long skippedPartitions,
                       long onlyInSource,
                       long onlyInTarget,
                       long matchedRows,
                       long matchedValues,
                       long mismatchedValues) {

        this.matchedPartitions      = new LongAdder();
        this.mismatchedPartitions   = new LongAdder();
        this.errorPartitions        = new LongAdder();
        this.skippedPartitions      = new LongAdder();
        this.onlyInSource           = new LongAdder();
        this.onlyInTarget           = new LongAdder();
        this.matchedRows            = new LongAdder();
        this.matchedValues          = new LongAdder();
        this.mismatchedValues       = new LongAdder();

        this.matchedPartitions.add(matchedPartitions);
        this.mismatchedPartitions.add(mismatchedPartitions);
        this.errorPartitions.add(errorPartitions);
        this.skippedPartitions.add(skippedPartitions);
        this.onlyInSource.add(onlyInSource);
        this.onlyInTarget.add(onlyInTarget);
        this.matchedRows.add(matchedRows);
        this.matchedValues.add(matchedValues);
        this.mismatchedValues.add(mismatchedValues);
    }

    public void matchedPartition() {
        matchedPartitions.add(1L);
    }

    public long getMatchedPartitions() {
        return matchedPartitions.sum();
    }

    public void onlyInSource() {
        onlyInSource.add(1L);
    }

    public long getOnlyInSource() {
        return onlyInSource.sum();
    }

    public void onlyInTarget() {
        onlyInTarget.add(1L);
    }

    public long getOnlyInTarget() {
        return onlyInTarget.sum();
    }

    public long getMatchedRows() {
        return matchedRows.sum();
    }

    public long getMatchedValues() {
        return matchedValues.sum();
    }

    public long getMismatchedValues() {
        return mismatchedValues.sum();
    }

    public void mismatchedPartition() {
        mismatchedPartitions.add(1L);
    }

    public long getMismatchedPartitions() {
        return mismatchedPartitions.sum();
    }

    public void skipPartition() {
        skippedPartitions.add(1L);
    }

    public long getSkippedPartitions() {
        return skippedPartitions.sum();
    }

    public void partitionError() {
        errorPartitions.add(1L);
    }

    public long getErrorPartitions() {
        return errorPartitions.sum();
    }

    public RangeStats accumulate(final PartitionStats partitionStats) {
        this.matchedRows.add(partitionStats.matchedRows);
        this.matchedValues.add(partitionStats.matchedValues);
        this.mismatchedValues.add(partitionStats.mismatchedValues);
        if (partitionStats.skipped)
            this.skippedPartitions.add(1L);

        return this;
    }

    public RangeStats accumulate(final RangeStats rangeStats) {
        this.matchedPartitions.add(rangeStats.matchedPartitions.sum());
        this.mismatchedPartitions.add(rangeStats.mismatchedPartitions.sum());
        this.errorPartitions.add(rangeStats.errorPartitions.sum());
        this.skippedPartitions.add(rangeStats.skippedPartitions.sum());
        this.onlyInSource.add(rangeStats.onlyInSource.sum());
        this.onlyInTarget.add(rangeStats.onlyInTarget.sum());
        this.matchedRows.add(rangeStats.matchedRows.sum());
        this.matchedValues.add(rangeStats.matchedValues.sum());
        this.mismatchedValues.add(rangeStats.mismatchedValues.sum());
        return this;
    }

    public boolean allMatches () {
        return onlyInSource.sum() == 0
               && errorPartitions.sum() == 0
               && onlyInTarget.sum() == 0
               && mismatchedValues.sum() == 0
               && skippedPartitions.sum() == 0;
    }

    public boolean isEmpty() {
        return matchedPartitions.sum() == 0
                && mismatchedPartitions.sum() == 0
                && errorPartitions.sum() == 0
                && skippedPartitions.sum() == 0
                && onlyInSource.sum() == 0
                && onlyInTarget.sum() == 0
                && matchedRows.sum() == 0
                && matchedValues.sum() == 0
                && mismatchedValues.sum() == 0;
    }

    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof RangeStats))
            return false;

        RangeStats other = (RangeStats)o;
        return this.matchedPartitions.sum() == other.matchedPartitions.sum()
               && mismatchedPartitions.sum() == other.mismatchedPartitions.sum()
               && errorPartitions.sum() == other.errorPartitions.sum()
               && skippedPartitions.sum() == other.skippedPartitions.sum()
               && onlyInSource.sum() == other.onlyInSource.sum()
               && onlyInTarget.sum() == other.onlyInTarget.sum()
               && matchedRows.sum() == other.matchedRows.sum()
               && matchedValues.sum() == other.matchedValues.sum()
               && mismatchedValues.sum() == other.mismatchedValues.sum();
    }

    public int hashCode() {
        return Objects.hash(matchedPartitions.sum(),
                            mismatchedPartitions.sum(),
                            errorPartitions.sum(),
                            skippedPartitions.sum(),
                            onlyInSource.sum(),
                            onlyInTarget.sum(),
                            matchedRows.sum(),
                            matchedValues.sum(),
                            mismatchedValues.sum());
    }

    public String toString() {
        return String.format("Matched Partitions - %d, " +
                             "Mismatched Partitions - %d, " +
                             "Partition Errors - %d, " +
                             "Partitions Only In Source - %d, " +
                             "Partitions Only In Target - %d, " +
                             "Skipped Partitions - %d, " +
                             "Matched Rows - %d, " +
                             "Matched Values - %d, " +
                             "Mismatched Values - %d ",
                             matchedPartitions.sum(),
                             mismatchedPartitions.sum(),
                             errorPartitions.sum(),
                             onlyInSource.sum(),
                             onlyInTarget.sum(),
                             skippedPartitions.sum(),
                             matchedRows.sum(),
                             matchedValues.sum(),
                             mismatchedValues.sum());
    }

    // For serialization

    private RangeStats() {}

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeLong(matchedPartitions.sum());
        out.writeLong(mismatchedPartitions.sum());
        out.writeLong(errorPartitions.sum());
        out.writeLong(skippedPartitions.sum());
        out.writeLong(onlyInSource.sum());
        out.writeLong(onlyInTarget.sum());
        out.writeLong(matchedRows.sum());
        out.writeLong(matchedValues.sum());
        out.writeLong(mismatchedValues.sum());
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        this.matchedPartitions      = new LongAdder();
        this.mismatchedPartitions   = new LongAdder();
        this.errorPartitions        = new LongAdder();
        this.skippedPartitions      = new LongAdder();
        this.onlyInSource           = new LongAdder();
        this.onlyInTarget           = new LongAdder();
        this.matchedRows            = new LongAdder();
        this.matchedValues          = new LongAdder();
        this.mismatchedValues       = new LongAdder();

        this.matchedPartitions.add(in.readLong());
        this.mismatchedPartitions.add(in.readLong());
        this.errorPartitions.add(in.readLong());
        this.skippedPartitions.add(in.readLong());
        this.onlyInSource.add(in.readLong());
        this.onlyInTarget.add(in.readLong());
        this.matchedRows.add(in.readLong());
        this.matchedValues.add(in.readLong());
        this.mismatchedValues.add(in.readLong());
    }
}
