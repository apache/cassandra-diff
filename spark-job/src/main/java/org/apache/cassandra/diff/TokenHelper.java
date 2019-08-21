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

public enum TokenHelper {

    MURMUR3 {
        @Override
        public BigInteger min() {
            return BigInteger.valueOf(Long.MIN_VALUE);
        }

        @Override
        public BigInteger max() {
            return BigInteger.valueOf(Long.MAX_VALUE);
        }

        @Override
        public Object forBindParam(BigInteger token) {
            return token.longValue();
        }
    },

    RANDOM {
        @Override
        public BigInteger min() {
            return BigInteger.ONE.negate();
        }

        @Override
        public BigInteger max() {
            return BigInteger.valueOf(2).pow(127).subtract(BigInteger.ONE);
        }

        @Override
        public Object forBindParam(BigInteger token) {
            return token;
        }
    };

    public abstract BigInteger min();
    public abstract BigInteger max();
    public abstract Object forBindParam(BigInteger token);

    public static TokenHelper forPartitioner(String partitionerName) {
        if (partitionerName.endsWith("Murmur3Partitioner")) return MURMUR3;
        else if (partitionerName.endsWith("RandomPartitioner")) return RANDOM;
        else throw new IllegalArgumentException("Unsupported Partitioner :" + partitionerName);
    }
}
