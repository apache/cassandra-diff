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
import java.util.concurrent.ThreadLocalRandom;

public class DiffContext {

    public final String keyspace;
    public final TableSpec table;
    public final BigInteger startToken;
    public final BigInteger endToken;
    public final DiffCluster source;
    public final DiffCluster target;
    private final SpecificTokens specificTokens;
    private final double reverseReadProbability;

    public DiffContext(final DiffCluster source,
                       final DiffCluster target,
                       final String keyspace,
                       final TableSpec table,
                       final BigInteger startToken,
                       final BigInteger endToken,
                       final SpecificTokens specificTokens,
                       final double reverseReadProbability) {
        this.keyspace = keyspace;
        this.table = table;
        this.startToken = startToken;
        this.endToken = endToken;
        this.source = source;
        this.target = target;
        this.specificTokens = specificTokens;
        this.reverseReadProbability = reverseReadProbability;
    }

    public boolean shouldReverse() {
        return ! table.getClusteringColumns().isEmpty()
               && reverseReadProbability > ThreadLocalRandom.current().nextDouble();
    }

    public boolean isTokenAllowed(BigInteger token) {
        return specificTokens.test(token);
    }

    public static String cqlizedString(final String str) {
        boolean shouldQuote = !str.startsWith("\"");
        if (shouldQuote) {
            return "\"" + str + "\"";
        } else {
            return str;
        }
    }

    public String toString() {
        return String.format("DiffContext: [keyspace: %s, start_token: %s, end_token: %s]",
                             keyspace, startToken, endToken);
    }
}
