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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public interface JobConfiguration extends Serializable {

    default boolean shouldAutoDiscoverTables() {
        List<KeyspaceTablePair> list = keyspaceTables();
        return null == list || list.isEmpty();
    }

    /**
     * @return qualified tables defined in the configuration. Return null if not defined.
     */
    @Nullable List<KeyspaceTablePair> keyspaceTables();

    /**
     * @return a list of keyspace names that are disallowed for comparison. Return null if not defined.
     */
    @Nullable List<String> disallowedKeyspaces();

    /**
     * @return filtered qualified tables based on the keyspaceTables and disallowedKeypsaces defined.
     *         Return null if keyspaceTables is not defined.
     */
    @Nullable default List<KeyspaceTablePair> filteredKeyspaceTables() {
        List<String> disallowedKeyspaces = disallowedKeyspaces();
        List<KeyspaceTablePair> list = keyspaceTables();
        if (disallowedKeyspaces != null && !disallowedKeyspaces.isEmpty() && list != null && !list.isEmpty()) {
            Set<String> filter = new HashSet<>(disallowedKeyspaces);
            return list.stream().filter(t -> !filter.contains(t.keyspace)).collect(Collectors.toList());
        } else {
            return list;
        }
    }

    int splits();

    int buckets();

    // rate limit is provided as a global limit - this is how many q/s we guess that the src clusters can take in total
    int rateLimit();

    default SpecificTokens specificTokens() {
        return SpecificTokens.NONE;
    }

    Optional<UUID> jobId();

    int tokenScanFetchSize();

    int partitionReadFetchSize();

    int readTimeoutMillis();

    double reverseReadProbability();

    String consistencyLevel();

    MetadataKeyspaceOptions metadataOptions();

    /**
     * Sampling probability ranges from 0-1 which decides how many partitions are to be diffed using probabilistic diff
     * default value is 1 which means all the partitions are diffed
     * @return partitionSamplingProbability
     */
    double partitionSamplingProbability();

    /**
     * Contains the options that specify the retry strategy for retrieving data at the application level.
     * Note that it is different than cassandra java driver's {@link com.datastax.driver.core.policies.RetryPolicy},
     * which is evaluated at the Netty worker threads.
     */
    RetryOptions retryOptions();

    Map<String, String> clusterConfig(String identifier);

    // Just an alias
    public static class RetryOptions extends HashMap<String, String> {
    }

}
