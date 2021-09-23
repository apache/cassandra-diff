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

import java.io.InputStream;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.CustomClassLoaderConstructor;

public class YamlJobConfiguration implements JobConfiguration {
    public int splits = 10000;
    public List<KeyspaceTablePair> keyspace_tables;
    public List<String> disallowed_keyspaces;
    public int buckets = 100;
    public int rate_limit = 10000;
    public String job_id = null;
    public int token_scan_fetch_size;
    public int partition_read_fetch_size;
    public int read_timeout_millis;
    public double reverse_read_probability;
    public String consistency_level = "ALL";
    public MetadataKeyspaceOptions metadata_options;
    public Map<String, Map<String, String>> cluster_config;
    public String specific_tokens = null;
    public String disallowed_tokens = null;
    public RetryOptions retry_options;
    public double partition_sampling_probability = 1;

    public static YamlJobConfiguration load(InputStream inputStream) {
        Yaml yaml = new Yaml(new CustomClassLoaderConstructor(YamlJobConfiguration.class,
                                                              Thread.currentThread().getContextClassLoader()));
        return yaml.loadAs(inputStream, YamlJobConfiguration.class);
    }

    public List<KeyspaceTablePair> keyspaceTables() {
        return keyspace_tables;
    }

    public List<String> disallowedKeyspaces() {
        return disallowed_keyspaces;
    }

    public int splits() {
        return splits;
    }

    public int buckets() {
        return buckets;
    }

    public int rateLimit() {
        return rate_limit;
    }

    public Optional<UUID> jobId() {
        return job_id == null ? Optional.empty() : Optional.of(UUID.fromString(job_id));
    }

    public int tokenScanFetchSize() {
        return token_scan_fetch_size;
    }

    public int partitionReadFetchSize() {
        return partition_read_fetch_size;
    }

    public int readTimeoutMillis() {
        return read_timeout_millis;
    }

    public double reverseReadProbability() {
        return reverse_read_probability;
    }

    public String consistencyLevel() {
        return consistency_level;
    }

    public MetadataKeyspaceOptions metadataOptions() {
        return metadata_options;
    }

    @Override
    public double partitionSamplingProbability() {
        return partition_sampling_probability;
    }

    public RetryOptions retryOptions() {
        return retry_options;
    }

    public Map<String, String> clusterConfig(String identifier) {
        return cluster_config.get(identifier);
    }

    public SpecificTokens specificTokens() {

        if (disallowed_tokens != null && specific_tokens != null)
            throw new RuntimeException("Cannot specify both disallowed and specific tokens");

        if (disallowed_tokens != null) {
            return new SpecificTokens(toTokens(disallowed_tokens), SpecificTokens.Modifier.REJECT);
        } else if (specific_tokens != null) {
            return new SpecificTokens(toTokens(specific_tokens), SpecificTokens.Modifier.ACCEPT);
        }
        return SpecificTokens.NONE;
    }

    public String toString() {
        return "YamlJobConfiguration{" +
               "splits=" + splits +
               ", keyspace_tables=" + keyspace_tables +
               ", buckets=" + buckets +
               ", rate_limit=" + rate_limit +
               ", partition_sampling_probability=" + partition_sampling_probability +
               ", job_id='" + job_id + '\'' +
               ", token_scan_fetch_size=" + token_scan_fetch_size +
               ", partition_read_fetch_size=" + partition_read_fetch_size +
               ", read_timeout_millis=" + read_timeout_millis +
               ", reverse_read_probability=" + reverse_read_probability +
               ", consistency_level='" + consistency_level + '\'' +
               ", metadata_options=" + metadata_options +
               ", cluster_config=" + cluster_config +
               '}';
    }

    private static Set<BigInteger> toTokens(String str) {
        Set<BigInteger> tokens = new HashSet<>();
        for (String token : str.split(",")) {
            token = token.trim();
            tokens.add(new BigInteger(token));
        }
        return tokens;
    }

}
