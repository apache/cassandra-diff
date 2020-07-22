package org.apache.cassandra.diff;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;

// Overrides methods on demand
public abstract class AbstractMockJobConfiguration implements JobConfiguration {
    private final UnsupportedOperationException uoe = new UnsupportedOperationException("Not implemented");

    @Nullable
    @Override
    public List<KeyspaceTablePair> keyspaceTables() {
        throw uoe;
    }

    @Nullable
    @Override
    public List<String> disallowedKeyspaces() {
        throw uoe;
    }

    @Override
    public int splits() {
        throw uoe;
    }

    @Override
    public int buckets() {
        throw uoe;
    }

    @Override
    public int rateLimit() {
        throw uoe;
    }

    @Override
    public Optional<UUID> jobId() {
        throw uoe;
    }

    @Override
    public int tokenScanFetchSize() {
        throw uoe;
    }

    @Override
    public int partitionReadFetchSize() {
        throw uoe;
    }

    @Override
    public int readTimeoutMillis() {
        throw uoe;
    }

    @Override
    public double reverseReadProbability() {
        throw uoe;
    }

    @Override
    public String consistencyLevel() {
        throw uoe;
    }

    @Override
    public MetadataKeyspaceOptions metadataOptions() {
        throw uoe;
    }

    @Override
    public Map<String, String> clusterConfig(String identifier) {
        throw uoe;
    }
}
