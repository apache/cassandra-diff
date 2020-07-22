package org.apache.cassandra.diff;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;

public class Schema {
    // Filter out all system keyspaces.
    private static final Set<String> DEFAULT_FILTER = ImmutableSet.of(
        "system", "system_schema", "system_traces", "system_auth", "system_distributed", "system_virtual_schema", "system_views"
    );

    private final Set<KeyspaceTablePair> qualifiedTables;

    public Schema(Metadata metadata, JobConfiguration configuration) {
        Set<String> keyspaceFilter = getKeyspaceFilter(configuration);
        qualifiedTables = new HashSet<>();
        for (KeyspaceMetadata keyspaceMetadata : metadata.getKeyspaces()) {
            if (keyspaceFilter.contains(keyspaceMetadata.getName()))
                continue;

            for (TableMetadata tableMetadata : keyspaceMetadata.getTables()) {
                qualifiedTables.add(KeyspaceTablePair.from(tableMetadata));
            }
        }
    }

    public Schema(Set<KeyspaceTablePair> qualifiedTables) {
        this.qualifiedTables = qualifiedTables;
    }

    public Schema intersect(Schema other) {
        if (this != other) {
            Set<KeyspaceTablePair> intersection = Sets.intersection(this.qualifiedTables, other.qualifiedTables);
            return new Schema(intersection);
        }
        return this;
    }

    public List<KeyspaceTablePair> toQualifiedTableList() {
        return new ArrayList<>(qualifiedTables);
    }

    public int size() {
        return qualifiedTables.size();
    }

    @VisibleForTesting
    public static Set<String> getKeyspaceFilter(JobConfiguration configuration) {
        List<String> disallowedKeyspaces = configuration.disallowedKeyspaces();
        if (null == disallowedKeyspaces) {
            return DEFAULT_FILTER;
        } else {
            return Sets.union(DEFAULT_FILTER, ImmutableSet.copyOf(disallowedKeyspaces));
        }
    }
}
