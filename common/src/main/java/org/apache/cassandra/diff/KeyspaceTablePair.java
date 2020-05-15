package org.apache.cassandra.diff;

import java.io.Serializable;
import java.util.Objects;

import com.google.common.base.MoreObjects;

import com.datastax.driver.core.TableMetadata;

public final class KeyspaceTablePair implements Serializable {
    public final String keyspace;
    public final String table;

    public static KeyspaceTablePair from(TableMetadata tableMetadata) {
        return new KeyspaceTablePair(tableMetadata.getKeyspace().getName(), tableMetadata.getName());
    }

    // Used by Yaml loader
    public KeyspaceTablePair(String input) {
        String[] parts = input.trim().split("\\.");
        assert parts.length == 2 : "Invalid keyspace table pair format";
        assert parts[0].length() > 0;
        assert parts[1].length() > 0;

        this.keyspace = parts[0];
        this.table = parts[1];
    }

    public KeyspaceTablePair(String keyspace, String table) {
        this.keyspace = keyspace;
        this.table = table;
    }

    public String toCqlValueString() {
        return String.format("%s.%s", keyspace, table);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("keyspace", keyspace)
                          .add("table", table)
                          .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyspace, table);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass())
            return false;
        KeyspaceTablePair that = (KeyspaceTablePair) obj;
        return Objects.equals(keyspace, that.keyspace)
               && Objects.equals(table, that.table);
    }
}
