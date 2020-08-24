package org.apache.cassandra.diff;

import java.util.concurrent.Callable;

import org.apache.cassandra.diff.DiffCluster.Type;

/**
 * Wraps the cause with the exception source indicator, {@param type} of the cluster.
 * It is used to distinguish driver exceptions among testing clusters.
 */
public class ClusterSourcedException extends RuntimeException {
    public final Type exceptionSource;

    ClusterSourcedException(Type exceptionSource, Throwable cause) {
        super(cause);
        this.exceptionSource = exceptionSource;
    }

    public static <T> T catches(Type exceptionSource, Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception ex) {
            throw new ClusterSourcedException(exceptionSource, ex);
        }
    }

    @Override
    public String getMessage() {
        return String.format("from %s - %s", exceptionSource.name(), super.getMessage());
    }
}
