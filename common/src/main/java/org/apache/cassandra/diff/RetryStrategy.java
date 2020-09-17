package org.apache.cassandra.diff;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RetryStrategy {
    private final static Logger logger = LoggerFactory.getLogger(RetryStrategy.class);

    public RetryStrategy(JobConfiguration.RetryOptions retryOptions) {
    }

    /**
     * Decide whether retry is desired or not.
     * @return true to retry, see {@link #retry(Callable)}.
     *         return false to re-throw the exception.
     */
    protected abstract boolean shouldRetry();

    public final <T> T retry(Callable<T> retryable) throws Exception {
        while (true) {
            try {
                return retryable.call();
            }
            catch (Exception exception) {
                if (!shouldRetry()) {
                    throw exception;
                }
                logger.warn("Retry with " + toString());
            }
        }
    }

    public static class NoRetry extends RetryStrategy {
        public final static RetryStrategy INSTANCE = new NoRetry(new JobConfiguration.RetryOptions());

        public NoRetry(JobConfiguration.RetryOptions retryOptions) {
            super(retryOptions);
        }

        @Override
        public boolean shouldRetry() {
            return false;
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName();
        }
    }
}
