package org.apache.cassandra.diff;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;

public class ExponentialRetryStrategyProvider extends RetryStrategyProvider {
    public ExponentialRetryStrategyProvider(JobConfiguration.RetryOptions retryOptions) {
        super(retryOptions);
    }

    @Override
    public RetryStrategy get() {
        return new ExponentialRetryStrategy(retryOptions);
    }

    static class ExponentialRetryStrategy extends RetryStrategy {
        public final static String BASE_DELAY_MS_KEY = "base_delay_ms";
        public final static String TOTAL_DELAY_MS_KEY = "total_delay_ms";
        private final static String DEFAULT_BASE_DELAY_MS = String.valueOf(TimeUnit.SECONDS.toMillis(1));
        private final static String DEFAULT_TOTAL_DELAY_MS = String.valueOf(TimeUnit.MINUTES.toMillis(30));

        private final Exponential exponential;
        private int attempts = 0;

        public ExponentialRetryStrategy(JobConfiguration.RetryOptions retryOptions) {
            super(retryOptions);
            long baseDelayMs = Long.parseLong(retryOptions.getOrDefault(BASE_DELAY_MS_KEY, DEFAULT_BASE_DELAY_MS));
            long totalDelayMs = Long.parseLong(retryOptions.getOrDefault(TOTAL_DELAY_MS_KEY, DEFAULT_TOTAL_DELAY_MS));
            this.exponential = new Exponential(baseDelayMs, totalDelayMs);
        }

        @Override
        protected boolean shouldRetry() {
            long pauseTimeMs = exponential.get(attempts);
            if (pauseTimeMs > 0) {
                Uninterruptibles.sleepUninterruptibly(pauseTimeMs, TimeUnit.MILLISECONDS);
                attempts += 1;
                return true;
            }
            return false;
        }

        @Override
        public String toString() {
            return String.format("%s(baseDelayMs: %s, totalDelayMs: %s, currentAttempts: %s)",
                                 this.getClass().getSimpleName(), exponential.baseDelayMs, exponential.totalDelayMs, attempts);
        }
    }

    /**
     * Calculate the pause time exponentially, according to the attempts.
     * The total delay is capped at totalDelayMs, meaning the sum of all the previous pauses cannot exceed it.
     */
    static class Exponential {
        // base delay in ms used to calculate the next pause time
        private final long baseDelayMs;
        // total delay in ms permitted
        private final long totalDelayMs;

        Exponential(long baseDelayMs, long totalDelayMs) {
            Preconditions.checkArgument(baseDelayMs <= totalDelayMs, "baseDelayMs cannot be greater than totalDelayMs");
            this.baseDelayMs = baseDelayMs;
            this.totalDelayMs = totalDelayMs;
        }

        /**
         * Calculate the pause time based on attempts.
         * It is guaranteed that the all the pauses do not exceed totalDelayMs.
         * @param attempts, number of attempts, starts with 0.
         * @return the next pasuse time in milliseconds, or negtive if no longer allowed.
         */
        long get(int attempts) {
            long nextMaybe = baseDelayMs * (1L << attempts); // Do not care about overflow. pausedInTotal() corrects the value
            if (attempts == 0) { // first retry
                return nextMaybe;
            } else {
                long pausedInTotal = pausedInTotal(attempts);
                if (pausedInTotal < totalDelayMs) {
                    return Math.min(totalDelayMs - pausedInTotal, nextMaybe); // adjust the next pause time if possible
                }
                return -1; // the previous retries have exhausted the permits
            }
        }

        // Returns the total pause time according to the `attempts`,
        // i.e. [0, attempts), which is guaranteed to be greater than or equal to 0.
        // No overflow can happen.
        private long pausedInTotal(int attempts) {
            if (attempts >= Long.SIZE) return totalDelayMs; // take care of overflow. Such long pause time is not realistic though.
            long result = baseDelayMs * ((1L << attempts) - 1); // 2^0 + 2^1 + ... + 2^n = 2^(n+1) - 1
            return Math.min(totalDelayMs, result);
        }
    }
}
