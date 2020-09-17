package org.apache.cassandra.diff;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.cassandra.diff.ExponentialRetryStategyProvider.Exponential;
import static org.apache.cassandra.diff.ExponentialRetryStategyProvider.ExponentialRetryStrategy;

public class ExponentialRetryStrategyTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testPauseTimeIncreaseExponentially() {
        long base = 1;
        long total = 1000;
        Exponential exponential = new Exponential(base, total);
        long totalSoFar = 0;
        for (int i = 0; i < 100; i ++) {
            long actual = exponential.get(i);
            long expected = base << i;
            if (totalSoFar >= total) {
                expected = -1;
            } else {
                if (totalSoFar + expected > total) {
                    expected = total - totalSoFar; // adjust the pause time for the last valid pause.
                }
                totalSoFar += expected;
            }
            Assert.assertEquals("Exponential generates unexpected sequence at iteration#" + i, expected, actual);
        }
        Assert.assertEquals("The total pause time is not capped at totalDelayMs", total, totalSoFar);
    }

    @Test
    public void testWrongArguments() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("baseDelayMs cannot be greater than totalDelayMs");
        new Exponential(10, 1);
    }

    @Test
    public void testToString() {
        ExponentialRetryStrategy strategy = new ExponentialRetryStrategy(new JobConfiguration.RetryOptions());
        String output = strategy.toString();
        Assert.assertEquals("ExponentialRetryStrategy(baseDelayMs: 1000, totalDelayMs: 1800000, currentAttempts: 0)",
                            output);
    }

    @Test
    public void testSuccessAfterRetry() throws Exception {
        AtomicInteger retryCount = new AtomicInteger(0);
        ExponentialRetryStrategy strategy = new ExponentialRetryStrategy(retryOptions(1, 1000));
        int result = strategy.retry(() -> {
            if (retryCount.getAndIncrement() < 2) {
                throw new RuntimeException("fail");
            }
            return 1;
        });
        Assert.assertEquals(1, result);
        Assert.assertEquals(3, retryCount.get());
    }

    @Test
    public void testFailureAfterAllRetries() throws Exception {
        AtomicInteger execCount = new AtomicInteger(0);
        ExponentialRetryStrategy strategy = new ExponentialRetryStrategy(retryOptions(1, 2));
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("fail at execution#2"); // 0 based
        // the lambda runs 3 times at timestamp 0, 1, 2 and fail
        strategy.retry(() -> {
            throw new RuntimeException("fail at execution#" + execCount.getAndIncrement());
        });
    }

    private JobConfiguration.RetryOptions retryOptions(long baseDelayMs, long totalDelayMs) {
        return new JobConfiguration.RetryOptions() {{
            put(ExponentialRetryStrategy.BASE_DELAY_MS_KEY, String.valueOf(baseDelayMs));
            put(ExponentialRetryStrategy.TOTAL_DELAY_MS_KEY, String.valueOf(totalDelayMs));
        }};
    }
}
