package org.apache.cassandra.diff;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.cassandra.diff.ExponentialRetryStrategyProvider.Exponential;
import static org.apache.cassandra.diff.ExponentialRetryStrategyProvider.ExponentialRetryStrategy;

public class ExponentialRetryStrategyTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testPauseTimeIncreaseExponentially() {
        long base = 10;
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
        ExponentialRetryStrategyProvider provider = new ExponentialRetryStrategyProvider(new JobConfiguration.RetryOptions());
        String output = provider.get().toString();
        Assert.assertEquals("ExponentialRetryStrategy(baseDelayMs: 1000, totalDelayMs: 1800000, currentAttempts: 0)",
                            output);
    }

    @Test
    public void testSuccessAfterRetry() throws Exception {
        AtomicInteger retryCount = new AtomicInteger(0);
        ExponentialRetryStrategy strategy = new ExponentialRetryStrategy(1, 1000);
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
        ExponentialRetryStrategy strategy = new ExponentialRetryStrategy(1, 2);
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("fail at execution#2"); // 0 based
        // the lambda runs 3 times at timestamp 0, 1, 2 and fail
        strategy.retry(() -> {
            throw new RuntimeException("fail at execution#" + execCount.getAndIncrement());
        });
    }

    @Test
    public void testOverflowPrevention() {
        Random rand = new Random();
        for (int i = 0; i < 1000; i++) {
            long base = rand.nextInt(100000) + 1; // [1, 100000]
            int leadingZeros = Long.numberOfLeadingZeros(base);
            Exponential exponential = new Exponential(base, Long.MAX_VALUE);
            Assert.assertTrue("The last attempt that still generate valid pause time. Failed with base: " + base,
                              exponential.get(leadingZeros - 1) > 0);
            Assert.assertEquals("Failed with base: " + base, -1, exponential.get(leadingZeros));
        }
    }

    @Test
    public void testNotMatchAndRetryWithRetryIfNot() {
        AtomicInteger execCount = new AtomicInteger(0);
        ExponentialRetryStrategy strategy = new ExponentialRetryStrategy(1, 5);
        // run the code and retry since the thrown exception does not match with the exclude list
        try {
            strategy.retryIfNot(() -> {
                execCount.getAndIncrement();
                throw new IllegalStateException();
            }, IllegalArgumentException.class, UnsupportedOperationException.class);
        }
        catch (Exception ex) {
            Assert.assertSame(IllegalStateException.class, ex.getClass());
            Assert.assertEquals(4, execCount.get());
        }
    }

    @Test
    public void testMatchAndRetryWithRetryIfNot() {
        AtomicInteger execCount = new AtomicInteger(0);
        ExponentialRetryStrategy strategy = new ExponentialRetryStrategy(1, 2);
        // run the code and not retry since the thrown exception matches the exclude list
        try {
            strategy.retryIfNot(() -> {
                execCount.getAndIncrement();
                throw new IllegalStateException();
            }, RuntimeException.class);
        }
        catch (Exception ex) {
            Assert.assertSame(IllegalStateException.class, ex.getClass());
            Assert.assertEquals(1, execCount.get());
        }
    }

    private JobConfiguration.RetryOptions retryOptions(long baseDelayMs, long totalDelayMs) {
        return new JobConfiguration.RetryOptions() {{
            put(ExponentialRetryStrategy.BASE_DELAY_MS_KEY, String.valueOf(baseDelayMs));
            put(ExponentialRetryStrategy.TOTAL_DELAY_MS_KEY, String.valueOf(totalDelayMs));
        }};
    }
}
