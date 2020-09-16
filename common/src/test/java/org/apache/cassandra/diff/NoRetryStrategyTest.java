package org.apache.cassandra.diff;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class NoRetryStrategyTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testNoRetry() throws Exception {
        RetryStrategy strategy = RetryStrategy.NoRetry.INSTANCE;
        Assert.assertFalse("NoRetry should always not retry",
                           strategy.shouldRetry());
        AtomicInteger execCount = new AtomicInteger(0);
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("fail at execution#0"); // no retry
        strategy.retry(() -> {
            throw new RuntimeException("fail at execution#" + execCount.getAndIncrement());
        });
    }
}
