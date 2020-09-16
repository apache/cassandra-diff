package org.apache.cassandra.diff;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.diff.JobConfiguration.RetryOptions;

public class RetryStrategyFactory {
    public final static String IMPLEMENTATION_KEY = "impl";
    private final static Logger logger = LoggerFactory.getLogger(RetryStrategyFactory.class);
    private final RetryOptions retryOptions;

    public RetryStrategyFactory(RetryOptions retryOptions) {
        this.retryOptions = retryOptions;
    }

    public RetryStrategy create() {
        if (retryOptions != null) {
            return create(retryOptions);
        } else {
            logger.info("Retry is disabled.");
            return RetryStrategy.NoRetry.INSTANCE;
        }
    }

    public static RetryStrategy create(Map<String, String> parameters) {
        String implClass = parameters.get(IMPLEMENTATION_KEY);
        try {
            return (RetryStrategy) Class.forName(implClass)
                                        .getConstructor(Map.class)
                                        .newInstance(parameters);
        } catch (Exception ex) {
            logger.warn("Unable to create retry strategy. Use the default, NoRetry.", ex);
            return RetryStrategy.NoRetry.INSTANCE;
        }
    }
}
