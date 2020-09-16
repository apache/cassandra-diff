package org.apache.cassandra.diff;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides new RetryStrategy instances.
 * Use abstract class instead of interface in order to retain the referece to retryOptions;
 */
public abstract class RetryStrategyProvider {
    protected final JobConfiguration.RetryOptions retryOptions;

    public RetryStrategyProvider(JobConfiguration.RetryOptions retryOptions) {
        this.retryOptions = retryOptions;
    }

    /**
     * Create a new instance of RetryStrategy.
     */
    public abstract RetryStrategy get();


    public final static String IMPLEMENTATION_KEY = "impl";
    private final static Logger logger = LoggerFactory.getLogger(RetryStrategyProvider.class);

    /**
     * Create a RetryStrategyProvider based on {@param retryOptions}.
     */
    public static RetryStrategyProvider create(JobConfiguration.RetryOptions retryOptions) {
        try {
            String implClass = retryOptions.get(IMPLEMENTATION_KEY);
            return (RetryStrategyProvider) Class.forName(implClass)
                                                .getConstructor(JobConfiguration.RetryOptions.class)
                                                .newInstance(retryOptions);
        } catch (Exception ex) {
            logger.warn("Unable to create RetryStrategyProvider. Use the default provider, NoRetry.", ex);

            return new RetryStrategyProvider(retryOptions) {
                @Override
                public RetryStrategy get() {
                    return RetryStrategy.NoRetry.INSTANCE;
                }
            };
        }
    }
}
