/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.diff;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides new RetryStrategy instances.
 * Use abstract class instead of interface in order to retain the referece to retryOptions;
 */
public abstract class RetryStrategyProvider implements Serializable {
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
