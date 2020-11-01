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

import java.util.concurrent.Callable;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RetryStrategy {
    private final static Logger logger = LoggerFactory.getLogger(RetryStrategy.class);

    /**
     * Decide whether retry is desired or not.
     * @return true to retry, see {@link #retry(Callable)}.
     *         return false to re-throw the exception.
     */
    protected abstract boolean shouldRetry();

    public final <T> T retry(Callable<T> retryable) throws Exception {
        return retryIfNot(retryable, FakeException.class);
    }

    /**
     * Retry a retryable.
     * Rethrow the exception from retryable if no more retry is permitted or the thrown exception is in the exclude list.
     */
    @SafeVarargs
    public final <T> T retryIfNot(Callable<T> retryable, Class<? extends Exception>... excludedExceptions) throws Exception {
        Function<Exception, Boolean> containsException = ex -> {
            for (Class<? extends Exception> xClass : excludedExceptions) {
                if (xClass.isInstance(ex))
                    return true;
            }
            return false;
        };
        while (true) {
            try {
                return retryable.call();
            }
            catch (Exception exception) {
                if (containsException.apply(exception) || !shouldRetry()) {
                    throw exception;
                }
                logger.warn("Retry with " + toString());
            }
        }
    }

    public static class NoRetry extends RetryStrategy {
        public final static RetryStrategy INSTANCE = new NoRetry();

        @Override
        public boolean shouldRetry() {
            return false;
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName();
        }
    }

    // The fake exception used internally.
    // No one extends it so that it is a never matched.
    private static final class FakeException extends Exception {
    }
}
