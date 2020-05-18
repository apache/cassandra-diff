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
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

/**
 * Wrapper for an ExecutorService which provides backpressure by blocking on submission when its
 * task queue is full.
 *
 * The internal ListeningExecutorService is instantiated with an unbounded work queue, but
 * this class uses a Semaphore to ensure that this queue cannot grow unreasonably. By default,
 * the Semaphore has 2x as many permits as the executor thread pool has threads, so at most
 * 2 x maxConcurrentTasks may be submitted before producers are blocked. The submit method
 * also adds success/failure callbacks which return the permits, enabling producers to make
 * progress at a manageable rate.
 *
 * Callers of the submit method also provide a Phaser, which they can use to ensure that any
 * tasks *they themselves have submitted* are completed before they proceed. This allows multiple
 * callers to submit tasks to the same ComparisonExecutor, but only wait for their own to complete
 * before moving onto the next stage of processing. Managing the increment and decrement of pending
 * tasks via the Phaser is handled transparently by ComparisonExecutor, so callers should not do
 * this externally.
 *
 * Submitters also provide callbacks to be run on either successful execution or failure of the
 * task. These callbacks are executed on the same thread as the task itself, which callers should bear
 * in mind when constructing them.
 *
 */
public class ComparisonExecutor {

    private final ListeningExecutorService executor;
    private final Semaphore semaphore;

    static ComparisonExecutor newExecutor(int maxConcurrentTasks, MetricRegistry metricRegistry) {
        return new ComparisonExecutor(
            MoreExecutors.listeningDecorator(
                Executors.newFixedThreadPool(maxConcurrentTasks,
                                             new ThreadFactoryBuilder().setNameFormat("partition-comparison-%d")
                                                                       .setDaemon(true)
                                                                       .build())),
            maxConcurrentTasks * 2,
            metricRegistry);
    }

    @VisibleForTesting
    ComparisonExecutor(ListeningExecutorService executor, int maxTasks, MetricRegistry metrics) {
        this.executor = executor;
        this.semaphore = new Semaphore(maxTasks);
        if (metrics != null) {
            metrics.register("BlockedTasks", (Gauge) semaphore::getQueueLength);
            metrics.register("AvailableSlots", (Gauge) semaphore::availablePermits);
        }
    }

    public <T> void submit(final Callable<T> callable,
                           final Consumer<T> onSuccess,
                           final Consumer<Throwable> onError,
                           final Phaser phaser) {

        phaser.register();
        semaphore.acquireUninterruptibly();
        try {
            Futures.addCallback(executor.submit(callable), new FutureCallback<T>() {
                public void onSuccess(T result) {
                    fireThenReleaseAndArrive(onSuccess, result, phaser);
                }

                public void onFailure(Throwable t) {
                    fireThenReleaseAndArrive(onError, t, phaser);
                }
            }, MoreExecutors.directExecutor());

        } catch (RejectedExecutionException e) {
            fireThenReleaseAndArrive(onError, e, phaser);
        }

    }

    private <T> void fireThenReleaseAndArrive(Consumer<T> callback, T argument, Phaser phaser) {
        try {
            callback.accept(argument);
        } finally {
            semaphore.release();
            phaser.arriveAndDeregister();
        }
    }

    public void shutdown() {
        executor.shutdown();
    }
}
