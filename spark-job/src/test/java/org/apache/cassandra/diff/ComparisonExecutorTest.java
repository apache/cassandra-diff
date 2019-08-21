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

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.google.common.util.concurrent.*;
import org.junit.Test;

import com.codahale.metrics.*;

import static org.apache.cassandra.diff.TestUtils.assertThreadWaits;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;

public class ComparisonExecutorTest {

    @Test
    public void submitBlocksWhenMaxTasksExceeded() throws Exception {
        // submit maxTasks, then assert that further submission blocks until tasks are processed
        int maxTasks = 3;
        MetricRegistry metrics = metrics();
        final ComparisonExecutor executor = new ComparisonExecutor(executor(1), maxTasks, metrics);
        Gauge waitingToSubmit = metrics.getGauges().get("BlockedTasks");
        assertEquals(0, waitingToSubmit.getValue());

        final AtomicInteger successful = new AtomicInteger(0);
        final Consumer<Integer> onSuccess = (i) -> successful.incrementAndGet();
        final AtomicInteger failed = new AtomicInteger(0);
        final Consumer<Throwable> onError = (t) -> failed.incrementAndGet();
        final Phaser phaser = new Phaser(1);

        BlockingTask[] tasks = new BlockingTask[5];
        for (int i=0; i<5; i++)
            tasks[i] = new BlockingTask(i);

        // Ensure that the submission itself does not block before the max number of tasks are submitted
        executor.submit(tasks[0], onSuccess, onError, phaser);
        executor.submit(tasks[1], onSuccess, onError, phaser);
        executor.submit(tasks[2], onSuccess, onError, phaser);
        assertEquals(0, waitingToSubmit.getValue());

        // Now submit another pair of tasks which should block as the executor is fully occupied
        final CountDownLatch latch = new CountDownLatch(2);
        Thread t1 = new Thread(() -> { latch.countDown(); executor.submit(tasks[3], onSuccess, onError, phaser);});
        Thread t2 = new Thread(() -> { latch.countDown(); executor.submit(tasks[4], onSuccess, onError, phaser);});
        t1.start();
        t2.start();
        // wait for both to attempt submission
        latch.await();
        assertThreadWaits(t1);
        assertThreadWaits(t2);
        assertEquals(2, waitingToSubmit.getValue());

        // Let the first waiting task complete, which should allow t1 to complete its submission
        tasks[0].latch.countDown();
        t1.join();
        // the second submission should still be waiting on a slot
        assertThreadWaits(t2);
        assertEquals(1, waitingToSubmit.getValue());

        // Let another task complete, allowing t2 to complete its submission
        tasks[1].latch.countDown();
        t2.join();
        assertEquals(0, waitingToSubmit.getValue());

        // Let all tasks complete, wait for them to do so then verify counters
        for (int i=2; i<=4; i++)
            tasks[i].latch.countDown();

        phaser.arriveAndAwaitAdvance();
        assertEquals(5, successful.get());
        assertEquals(0, failed.get());
    }

    @Test
    public void handleTaskFailure() {
        // Ensure that the failure callback is fired, a permit for task submission
        // returned and the phaser notified when a task throws
        int maxTasks = 5;
        MetricRegistry metrics = metrics();
        ComparisonExecutor executor = new ComparisonExecutor(executor(1), maxTasks, metrics);
        Gauge availableSlots = metrics.getGauges().get("AvailableSlots");
        final AtomicInteger successful = new AtomicInteger(0);
        final Consumer<Integer> onSuccess = (i) -> successful.incrementAndGet();
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        final Consumer<Throwable> onError = thrown::set;
        final Phaser phaser = new Phaser(1);

        assertEquals(maxTasks, availableSlots.getValue());

        RuntimeException toThrow = new RuntimeException("FAIL");
        BlockingTask task = new BlockingTask(0, toThrow);
        executor.submit(task, onSuccess, onError, phaser);
        assertEquals(maxTasks - 1, availableSlots.getValue());

        assertEquals(2, phaser.getUnarrivedParties());
        task.latch.countDown();

        phaser.arriveAndAwaitAdvance();
        assertEquals(maxTasks, availableSlots.getValue());
        assertEquals(0, successful.get());
        assertEquals(toThrow, thrown.get());
    }

    @Test
    public void handleUncaughtExceptionInFailureCallback() {
        // Ensure that if the failure callback throws, a permit for submission is
        // still returned and the phaser notified
        int maxTasks = 5;
        MetricRegistry metrics = metrics();
        ComparisonExecutor executor = new ComparisonExecutor(executor(1), maxTasks, metrics);
        Gauge availableSlots = metrics.getGauges().get("AvailableSlots");
        final AtomicInteger successful = new AtomicInteger(0);
        final AtomicInteger failures = new AtomicInteger(0);
        final Consumer<Integer> onSuccess = (i) -> successful.incrementAndGet();
        final Consumer<Throwable> onError =  (t) -> { failures.incrementAndGet(); throw new RuntimeException("UNCAUGHT"); };
        final Phaser phaser = new Phaser(1);

        assertEquals(maxTasks, availableSlots.getValue());

        RuntimeException toThrow = new RuntimeException("FAIL");
        try {
            onError.accept(toThrow);
            fail("Failure callback should throw RuntimeException");
        } catch (RuntimeException e) {
            // expected - reset failure count
            failures.set(0);
        }

        BlockingTask task = new BlockingTask(0, toThrow);
        executor.submit(task, onSuccess, onError, phaser);
        assertEquals(maxTasks - 1, availableSlots.getValue());

        assertEquals(2, phaser.getUnarrivedParties());
        task.latch.countDown();

        phaser.arriveAndAwaitAdvance();
        assertEquals(maxTasks, availableSlots.getValue());
        assertEquals(0, successful.get());
        assertEquals(1, failures.get());
    }

    @Test
    public void handleUncaughtExceptionInSuccessCallback() {
        // Ensure that if the success callback throws, a permit for submission is
        // still returned and the phaser notified
        int maxTasks = 5;
        MetricRegistry metrics = metrics();
        ComparisonExecutor executor = new ComparisonExecutor(executor(1), maxTasks, metrics);
        Gauge availableSlots = metrics.getGauges().get("AvailableSlots");
        final AtomicInteger successful = new AtomicInteger(0);
        final AtomicInteger failures = new AtomicInteger(0);
        final Consumer<Integer> onSuccess = (i) ->  { successful.incrementAndGet(); throw new RuntimeException("UNCAUGHT"); };
        final Consumer<Throwable> onError =  (t) -> failures.incrementAndGet();
        final Phaser phaser = new Phaser(1);

        assertEquals(maxTasks, availableSlots.getValue());
        try {
            onSuccess.accept(0);
            fail("Success callback should throw RuntimeException");
        } catch (RuntimeException e) {
            // expected - reset failure count
            successful.set(0);
        }

        BlockingTask task = new BlockingTask(0);
        executor.submit(task, onSuccess, onError, phaser);
        assertEquals(maxTasks - 1, availableSlots.getValue());

        assertEquals(2, phaser.getUnarrivedParties());
        task.latch.countDown();

        phaser.arriveAndAwaitAdvance();
        assertEquals(maxTasks, availableSlots.getValue());
        assertEquals(1, successful.get());
        assertEquals(0, failures.get());
    }

    @Test
    public void handleRejectedExecutionException() {
        // In the case that the underlying ExecutorService rejects a task submission, a permit
        // should be returned and the phaser notified
        int maxTasks = 5;
        final AtomicInteger successful = new AtomicInteger(0);
        final AtomicInteger failures = new AtomicInteger(0);
        final AtomicInteger rejections = new AtomicInteger(0);
        final Consumer<Integer> onSuccess = (i) ->  successful.incrementAndGet();
        final Consumer<Throwable> onError =  (t) -> failures.incrementAndGet();

        MetricRegistry metrics = metrics();
        ExecutorService rejectingExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                                                                   new LinkedBlockingQueue<>(1),
                                                                   (r, executor) -> { rejections.incrementAndGet();
                                                                                      throw new RejectedExecutionException("REJECTED");});

        ComparisonExecutor executor = new ComparisonExecutor(MoreExecutors.listeningDecorator(rejectingExecutor), maxTasks, metrics);
        Gauge availableSlots = metrics.getGauges().get("AvailableSlots");
        final Phaser phaser = new Phaser(1);

        // Submit an initial pair of tasks to ensure that the underlying work queue is full
        BlockingTask t0 = new BlockingTask(0);
        BlockingTask t1 = new BlockingTask(1);
        executor.submit(t0, onSuccess, onError, phaser);
        executor.submit(t1, onSuccess, onError, phaser);
        assertEquals(3, phaser.getUnarrivedParties());
        assertEquals(maxTasks - 2, availableSlots.getValue());

        // Submit a third task which will be rejected by the executor service
        executor.submit(new BlockingTask(2), onSuccess, onError, phaser);
        t0.latch.countDown();
        t1.latch.countDown();

        phaser.arriveAndAwaitAdvance();
        assertEquals(maxTasks, availableSlots.getValue());
        assertEquals(2, successful.get());
        assertEquals(1, failures.get());
        assertEquals(1, rejections.get());
    }

    class BlockingTask implements Callable<Integer> {

        final int id;
        final Exception e;
        final CountDownLatch latch;

        BlockingTask(int id) {
            this(id, null);
        }

        BlockingTask(int id, Exception toThrow) {
            this.id = id;
            this.e = toThrow;
            this.latch = new CountDownLatch(1);
        }

        public Integer call() throws Exception {
            latch.await();
            if (e != null)
                throw e;
            return id;
        }
    }

    private static ListeningExecutorService executor(int threads) {
        return MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(threads,
                                         new ThreadFactoryBuilder().setNameFormat("partition-comparison-%d")
                                                                   .setDaemon(true)
                                                                   .build()));
    }

    private static MetricRegistry metrics() {
        return new MetricRegistry();
    }
}

