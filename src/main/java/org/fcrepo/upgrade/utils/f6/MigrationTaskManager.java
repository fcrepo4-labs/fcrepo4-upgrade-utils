/*
 * Licensed to DuraSpace under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.
 *
 * DuraSpace licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fcrepo.upgrade.utils.f6;

import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Task manager for coordinating resource migration tasks.
 *
 * @author pwinckles
 */
public class MigrationTaskManager {

    private static final Logger LOGGER = getLogger(MigrationTaskManager.class);

    private final ExecutorService executorService;
    private final BlockingQueue<Runnable> workQueue;
    private final ResourceMigrator resourceMigrator;
    private final AtomicLong count;
    private final Object lock;
    private final ResourceInfoLogger infoLogger;
    private final Field callableField;

    private boolean shutdown = false;

    /**
     * @param threadCount the number of threads to use
     * @param resourceMigrator the object responsible for performing the migration
     * @param infoLogger the logger to use to record failed migrations
     */
    public MigrationTaskManager(final int threadCount,
                                final ResourceMigrator resourceMigrator,
                                final ResourceInfoLogger infoLogger) {
        this.workQueue = new LinkedBlockingQueue<>();
        this.executorService = new ThreadPoolExecutor(threadCount, threadCount,
                0L, TimeUnit.MILLISECONDS,
                workQueue);
        this.resourceMigrator = resourceMigrator;
        this.count = new AtomicLong(0);
        this.lock = new Object();
        this.infoLogger = infoLogger;

        // Dirty hack to get original callable
        try {
            callableField = FutureTask.class.getDeclaredField("callable");
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        callableField.setAccessible(true);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (!shutdown) {
                LOGGER.info("Shutting down...");
                shutdown();
            }
        }));
    }

    /**
     * Submits a new resource to be migrated. This method returns immediately, and the resource is migrated
     * asynchronously.
     *
     * @param info the resource to migrate
     */
    public void submit(final ResourceInfo info) {
        executorService.submit(new TaskWrapper(info,
                new MigrateResourceTask(this, resourceMigrator, infoLogger, info)));

        count.incrementAndGet();
    }

    /**
     * Blocks until all migration tasks are complete. Note, this does not prevent additional tasks from being submitted.
     * It simply waits until the queue is empty.
     *
     * @throws InterruptedException on interrupt
     */
    public void awaitCompletion() throws InterruptedException {
        if (count.get() == 0) {
            return;
        }

        synchronized (lock) {
            while (count.get() > 0) {
                lock.wait();
            }
        }
    }

    /**
     * Shutsdown the executor service, drains all remaining tasks to the log, and waits for the in progress
     * tasks to complete.
     */
    public synchronized void shutdown() {
        if (!shutdown) {
            shutdown = true;

            try {
                executorService.shutdown();
                drainTasks();
                LOGGER.info("Waiting for any inflight tasks to complete...");
                if (!executorService.awaitTermination(5, TimeUnit.MINUTES)) {
                    LOGGER.warn("Failed to shutdown executor service cleanly after 5 minutes of waiting");
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                LOGGER.warn("Failed to shutdown executor service cleanly");
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                resourceMigrator.close();
            }
        }
    }

    /**
     * Empties the queue of unprocessed tasks and adds them to the remaining log
     */
    private void drainTasks() {
        final List<Runnable> remaining = new ArrayList<>(workQueue.size());
        workQueue.drainTo(remaining);
        remaining.forEach(task -> {
            try {
                final TaskWrapper inner = (TaskWrapper) callableField.get(task);
                infoLogger.log(inner.info);
            } catch (Exception e) {
                LOGGER.warn("Failed to extract unprocessed resource info", e);
            }
        });
    }

    private class TaskWrapper implements Callable<Void> {
        private final ResourceInfo info;
        private final Runnable runnable;

        private TaskWrapper(final ResourceInfo info, final Runnable runnable) {
            this.info = info;
            this.runnable = runnable;
        }

        @Override
        public Void call() {
            try {
                runnable.run();
                return null;
            } finally {
                count.decrementAndGet();
                synchronized (lock) {
                    lock.notifyAll();
                }
            }
        }
    }

}
