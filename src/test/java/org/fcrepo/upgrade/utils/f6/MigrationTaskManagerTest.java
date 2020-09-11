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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

/**
 * @author pwinckles
 */
@RunWith(MockitoJUnitRunner.class)
public class MigrationTaskManagerTest {

    private static final String INFO_FEDORA = "info:fedora";

    @Mock
    public ResourceMigrator resourceMigrator;

    private MigrationTaskManager manager;
    private ExecutorService executorService;

    private ResourceInfo defaultInfo;

    @Before
    public void setup() {
        executorService = Executors.newSingleThreadExecutor();
        manager = new MigrationTaskManager(executorService, resourceMigrator);
        final var parent = randomId();
        defaultInfo = ResourceInfo.container(parent, join(parent, "child"), Paths.get("/"), "child");
    }

    @Test
    public void blockUntilAllTasksFinish() throws InterruptedException {
        final var count = new AtomicInteger(0);

        doAnswer(invocation -> {
            TimeUnit.SECONDS.sleep(2);
            count.incrementAndGet();
            return new ArrayList<ResourceInfo>();
        }).when(resourceMigrator).migrate(Mockito.any());

        manager.submit(defaultInfo);
        manager.submit(defaultInfo);
        manager.submit(defaultInfo);

        assertNotEquals(3, count.get());
        manager.awaitCompletion();
        assertEquals(3, count.get());
    }

    @Test(expected = RejectedExecutionException.class)
    public void rejectTaskWhenShutdown() throws InterruptedException {
        doReturn(new ArrayList<ResourceInfo>()).when(resourceMigrator).migrate(Mockito.any());

        manager.submit(defaultInfo);
        manager.awaitCompletion();
        manager.shutdown();

        manager.submit(defaultInfo);
    }

    private String randomId() {
        return id(UUID.randomUUID().toString());
    }

    private String id(final String value) {
        return join(INFO_FEDORA, value);
    }

    private String join(final String left, final String right) {
        return left + "/" + right;
    }

}
