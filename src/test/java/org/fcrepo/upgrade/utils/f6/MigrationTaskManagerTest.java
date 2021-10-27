/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */

package org.fcrepo.upgrade.utils.f6;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

/**
 * @author pwinckles
 */
@RunWith(MockitoJUnitRunner.class)
public class MigrationTaskManagerTest {

    private static final String INFO_FEDORA = "info:fedora";

    @Mock
    public ResourceMigrator resourceMigrator;

    private MigrationTaskManager manager;

    private ResourceInfo defaultInfo;

    private Path logPath;

    @Before
    public void setup() throws IOException {
        manager = new MigrationTaskManager(1, resourceMigrator, new ResourceInfoLogger());
        final var parent = randomId();
        defaultInfo = ResourceInfo.container(parent, join(parent, "child"), Paths.get("/"), "child");
        logPath = Paths.get("target/remaining.log");
        if (Files.exists(logPath)) {
            Files.write(logPath, new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
        }
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

        submitAndComplete(defaultInfo);

        manager.submit(defaultInfo);
    }

    @Test
    public void logInfoWhenTasksFail() throws InterruptedException {
        doThrow(new RuntimeException()).when(resourceMigrator).migrate(Mockito.any());

        submitAndComplete(defaultInfo);

        assertLogContains(defaultInfo);
    }

    @Test
    public void logInfoWhenShutdownWithOutstandingTasks() throws InterruptedException {
        doAnswer(invocation -> {
            TimeUnit.SECONDS.sleep(2);
            return new ArrayList<ResourceInfo>();
        }).when(resourceMigrator).migrate(Mockito.any());

        final var info2 = ResourceInfo.container(defaultInfo.getFullId(),
                join(defaultInfo.getFullId(), "child"), Paths.get("/"), "child");
        final var info3 = ResourceInfo.container(info2.getFullId(),
                join(info2.getFullId(), "child"), Paths.get("/"), "child");

        manager.submit(defaultInfo);
        manager.submit(info2);
        manager.submit(info3);

        TimeUnit.SECONDS.sleep(1);

        manager.shutdown();

        assertLogContains(info2, info3);
    }

    private void submitAndComplete(final ResourceInfo info) throws InterruptedException {
        manager.submit(info);
        manager.awaitCompletion();
        manager.shutdown();
    }

    private void assertLogContains(final ResourceInfo... infos) {
        final var actual = new ResourceInfoLogger().parseLog(logPath);
        assertThat(actual, containsInAnyOrder(infos));
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
