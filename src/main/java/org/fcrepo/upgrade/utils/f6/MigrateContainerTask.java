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

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author pwinckles
 */
public class MigrateContainerTask implements Runnable {

    private static final Logger LOGGER = getLogger(MigrateContainerTask.class);

    private final MigrationTaskManager taskManager;
    private final ContainerMigrator containerMigrator;
    private final ResourceInfo info;

    public MigrateContainerTask(final MigrationTaskManager taskManager,
                                final ContainerMigrator containerMigrator,
                                final ResourceInfo info) {
        this.taskManager = taskManager;
        this.containerMigrator = containerMigrator;
        this.info = info;
    }

    @Override
    public void run() {
        try {
            final var children = containerMigrator.migrateContainer(info);
            children.forEach(taskManager::submit);
            // TODO Failures could be logged to a file for reprocessing at a later date
        } catch (UnsupportedOperationException e) {
            // This is thrown when a resource is encountered that is not currently handled
            LOGGER.error(e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.error("Failed to process {}", info, e);
        }
    }

}
