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
 * A task for migrating a resource to F6.
 *
 * @author pwinckles
 */
public class MigrateResourceTask implements Runnable {

    private static final Logger LOGGER = getLogger(MigrateResourceTask.class);

    private final MigrationTaskManager taskManager;
    private final ResourceMigrator resourceMigrator;
    private final ResourceInfoLogger infoLogger;
    private final ResourceInfo info;

    /**
     * @param taskManager the task manager that is coordinating migration tasks
     * @param resourceMigrator the object responsible for performing the migration
     * @param infoLogger the logger to use to record failed migrations
     * @param info the resource to be migrated
     */
    public MigrateResourceTask(final MigrationTaskManager taskManager,
                               final ResourceMigrator resourceMigrator,
                               final ResourceInfoLogger infoLogger,
                               final ResourceInfo info) {
        this.taskManager = taskManager;
        this.resourceMigrator = resourceMigrator;
        this.infoLogger = infoLogger;
        this.info = info;
    }

    @Override
    public void run() {
        try {
            final var children = resourceMigrator.migrate(info);

            for (final var child : children) {
                try {
                    taskManager.submit(child);
                } catch (RuntimeException e) {
                    LOGGER.warn("Failed to queue {} for migration", child.getFullId());
                    infoLogger.log(child);
                }
            }
        } catch (UnsupportedOperationException e) {
            // This is thrown when a resource is encountered that is not currently handled
            LOGGER.error(e.getMessage());
            infoLogger.log(info);
        } catch (RuntimeException e) {
            LOGGER.error("Failed to process {}", info, e);
            infoLogger.log(info);
        }
    }

}
