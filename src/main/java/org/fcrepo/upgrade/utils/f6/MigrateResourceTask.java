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

import java.util.ArrayList;
import java.util.List;

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
    private final ResourceInfo info;

    /**
     * @param taskManager the task manager that is coordinating migration tasks
     * @param resourceMigrator the object responsible for performing the migration
     * @param info the resource to be migrated
     */
    public MigrateResourceTask(final MigrationTaskManager taskManager,
                               final ResourceMigrator resourceMigrator,
                               final ResourceInfo info) {
        this.taskManager = taskManager;
        this.resourceMigrator = resourceMigrator;
        this.info = info;
    }

    @Override
    public void run() {
        List<ResourceInfo> children = new ArrayList<>();

        try {
            children = resourceMigrator.migrate(info);
            // TODO Failures could be logged to a file for reprocessing at a later date
        } catch (UnsupportedOperationException e) {
            // This is thrown when a resource is encountered that is not currently handled
            LOGGER.error(e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.error("Failed to process {}", info, e);
        }

        for (final var child : children) {
            try {
                taskManager.submit(child);
            } catch (RuntimeException e) {
                // TODO log to file for reprocessing
                LOGGER.warn("Failed to queue {} for migration", child.getFullId());
            }
        }
    }

}
