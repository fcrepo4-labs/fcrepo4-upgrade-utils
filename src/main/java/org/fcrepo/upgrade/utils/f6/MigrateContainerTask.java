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

/**
 * @author pwinckles
 */
public class MigrateContainerTask implements Runnable {

    private final MigrationTaskManager taskManager;
    private final ContainerMigrator containerMigrator;
    private final ContainerInfo info;

    public MigrateContainerTask(final MigrationTaskManager taskManager,
                                final ContainerMigrator containerMigrator,
                                final ContainerInfo info) {
        this.taskManager = taskManager;
        this.containerMigrator = containerMigrator;
        this.info = info;
    }

    @Override
    public void run() {
        // TODO log on failure for reprocessing
        final var children = containerMigrator.migrateContainer(info);
        children.forEach(taskManager::submit);
    }

}
