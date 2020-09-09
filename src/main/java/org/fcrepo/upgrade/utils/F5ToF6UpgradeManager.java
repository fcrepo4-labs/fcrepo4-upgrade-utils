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
package org.fcrepo.upgrade.utils;

import org.fcrepo.upgrade.utils.f6.ContainerInfo;
import org.fcrepo.upgrade.utils.f6.MigrationTaskManager;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author dbernstein
 * @since 2020-08-05
 */
class F5ToF6UpgradeManager implements UpgradeManager {

    private static final Logger LOGGER = getLogger(F5ToF6UpgradeManager.class);

    private final Config config;
    private final MigrationTaskManager migrationTaskManager;

    /**
     * Constructor
     */
    public F5ToF6UpgradeManager(final Config config,
                                final MigrationTaskManager migrationTaskManager) {
        this.config = config;
        this.migrationTaskManager = migrationTaskManager;
    }

    /**
     * Runs the upgrade util process
     */
    public void start() {
        LOGGER.info("Starting upgrade: config={}", config);

        /*

        - Migrate root?
        - Migrate root acl
        - Files ending with ID.ttl are containers (note: fcr files also end in ttl)
        - Files ending with ID.binary are binaries
        - ID/fcr%3Aversions/ contains versions
        - ID/fcr%3Ametadata.ttl contains the binary meta
        - ID/fcr%3Ametadata/fcr%3Aversions/ contains binary meta versions
        - Presumably ID/fcr%3Aacl.ttl contains web acls
        - Tombstones? ID/fcr%3Atombstone.ttl?
        - What triples should be stripped?
        - How are ghost nodes exported?
        - External references?

        Parallelize by having a fixed pool of worker threads. Work is at the container level.

        1. Process resources breadth first
            1. Check if container has any versions, if so each version must be created in order
            2. Create resource headers
            3. Transform RDF, removing any server managed triples
            4. Add to OCFL and create version, with correct timestamp
            5. If there is a web acl, it should be added in the first version
            6. If there is a tombstone, the resource should be deleted
            7. Add any child containers to work queue
            8. Process any child binaries:
                1. Check if binary has any versions, if so each version must be created in order
                2. Create resource headers
                3. Add to OCFL, but do not create version
                4. If there is a web acl, add it, but only to the first version
                5. Add the binary description
                6. Create a new version, with the correct timestamp (there is a problem here where the last modified timestamp is going to get touched...)
                7. If there is a tombstone, the resource, and its description, should be deleted

            Exception handling:
                - rollback container and all contained binaries
                - write container id to file
                - continue processing other containers
                - process can be re-run using file with failed container ids
         */

        final var repoRoot = new ContainerInfo(null, "info:fedora", "rest", config.getInputDir().toPath());
        migrationTaskManager.submit(repoRoot);

        try {
            migrationTaskManager.awaitCompletion();
            LOGGER.info("Upgrade complete.");
            migrationTaskManager.shutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

}
