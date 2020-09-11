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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link UpgradeManagerFactory}
 *
 * @author dbernstein
 */

public class UpgradeManagerFactoryTest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testCreateF4To5UpgradeManager() throws Exception {
        final var config = new Config();
        config.setSourceVersion(FedoraVersion.V_4_7_5);
        config.setTargetVersion(FedoraVersion.V_5);
        //run
        assertTrue(UpgradeManagerFactory.create(config) instanceof F47ToF5UpgradeManager);
    }

    @Test
    public void testCreateF5To6UpgradeManager() throws Exception {
        final var config = new Config();
        config.setSourceVersion(FedoraVersion.V_5);
        config.setTargetVersion(FedoraVersion.V_6);
        config.setBaseUri("http://localhost:8080/rest");
        config.setInputDir(temp.newFolder());
        config.setOutputDir(temp.newFolder());
        //run
        assertTrue(UpgradeManagerFactory.create(config) instanceof  F5ToF6UpgradeManager);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidMigrationPath() throws Exception {
        final var config = new Config();
        config.setSourceVersion(FedoraVersion.V_6);
        config.setTargetVersion(FedoraVersion.V_5);
        UpgradeManagerFactory.create(config);
    }


}
