/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
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
