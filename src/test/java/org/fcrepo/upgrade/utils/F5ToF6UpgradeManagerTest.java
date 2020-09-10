package org.fcrepo.upgrade.utils;

import org.junit.Test;

import java.nio.file.Paths;

public class F5ToF6UpgradeManagerTest {

    @Test
    public void asdf() {
        final var in = Paths.get("/home/winckles/Desktop/f5-copy");
        final var out = Paths.get("/var/tmp/f5-out-2");
        final var config = new Config();
        config.setInputDir(in.toFile());
        config.setOutputDir(out.toFile());
        config.setSourceVersion(FedoraVersion.V_5);
        config.setTargetVersion(FedoraVersion.V_6);

        final var upgradeManager = UpgradeManagerFactory.create(config);

        upgradeManager.start();
    }

}
