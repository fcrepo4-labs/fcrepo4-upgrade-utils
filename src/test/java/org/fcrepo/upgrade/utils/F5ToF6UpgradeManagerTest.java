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

import org.junit.Test;

import java.nio.file.Paths;

public class F5ToF6UpgradeManagerTest {

    @Test
    public void asdf() {
        final var in = Paths.get("/home/winckles/Desktop/f5-copy");
//        final var in = Paths.get("/home/winckles/Desktop/f5-versions");
        final var out = Paths.get("/var/tmp/f5-out-2");
        final var config = new Config();
        config.setInputDir(in.toFile());
        config.setOutputDir(out.toFile());
        config.setSourceVersion(FedoraVersion.V_5);
        config.setTargetVersion(FedoraVersion.V_6);
        config.setBaseUri("http://localhost:8080/rest/");

        final var upgradeManager = UpgradeManagerFactory.create(config);

        upgradeManager.start();
    }

}
