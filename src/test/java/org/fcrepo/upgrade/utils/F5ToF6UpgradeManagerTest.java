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

import org.apache.commons.codec.digest.DigestUtils;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * @author pwinckles
 */
public class F5ToF6UpgradeManagerTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private Config config;
    private Path out;

    @Before
    public void setup() throws IOException {
        out = tempFolder.newFolder().toPath();

        config = new Config();
        config.setForceWindowsMode(true);
        config.setOutputDir(out.toFile());
        config.setSourceVersion(FedoraVersion.V_5);
        config.setTargetVersion(FedoraVersion.V_6);
        config.setBaseUri("http://localhost:8080/rest/");
    }

    @Test
    public void migrateEntireExport() {
        config.setInputDir(new File("src/test/resources/5.1-export"));

        final var upgradeManager = UpgradeManagerFactory.create(config);

        upgradeManager.start();

        assertMigration(Paths.get("src/test/resources/5.1-to-6-expected"));
    }

    @Test
    public void migrateExportFromAContext() {
        config.setBaseUri("http://localhost:8080/fcrepo/rest/");
        config.setInputDir(new File("src/test/resources/5.1-export-with-context"));

        final var upgradeManager = UpgradeManagerFactory.create(config);

        upgradeManager.start();

        assertMigration(Paths.get("src/test/resources/5.1-to-6-expected-with-context"));
    }

    private void assertMigration(final Path expected) {
        final var expectedRoot = expected.resolve("data").resolve("ocfl-root");
        final var actualRoot = out.resolve("data").resolve("ocfl-root");

        final var expectedFiles = listAllFiles(expectedRoot);
        final var actualFiles = listAllFiles(actualRoot);

        assertThat(actualFiles, Matchers.containsInAnyOrder(expectedFiles.toArray(new String[0])));

        for (final var file : expectedFiles) {
            final var expectedFile = expectedRoot.resolve(file);
            final var actualFile = actualRoot.resolve(file);
            assertEquals(String.format("Files %s and %s are not the same", expectedFile, actualFile),
                    hash(expectedFile), hash(actualFile));
        }
    }

    private Set<String> listAllFiles(final Path root) {
        try (final var files = Files.walk(root)) {
            return files.filter(Files::isRegularFile)
                    .filter(f -> !f.getParent().equals(root))
                    .map(f -> root.relativize(f).toString())
                    .collect(Collectors.toSet());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String hash(final Path path) {
        try {
            return DigestUtils.sha256Hex(Files.newInputStream(path));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
