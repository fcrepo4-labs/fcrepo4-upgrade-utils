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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests the {@link F47ToF5UpgradeManagerTest}
 *
 * @author dbernstein
 */

public class F47ToF5UpgradeManagerTest {

    static final String TARGET_DIR = System.getProperty("project.build.directory");

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testUpgrade() throws Exception {
        //prepare
        final File tmpDir = tempFolder.newFolder();
        final File input = new File(TARGET_DIR + "/test-classes/4.7.5-export");
        final File output = new File(tmpDir, "output");
        output.mkdir();

        final var config = new Config();
        config.setSourceVersion(FedoraVersion.V_4_7_5);
        config.setTargetVersion(FedoraVersion.V_5);
        config.setInputDir(input);
        config.setOutputDir(output);
        //run
        UpgradeManager upgradeManager = UpgradeManagerFactory.create(config);
        upgradeManager.start();
        //ensure all expected files exist
        final String[] expectedFiles =
            new String[]{"rest.ttl",
                         "rest.ttl.headers",
                         "rest/external1",
                         "rest/external1/fcr%3Ametadata.ttl",
                         "rest/container1.ttl",
                         "rest/container1.ttl.headers",
                         "rest/container1/fcr%3Aversions/20201015053947.ttl",
                         "rest/container1/fcr%3Aversions/20201015053947.ttl.headers",
                         "rest/container1/fcr%3Aversions/20201015053526.ttl",
                         "rest/container1/fcr%3Aversions/20201015053526.ttl.headers",
                         "rest/container1/testbinary.binary",
                         "rest/container1/testbinary/fcr%3Ametadata.ttl",
                         "rest/container1/testbinary.binary.headers",
                         "rest/container1/testbinary/fcr%3Ametadata/fcr%3Aversions/20201015053717.ttl",
                         "rest/container1/testbinary/fcr%3Ametadata/fcr%3Aversions/20201015053717.ttl.headers",
                         "rest/container1/testbinary/fcr%3Ametadata/fcr%3Aversions/20201015053848.ttl",
                         "rest/container1/testbinary/fcr%3Ametadata/fcr%3Aversions/20201015053848.ttl.headers",
                         "rest/container1/testbinary/fcr%3Aversions/20201015053848.binary",
                         "rest/container1/testbinary/fcr%3Aversions/20201015053848.binary.headers",
                         "rest/container1/testbinary/fcr%3Aversions/20201015053717.binary",
                         "rest/external1.external.headers",
                         "rest/external1.external"};

        for (String f : expectedFiles) {
            assertTrue(f + " does not exist as expected", new File(output, f).exists());
        }

        //ensure external content has been transformed properly

        final String externalContent = FileUtils
            .readFileToString(new File(output, "rest/external1/fcr%3Ametadata.ttl"), "UTF-8");
        assertFalse("external content metadata should contain the mimetype", externalContent.contains("image/jpg"));
        assertFalse("message/external-body should not be present in the external content metadata",
                    externalContent.contains("message/external-body"));

        //ensure the binaries contain NonRDFSource types in their headers
        final Map<String, List<String>> bheadders =
            deserializeHeaders(new File(output, "rest/container1/testbinary.binary.headers"));
        assertTrue("binary does not contain NonRDFSource type in the link headers",
                   bheadders.get("Link").stream().anyMatch(x -> x.contains("NonRDFSource")));

        for (String f : expectedFiles) {
            final var file = new File(output, f);
            if (f.contains("fcr%3Aversions")) {

                if(f.endsWith(".headers")){
                    final Map<String, List<String>> mHeaders =
                        deserializeHeaders(file);
                    assertTrue("Memento headers do not contain memento type link header",
                               mHeaders.get("Link").stream().anyMatch(x -> x.contains("Memento")));
                    assertTrue("Memento headers do not contain Memento-Datetime header",
                               mHeaders.get("Memento-Datetime") != null);
                } else if(f.contains("fcr:%3Ametadata")) {
                   final var contents = IOUtils.toString(new FileInputStream(file), Charset.defaultCharset());
                   assertTrue("Mementos should not contain links to other mementos",
                              !contents.contains("fcr:versions/"));
                }
            }
        }


    }

    private Map<String, List<String>> deserializeHeaders(final File headerFile) throws IOException {
        final byte[] mapData = Files.readAllBytes(Paths.get(headerFile.toURI()));
        final ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(mapData, new TypeReference<HashMap<String, List<String>>>() {
        });
    }

}
