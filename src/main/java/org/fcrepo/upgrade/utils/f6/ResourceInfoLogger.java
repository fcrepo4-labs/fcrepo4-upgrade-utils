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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Logs ResourceInfo objects to a file
 *
 * @author pwinckles
 */
public class ResourceInfoLogger {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceInfoLogger.class);
    private static final Logger INFO_LOG = LoggerFactory.getLogger("org.fcrepo.upgrade.utils.remaining");

    private final ObjectMapper objectMapper;

    public ResourceInfoLogger() {
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Logs the resource info to the remaining log file.
     *
     * @param info resource info to log
     */
    public void log(final ResourceInfo info) {
        try {
            INFO_LOG.error("{}", objectMapper.writeValueAsString(info));
        } catch (JsonProcessingException e) {
            LOG.error("Failed to serialize {} to remaining log", info, e);
        }
    }

    /**
     * Parses a log file that contains JSON serialized ResourceInfo objects, one per line, into a list of objects
     *
     * @param logPath log file to parse
     * @return list of parsed objects
     */
    public List<ResourceInfo> parseLog(final Path logPath) {
        final var infos = new ArrayList<ResourceInfo>();

        try (final var reader = new BufferedReader(new InputStreamReader(Files.newInputStream(logPath)))) {
            while (reader.ready()) {
                infos.add(objectMapper.readValue(reader.readLine(), ResourceInfo.class));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return infos;
    }

}
