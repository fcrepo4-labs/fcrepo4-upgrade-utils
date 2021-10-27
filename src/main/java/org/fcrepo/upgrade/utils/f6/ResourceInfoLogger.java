/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
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
