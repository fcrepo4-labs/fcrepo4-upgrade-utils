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

import static org.fcrepo.upgrade.utils.HttpConstants.CONTENT_LOCATION_HEADER;
import static org.fcrepo.upgrade.utils.HttpConstants.CONTENT_TYPE_HEADER;
import static org.fcrepo.upgrade.utils.HttpConstants.LOCATION_HEADER;
import static org.fcrepo.upgrade.utils.RdfConstants.EBUCORE_HAS_MIME_TYPE;
import static org.fcrepo.upgrade.utils.RdfConstants.LDP_BASIC_CONTAINER;
import static org.fcrepo.upgrade.utils.RdfConstants.LDP_CONTAINER_TYPES;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDF;
import org.fcrepo.client.FcrepoLink;

/**
 * @author dbernstein
 * @since 2019-08-05
 */
class F47ToF5UpgradeManager extends UpgradeManagerBase implements UpgradeManager {

    private static final org.slf4j.Logger LOGGER = getLogger(F47ToF5UpgradeManager.class);
    private static final Pattern MESSAGE_EXTERNAL_BODY_URL_PATTERN = Pattern
        .compile("^.*url=\"(.*)\".*$", Pattern.CASE_INSENSITIVE);

    /**
     * Constructor
     *
     * @param config the upgrade configuration
     */
    F47ToF5UpgradeManager(final Config config) {
        super(config);
    }

    @Override
    public void start() {
        //walk the directory structure
        processDirectory(this.config.getInputDir());
    }

    private void processDirectory(final File dir) {
        LOGGER.info("Processing directory: {}", dir.getAbsolutePath());
        try (final Stream<Path> walk = Files.walk(dir.toPath())) {
            walk.filter(Files::isRegularFile).forEach(this::processFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void processFile(final Path path) {
        final Path inputPath = this.config.getInputDir().toPath();
        final Path relativePath = inputPath.relativize(path);
        final Path newLocation = this.config.getOutputDir().toPath().resolve(relativePath);

        try {
            Files.createDirectories(newLocation.getParent());
            LOGGER.debug("copy file {} to {}", path, newLocation);
            FileUtils.copyFile(path.toFile(), newLocation.toFile());
            if (newLocation.toString().endsWith(".ttl")) {
                //parse the file
                final Model model = ModelFactory.createDefaultModel();
                try (final FileInputStream is = new FileInputStream(newLocation.toFile())) {
                    RDFDataMgr.read(model, is, Lang.TTL);
                }

                final Map<String, List<String>> headers = new HashMap<>();

                final AtomicBoolean isBinary = new AtomicBoolean(false);
                final AtomicBoolean isExternal = new AtomicBoolean(false);
                final AtomicBoolean isContainer = new AtomicBoolean(false);
                final AtomicBoolean isConcreteContainerDefined = new AtomicBoolean(false);
                final AtomicReference<Resource> containerSubject = new AtomicReference<>();
                final AtomicBoolean rewriteModel = new AtomicBoolean();

                // toList here because we may need to modify the model mid-iteration
                model.listStatements().toList().forEach(statement -> {
                    if (statement.getPredicate().equals(RDF.type)) {
                        final Resource object = statement.getObject().asResource();

                        if (object.equals(RdfConstants.LDP_NON_RDFSOURCE)) {
                            isBinary.set(true);
                        } else if (object.equals(RdfConstants.LDP_CONTAINER)) {
                            isContainer.set(true);
                            containerSubject.set(statement.getSubject());
                        } else if (LDP_CONTAINER_TYPES.contains(object)) {
                            isConcreteContainerDefined.set(true);
                        }

                        if (!headers.containsKey("Link")) {
                            headers.put("Link", new ArrayList<String>());
                        }

                        final FcrepoLink link = FcrepoLink.fromUri(object.getURI()).rel("type").build();
                        headers.get("Link").add(link.toString());

                    }

                    if (statement.getPredicate().equals(EBUCORE_HAS_MIME_TYPE)) {
                        final String value = statement.getString();
                        LOGGER.debug("predicate value={}", value);
                        if (value.startsWith("message/external-body")) {
                            final var matcher = MESSAGE_EXTERNAL_BODY_URL_PATTERN.matcher(value);
                            String externalURI = null;
                            if(matcher.matches()) {
                                externalURI = matcher.group(1);
                            }

                            LOGGER.debug("externalURI={}", externalURI);
                            model.remove(statement);
                            model.add(statement.getSubject(), statement.getPredicate(), "application/octet-stream");
                            rewriteModel.set(true);
                            if (externalURI != null) {
                                headers.put(LOCATION_HEADER, Collections.singletonList(externalURI));
                                headers.put(CONTENT_LOCATION_HEADER, Collections.singletonList(externalURI));
                                isExternal.set(true);
                            }
                        } else {
                            if (!headers.containsKey(CONTENT_TYPE_HEADER)) {
                                headers.put(CONTENT_TYPE_HEADER, new ArrayList<String>());
                            }
                            headers.get(CONTENT_TYPE_HEADER).add(value);
                        }
                    }
                });

                // While F5 assumes BasicContainer when no concrete container is not present in the RDF on import,
                // the F5->F6 upgrade pathway requires its presence.  Thus I add it here for consistency.
                // As a note, when an F5 repository is exported the BasicContainer type triple will be present in
                // the exported RDF.
                if (isContainer.get() && !isConcreteContainerDefined.get()) {
                    model.add(containerSubject.get(), RDF.type, LDP_BASIC_CONTAINER);
                    rewriteModel.set(true);
                }

                // write only if the model as changed.
                if (rewriteModel.get()) {
                    try {
                        RDFDataMgr.write(new FileOutputStream(newLocation.toFile()), model, Lang.TTL);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                String headersPrefix;

                if (isBinary.get()) {
                    headersPrefix = newLocation.getParent().toAbsolutePath().toString();
                    headersPrefix += isExternal.get() ? ".external" : ".binary";
                } else {
                    headersPrefix = newLocation.toAbsolutePath().toString();
                }

                LOGGER.debug("isBinary={}", isBinary.get());
                LOGGER.debug("isExternal={}", isExternal.get());
                LOGGER.debug("headersPrefix={}", headersPrefix);
                LOGGER.debug("isContainer={}", isContainer);
                LOGGER.debug("isConcreteContainerDefined={}", isConcreteContainerDefined);
                LOGGER.debug("containerSubject={}", containerSubject);

                writeHeadersFile(headers, new File(headersPrefix + ".headers"));

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeHeadersFile(final Map<String, List<String>> headers, final File file) throws IOException {
        final String json = new ObjectMapper().writeValueAsString(headers);
        try (final FileWriter writer = new FileWriter(file)) {
            writer.write(json);
        }
    }
}
