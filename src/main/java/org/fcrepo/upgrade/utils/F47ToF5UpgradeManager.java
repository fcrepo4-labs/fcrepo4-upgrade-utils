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
import static org.fcrepo.upgrade.utils.HttpConstants.LINK_HEADER;
import static org.fcrepo.upgrade.utils.HttpConstants.LOCATION_HEADER;
import static org.fcrepo.upgrade.utils.RdfConstants.EBUCORE_HAS_MIME_TYPE;
import static org.fcrepo.upgrade.utils.RdfConstants.FEDORA_CREATED_DATE;
import static org.fcrepo.upgrade.utils.RdfConstants.FEDORA_VERSION;
import static org.fcrepo.upgrade.utils.RdfConstants.LDP_BASIC_CONTAINER;
import static org.fcrepo.upgrade.utils.RdfConstants.LDP_CONTAINER;
import static org.fcrepo.upgrade.utils.RdfConstants.LDP_CONTAINER_TYPES;
import static org.fcrepo.upgrade.utils.RdfConstants.LDP_NON_RDF_SOURCE;
import static org.fcrepo.upgrade.utils.RdfConstants.LDP_RDF_SOURCE;
import static org.fcrepo.upgrade.utils.RdfConstants.MEMENTO;
import static org.fcrepo.upgrade.utils.RdfConstants.NON_RDF_SOURCE_DESCRIPTION;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
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
    private static final DateTimeFormatter ISO_DATE_TIME_FORMATTER = DateTimeFormatter.ISO_DATE_TIME
        .withZone(ZoneId.of("UTC"));
    private static final DateTimeFormatter RFC_1123_FORMATTER = DateTimeFormatter.RFC_1123_DATE_TIME
        .withZone(ZoneId.of("UTC"));
    private static final DateTimeFormatter MEMENTO_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    private static final String MEMENTO_DATETIME_HEADER = "Memento-Datetime";
    private static final String FCR_METADATA_PATH_SEGMENT = "fcr%3Ametadata";
    private static final String FCR_VERSIONS_PATH_SEGMENT = "fcr%3Aversions";
    private static final String TYPE_RELATION = "type";
    private static final String TURTLE_EXTENSION = ".ttl";
    private static final String HEADERS_SUFFIX = ".headers";
    public static final String APPLICATION_OCTET_STREAM_MIMETYPE = "application/octet-stream";

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

        //skip versions container
        if (path.endsWith(FCR_VERSIONS_PATH_SEGMENT + TURTLE_EXTENSION)) {
            LOGGER.debug("version containers are not required for import.  Skipping {}...", path);
            return;
        }

        final boolean isVersionedResource = path.toString().contains(FCR_VERSIONS_PATH_SEGMENT + File.separator);
        final boolean isBinaryDescription = path.toString().contains(FCR_METADATA_PATH_SEGMENT);
        final Path inputPath = this.config.getInputDir().toPath();
        final Path relativePath = inputPath.relativize(path);
        final Path relativeNewLocation;
        final TemporalAccessor versionTimestamp;

        if (isVersionedResource) {
            versionTimestamp = resolveMementoTimestamp(path);
            relativeNewLocation = resolveNewVersionedResourceLocation(path, versionTimestamp);
        } else {
            versionTimestamp = null;
            relativeNewLocation = relativePath;
        }

        final var newLocation = this.config.getOutputDir().toPath().resolve(relativeNewLocation);

        try {
            Files.createDirectories(newLocation.getParent());
            LOGGER.debug("copy file {} to {}", path, newLocation);
            FileUtils.copyFile(path.toFile(), newLocation.toFile());
            if (newLocation.toString().endsWith(TURTLE_EXTENSION)) {
                upgradeRdfAndCreateHeaders(versionTimestamp, newLocation);
            }
            LOGGER.info("Resource upgraded: {}", path);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void upgradeRdfAndCreateHeaders(final TemporalAccessor versionTimestamp,
                                            final Path newLocation)
        throws IOException {
        //parse the file
        final Model model = ModelFactory.createDefaultModel();
        try (final FileInputStream is = new FileInputStream(newLocation.toFile())) {
            RDFDataMgr.read(model, is, Lang.TTL);
        }

        final Map<String, List<String>> metadataHeaders = new HashMap<>();
        final Map<String, List<String>> binaryHeaders = new HashMap<>();
        metadataHeaders.computeIfAbsent(LINK_HEADER, x -> new ArrayList<>());
        binaryHeaders.computeIfAbsent(LINK_HEADER, x -> new ArrayList<>());
        metadataHeaders.put(CONTENT_TYPE_HEADER, Collections.singletonList("text/turtle"));
        final AtomicBoolean isExternal = new AtomicBoolean(false);
        final AtomicReference<Resource> containerSubject = new AtomicReference<>();
        final AtomicBoolean rewriteModel = new AtomicBoolean();

        final var statements = model.listStatements().toList();
        //gather the rdf types
        final var rdfTypes = statements.stream().filter(s -> s.getPredicate().equals(RDF.type))
                                       .map(s -> s.getObject().asResource()).collect(Collectors.toList());
        final var isBinary = rdfTypes.contains(LDP_NON_RDF_SOURCE);

        final var isContainer = rdfTypes.contains(LDP_CONTAINER);

        rdfTypes.retainAll(LDP_CONTAINER_TYPES);
        final var isConcreteContainerDefined = !rdfTypes.isEmpty();

        addTypeLinkHeader(metadataHeaders, LDP_RDF_SOURCE.getURI());
        if (isBinary) {
            addTypeLinkHeader(binaryHeaders, LDP_NON_RDF_SOURCE.getURI());
            addTypeLinkHeader(metadataHeaders, NON_RDF_SOURCE_DESCRIPTION.getURI());
        }

        //loop through all the statements, modifying the model as necessary.
        statements.forEach(statement -> {
            var currentStatement = statement;

            //replace subject and internal objects with original resource uri if versioned
            if (versionTimestamp != null) {
                model.remove(currentStatement);
                final var object = currentStatement.getObject();
                currentStatement = model.createStatement(getOriginalResource(currentStatement.getSubject()),
                                                         currentStatement.getPredicate(),
                                                         object.isURIResource() ?
                                                         getOriginalResource(object.asResource()) : object);
                model.add(currentStatement);
                rewriteModel.set(true);
            }

            containerSubject.set(currentStatement.getSubject());

            final var object = currentStatement.getObject();

            //remove fedora:Version rdf type statement.
            if (currentStatement.getPredicate().equals(RDF.type) && object.asResource().equals(FEDORA_VERSION)) {
                model.remove(currentStatement);
                rewriteModel.set(true);
            } else if (statement.getPredicate().equals(EBUCORE_HAS_MIME_TYPE)) {
                //convert hasMimetype statement
                final String value = currentStatement.getString();
                LOGGER.debug("predicate value={}", value);
                var mimetype = value;
                if (value.startsWith("message/external-body")) {
                    mimetype = APPLICATION_OCTET_STREAM_MIMETYPE;
                    final var matcher = MESSAGE_EXTERNAL_BODY_URL_PATTERN.matcher(value);
                    String externalURI = null;
                    if (matcher.matches()) {
                        externalURI = matcher.group(1);
                    }

                    LOGGER.debug("externalURI={}", externalURI);
                    //remove old has mimetype statement
                    model.remove(currentStatement);
                    currentStatement = model.createStatement(currentStatement.getSubject(),
                                                             currentStatement.getPredicate(),
                                                             mimetype);
                    //add in the new one
                    model.add(currentStatement);
                    rewriteModel.set(true);

                    //if external add appropriate binary headers
                    if (externalURI != null) {
                        binaryHeaders.put(LOCATION_HEADER, Collections.singletonList(externalURI));
                        binaryHeaders.put(CONTENT_LOCATION_HEADER, Collections.singletonList(externalURI));
                        isExternal.set(true);
                    }
                }
                binaryHeaders.put(CONTENT_TYPE_HEADER, Collections.singletonList(mimetype));
            }
        });

        if (versionTimestamp != null) {
            addMementoDatetimeHeader(versionTimestamp, metadataHeaders);
            addMementoDatetimeHeader(versionTimestamp, binaryHeaders);
            addTypeLinkHeader(metadataHeaders, MEMENTO.getURI());
            addTypeLinkHeader(binaryHeaders, MEMENTO.getURI());
        }

        // While F5 assumes BasicContainer when no concrete container is present in the RDF on import,
        // the F5->F6 upgrade pathway requires its presence.  Thus I add it here for consistency.
        // As a note, when an F5 repository is exported the BasicContainer type triple will be present in
        // the exported RDF.
        if (isContainer && !isConcreteContainerDefined) {
            model.add(containerSubject.get(), RDF.type, LDP_BASIC_CONTAINER);
            rewriteModel.set(true);
        }

        // rewrite only if the model has changed.
        if (rewriteModel.get()) {
            try {
                RDFDataMgr.write(new FileOutputStream(newLocation.toFile()), model, Lang.TTL);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        //write rdf headers file
        final var headersPrefix = newLocation.toAbsolutePath().toString();
        writeHeadersFile(metadataHeaders, new File(headersPrefix + HEADERS_SUFFIX));

        //write binary headers file
        if (isBinary) {
            String binaryHeadersPrefix;
            if (versionTimestamp != null) {
                //locate the related binary prefix
                binaryHeadersPrefix = locateBinaryHeadersPrefixForVersionedBinary(newLocation);
            } else  {
                //for unversioned binaries we want simply to translate from
                // path/to/binary/fcr%3Ametadata.ttl to path/to/binary
                binaryHeadersPrefix = newLocation.getParent().toAbsolutePath().toString();
            }

            binaryHeadersPrefix += isExternal.get() ? ".external" : ".binary";
            writeHeadersFile(binaryHeaders, new File(binaryHeadersPrefix + HEADERS_SUFFIX));
        }

        LOGGER.debug("isExternal={}", isExternal.get());
        LOGGER.debug("headersPrefix={}", headersPrefix);
        LOGGER.debug("isContainer={}", isContainer);
        LOGGER.debug("isConcreteContainerDefined={}", isConcreteContainerDefined);
        LOGGER.debug("containerSubject={}", containerSubject);
    }

    private String locateBinaryHeadersPrefixForVersionedBinary(final Path newLocation) {
        //the idea here is translate a versioned metadata resource
        //   from
        //   path/to/binary/fcr%3Ametdata/fcr%3Aversions/20201105171804.ttl
        //   to
        //   path/to/binary/fcr%3Aversions/20201105171804
        final var fileName = newLocation.getFileName().toString();
        final var filenameWithoutExtension = FilenameUtils.getBaseName(fileName);
        return newLocation.getParent().getParent().getParent().toAbsolutePath() + File.separator +
                              FCR_VERSIONS_PATH_SEGMENT + File.separator + filenameWithoutExtension;
    }

    private Resource getOriginalResource(Resource resource) {
        return ResourceFactory.createResource(resource.getURI()
                                                      .replaceAll("/fcr:versions/[a-zA-Z0-9.]*", ""));
    }

    private void addTypeLinkHeader(Map<String, List<String>> headers, String typeUri) {
        final FcrepoLink link = FcrepoLink.fromUri(typeUri).rel(TYPE_RELATION).build();
        headers.get(LINK_HEADER).add(link.toString());
    }

    private void addMementoDatetimeHeader(TemporalAccessor versionTimestamp, Map<String, List<String>> headers) {
        headers.put(MEMENTO_DATETIME_HEADER, Collections.singletonList(RFC_1123_FORMATTER.format(versionTimestamp)));
    }

    private Path resolveVersionsContainer(final Path path) {
        var currentPath = path;
        while (currentPath != path.getRoot()) {
            final var parent = currentPath.getParent();
            if (parent.endsWith(FCR_VERSIONS_PATH_SEGMENT)) {
                return Path.of(parent.toString() + TURTLE_EXTENSION);
            }

            currentPath = parent;
        }
        return null;
    }

    private TemporalAccessor resolveMementoTimestamp(final Path path) {
        var metadataPath = path;
        if (!path.toString().endsWith(TURTLE_EXTENSION)) {
            final var metadataPathStr = metadataPath.toString();
            final var index = metadataPathStr.lastIndexOf(".");
            final var newMetadataPathStr = metadataPathStr
                                               .substring(0,
                                                          index) + File.separator + FCR_METADATA_PATH_SEGMENT + TURTLE_EXTENSION;
            metadataPath = Path.of(newMetadataPathStr);
        }

        //resolve the subject associated with the resource
        final Model model = ModelFactory.createDefaultModel();
        try (final FileInputStream is = new FileInputStream(metadataPath.toFile())) {
            RDFDataMgr.read(model, is, Lang.TTL);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        final var subject = model.listSubjects().nextResource();

        //resolve the versions file
        final var versionsContainer = resolveVersionsContainer(path);
        //read the versions container in order to resolve the version timestamp
        final Model versionsContainerModel = ModelFactory.createDefaultModel();
        try (final FileInputStream is = new FileInputStream(versionsContainer.toFile())) {
            RDFDataMgr.read(versionsContainerModel, is, Lang.TTL);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        final var iso8601Timestamp = versionsContainerModel.listObjectsOfProperty(subject, FEDORA_CREATED_DATE)
                                                           .next().asLiteral().getString();
        //create memento id based on RFC 8601 timestamp
        return ISO_DATE_TIME_FORMATTER.parse(iso8601Timestamp);
    }

    private Path resolveNewVersionedResourceLocation(final Path path, final TemporalAccessor mementoTimestamp) {
        final var mementoId = MEMENTO_FORMATTER.format(mementoTimestamp);
        //create a new location compatible with an F5 export.
        final var isDescription = path.endsWith(FCR_METADATA_PATH_SEGMENT + TURTLE_EXTENSION);
        final var inputPath = this.config.getInputDir().toPath();
        final var relativePath = inputPath.relativize(path);
        final var relativePathStr = relativePath.toString();
        var newLocation = relativePath;
        final var extension = relativePathStr.substring(relativePathStr.lastIndexOf("."));
        if (!isDescription) {
            newLocation = Path.of(relativePath.getParent().toString(), mementoId + extension);
        } else {
            newLocation = Path.of(relativePath.getParent().getParent().getParent().toString(),
                                  FCR_METADATA_PATH_SEGMENT, FCR_VERSIONS_PATH_SEGMENT, mementoId + extension);
        }
        return newLocation;
    }

    private void writeHeadersFile(final Map<String, List<String>> headers, final File file) throws IOException {
        final String json = new ObjectMapper().writeValueAsString(headers);
        file.getParentFile().mkdirs();
        try (final FileWriter writer = new FileWriter(file)) {
            writer.write(json);
        }
    }
}
