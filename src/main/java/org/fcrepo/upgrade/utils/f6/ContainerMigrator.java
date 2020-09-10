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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.SimpleSelector;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.StreamRDFWriter;
import org.apache.jena.vocabulary.RDF;
import org.fcrepo.storage.ocfl.InteractionModel;
import org.fcrepo.storage.ocfl.OcflObjectSession;
import org.fcrepo.storage.ocfl.OcflObjectSessionFactory;
import org.fcrepo.storage.ocfl.ResourceHeaders;
import org.fcrepo.upgrade.utils.RdfConstants;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.time.ZoneOffset.UTC;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author pwinckles
 */
public class ContainerMigrator {

    private static final Logger LOGGER = getLogger(ContainerMigrator.class);

    private static final DateTimeFormatter MEMENTO_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(UTC);

    // TODO need to support other rdf languages
    private static final String TTL_EXT = ".ttl";
    private static final String BINARY_EXT = ".binary";
    private static final String EXTERNAL_EXT = ".external";
    private static final String HEADERS_EXT = ".headers";

    private static final String FCR = "fcr%3A";
    private static final String FCR_VERSIONS = FCR + "versions";
    private static final String FCR_METADATA = FCR + "metadata";
    private static final String FCR_ACL = FCR + "acl";

    private static final String FCR_METADATA_ID = "fcr:metadata";
    private static final String FCR_ACL_ID = "fcr:acl";

    private final OcflObjectSessionFactory objectSessionFactory;
    private final ObjectMapper objectMapper;

    public ContainerMigrator(final OcflObjectSessionFactory objectSessionFactory) {
        this.objectSessionFactory = objectSessionFactory;
        this.objectMapper = new ObjectMapper();
    }

    public List<ResourceInfo> migrateContainer(final ResourceInfo info) {
        LOGGER.info("Migrating container {}", info.getFullId());

        final var containerDir = info.getInnerDirectory();

        try {
            if (hasVersions(containerDir)) {
                final var versions = identifyVersions(containerDir);
                LOGGER.debug("Resource {} has versions: {}", info.getFullId(), versions);

                final var first = new AtomicBoolean(true);

                versions.forEach(version -> {
                    migrateContainerVersion(info, containerDir,
                            containerDir.resolve(FCR_VERSIONS).resolve(version + TTL_EXT),
                            first.getAndSet(false));
                });
            } else {
                migrateContainerVersion(info, containerDir,
                        info.getOuterDirectory().resolve(info.getNameEncoded() + TTL_EXT),
                        true);
            }

            return processChildren(info, containerDir);
        } catch (RuntimeException e) {
            LOGGER.info("Failed to migration resource {}. Rolling back...", info.getFullId());
            deleteObject(info.getFullId());
            throw e;
        }
    }

    public void close() {
        objectSessionFactory.close();
    }

    private List<ResourceInfo> processChildren(final ResourceInfo info, final Path containerDir) {
        final var children = listAllChildren(info.getFullId(), containerDir);

        final var binaries = new ArrayList<ResourceInfo>();
        final var externalBinaries = new ArrayList<ResourceInfo>();
        final var containers = new ArrayList<ResourceInfo>();

        children.forEach(child -> {
            switch (child.getType()) {
                case BINARY:
                    binaries.add(child);
                    break;
                case EXTERNAL_BINARY:
                    externalBinaries.add(child);
                    break;
                case CONTAINER:
                    containers.add(child);
                    break;
                default:
                    throw new IllegalStateException("Unmapped resource type");
            }
        });

        try {
            binaries.forEach(this::migrateBinary);
            externalBinaries.forEach(this::migrateExternalBinary);
        } catch (RuntimeException e) {
            LOGGER.info("Failed to migration resource {}. Rolling back...", info.getFullId());
            deleteObject(info.getFullId());
            binaries.forEach(b -> deleteObject(b.getFullId()));
            externalBinaries.forEach(b -> deleteObject(b.getFullId()));
            throw e;
        }

        return containers;
    }

    private void migrateContainerVersion(final ResourceInfo info,
                                         final Path containerDir,
                                         final Path rdfFile,
                                         final boolean isFirstVersion) {
        final var rdf = parseRdf(rdfFile);

        final var interactionModel = identifyInteractionModel(info.getFullId(), rdf);

        if (interactionModel != InteractionModel.BASIC_CONTAINER) {
            throw new UnsupportedOperationException(String.format(
                    "Resource %s could not be migrated." +
                            " Migrating direct/indirect containers is not currently supported.", info.getFullId()));
        }

        final var headers = createContainerHeaders(info.getParentId(), info.getFullId(), interactionModel,rdf);

        final var session = objectSessionFactory.newSession(info.getFullId());
        try {
            session.versionCreationTimestamp(headers.getLastModifiedDate().atOffset(ZoneOffset.UTC));
            session.writeResource(headers, writeRdf(info.getFullId(), rdf));

            if (isFirstVersion && hasAcl(containerDir)) {
                migrateAcl(info.getFullId(), containerDir, session);
            }

            session.commit();
        } catch (RuntimeException e) {
            session.abort();
            throw new RuntimeException("Failed to migrate resource " + info.getFullId(), e);
        }
    }

    private void migrateBinary(final ResourceInfo info) {
        final var fullId = info.getFullId();

        LOGGER.info("Migrating binary {}", fullId);

        final var binaryDir = info.getInnerDirectory();

        if (hasVersions(binaryDir)) {
            final var versions = identifyVersions(binaryDir);
            LOGGER.debug("Resource {} has versions: {}", fullId, versions);

            final var first = new AtomicBoolean(true);

            versions.forEach(version -> {
                migrateBinaryVersion(info.getParentId(), fullId, binaryDir,
                        binaryDir.resolve(FCR_VERSIONS).resolve(version + BINARY_EXT),
                        binaryDir.resolve(FCR_METADATA).resolve(FCR_VERSIONS).resolve(version + TTL_EXT),
                        first.getAndSet(false));
            });
        } else {
            migrateBinaryVersion(info.getParentId(), fullId, binaryDir,
                    info.getOuterDirectory().resolve(info.getNameEncoded() + BINARY_EXT),
                    binaryDir.resolve(FCR_METADATA + TTL_EXT),
                    true);
        }
    }

    private void migrateBinaryVersion(final String parentId,
                                      final String fullId,
                                      final Path binaryDir,
                                      final Path binaryFile,
                                      final Path descFile,
                                      final boolean isFirstVersion) {
        final var rdf = parseRdf(descFile);
        final var headers = createBinaryHeaders(parentId, fullId, rdf);

        final var descId = joinId(fullId, FCR_METADATA_ID);
        final var descHeaders = createBinaryDescHeaders(fullId, descId, rdf);

        try (final var stream = Files.newInputStream(binaryFile)) {
            writeBinary(fullId, binaryDir, headers, stream, descHeaders, rdf, isFirstVersion);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void migrateExternalBinary(final ResourceInfo info) {
        final var fullId = info.getFullId();

        LOGGER.info("Migrating external binary {}", fullId);

        final var descFile = info.getInnerDirectory().resolve(FCR_METADATA + TTL_EXT);
        final var rdf = parseRdf(descFile);
        final var headers = createBinaryHeaders(info.getParentId(), fullId, rdf);

        final var externalResource = parseExternalResource(info);
        headers.setExternalUrl(externalResource.location);
        headers.setExternalHandling(externalResource.handling);

        final var descId = joinId(fullId, FCR_METADATA_ID);
        final var descHeaders = createBinaryDescHeaders(fullId, descId, rdf);

        writeBinary(fullId, info.getInnerDirectory(), headers, null, descHeaders, rdf, true);
    }

    private void migrateAcl(final String parentId, final Path directory, final OcflObjectSession session) {
        final var fullId = joinId(parentId, FCR_ACL_ID);
        LOGGER.info("Migrating acl {}", fullId);

        final var file = directory.resolve(FCR_ACL + TTL_EXT);
        final var rdf = parseRdf(file);
        final var headers = createAclHeaders(parentId, fullId, rdf);

        session.writeResource(headers, writeRdf(fullId, rdf));
    }

    private void writeBinary(final String fullId,
                             final Path binaryDir,
                             final ResourceHeaders contentHeaders,
                             final InputStream content,
                             final ResourceHeaders descHeaders,
                             final Model rdf,
                             final boolean isFirstVersion) {
        final var session = objectSessionFactory.newSession(fullId);

        try {
            session.versionCreationTimestamp(contentHeaders.getLastModifiedDate().atOffset(ZoneOffset.UTC));
            session.writeResource(contentHeaders, content);
            session.writeResource(descHeaders, writeRdf(fullId, rdf));

            if (isFirstVersion && hasAcl(binaryDir)) {
                migrateAcl(fullId, binaryDir, session);
            }

            session.commit();
        } catch (RuntimeException e) {
            session.abort();
            throw new RuntimeException("Failed to migrate resource " + fullId, e);
        }
    }

    private void deleteObject(final String fullId) {
        try {
            final var session = objectSessionFactory.newSession(fullId);
            if (session.containsResource(fullId)) {
                LOGGER.debug("Deleting resource {} due to failed migration", fullId);
                session.deleteResource(fullId);
                session.commit();
            }
        } catch (RuntimeException e) {
            LOGGER.error("Failed to delete OCFL object for resource {}", fullId, e);
        }
    }

    private List<ResourceInfo> listAllChildren(final String parentId, final Path containerDir) {
        final var childMap = listDirectChildren(parentId, containerDir);
        final var children = new ArrayList<>(childMap.values());
        final var ghosts = listGhostNodes(containerDir, childMap.keySet());

        return ghosts.stream()
                .map(ghost -> {
                    final var name = decode(ghost.getFileName().toString());
                    final var id = joinId(parentId, name);
                    return listAllChildren(id, ghost);
                })
                .reduce(children, (l, r) -> {
                    l.addAll(r);
                    return l;
                });
    }

    private Map<String, ResourceInfo> listDirectChildren(final String parentId, final Path containerDir) {
        try (final var children = Files.list(containerDir)) {
            return children.filter(Files::isRegularFile)
                    .map(f -> f.getFileName().toString())
                    .filter(f -> !f.startsWith(FCR))
                    .filter(f -> !f.endsWith(HEADERS_EXT))
                    .map(filename -> {
                        final var stripped = extractName(filename);
                        final var decoded = decode(stripped);
                        final var fullId = joinId(parentId, decoded);

                        if (isBinary(filename)) {
                            return ResourceInfo.binary(parentId, fullId, containerDir, stripped);
                        } else if (isExternal(filename)) {
                            return ResourceInfo.externalBinary(parentId, fullId, containerDir, stripped);
                        } else if (isContainer(filename)) {
                            return ResourceInfo.container(parentId, fullId, containerDir, stripped);
                        }

                        return null;
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toMap(ResourceInfo::getNameEncoded, Function.identity()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<Path> listGhostNodes(final Path containerDir, final Set<String> children) {
        final var ghosts = new ArrayList<Path>();

        try (final var list = Files.list(containerDir)) {
            list.filter(Files::isDirectory).forEach(file -> {
                final var name = file.getFileName().toString();
                if (!children.contains(name)) {
                    ghosts.add(file);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return ghosts;
    }

    private boolean isBinary(final String filename) {
        return filename.endsWith(BINARY_EXT);
    }

    private boolean isExternal(final String filename) {
        return filename.endsWith(EXTERNAL_EXT);
    }

    private boolean isContainer(final String filename) {
        return filename.endsWith(TTL_EXT);
    }

    private List<String> identifyVersions(final Path directory) {
        try (final var children = Files.list(directory.resolve(FCR_VERSIONS))) {
            return children.filter(Files::isRegularFile)
                    .map(f -> f.getFileName().toString())
                    .filter(f -> !f.endsWith(HEADERS_EXT))
                    .map(f -> f.substring(0, f.lastIndexOf(".")))
                    .sorted(Comparator.comparing(n -> Instant.from(MEMENTO_FORMAT.parse(n))))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private InteractionModel identifyInteractionModel(final String fullId, final Model rdf) {
        for (final var it = listStatements(RDF.type, rdf); it.hasNext();) {
            final var statement = it.nextStatement();
            try {
                return InteractionModel.fromString(statement.getObject().toString());
            } catch (IllegalArgumentException e) {
                // ignore
            }
        }
        throw new IllegalStateException("Failed to identify interaction model for resource " + fullId);
    }

    private ResourceHeaders createCommonHeaders(final String parentId,
                                                final String fullId,
                                                final InteractionModel interactionModel,
                                                final Model rdf) {
        final var headers = new ResourceHeaders();

        headers.setId(fullId);
        headers.setParent(parentId);
        headers.setInteractionModel(interactionModel.getUri());
        headers.setArchivalGroup(false);
        headers.setDeleted(false);

        headers.setCreatedBy(getFirstValue(RdfConstants.FEDORA_CREATED_BY, rdf));
        headers.setCreatedDate(getDateValue(RdfConstants.FEDORA_CREATED_DATE, rdf));
        headers.setLastModifiedBy(getFirstValue(RdfConstants.FEDORA_LAST_MODIFIED_BY, rdf));
        headers.setLastModifiedDate(getDateValue(RdfConstants.FEDORA_LAST_MODIFIED_DATE, rdf));

        return headers;
    }

    private ResourceHeaders createContainerHeaders(final String parentId,
                                                   final String fullId,
                                                   final InteractionModel interactionModel,
                                                   final Model rdf) {
        final var headers = createCommonHeaders(parentId, fullId, interactionModel, rdf);
        headers.setObjectRoot(true);
        return headers;
    }

    private ResourceHeaders createBinaryDescHeaders(final String parentId, final String fullId, final Model rdf) {
        final var headers = createCommonHeaders(parentId, fullId, InteractionModel.NON_RDF_DESCRIPTION, rdf);
        headers.setObjectRoot(false);
        return headers;
    }

    private ResourceHeaders createAclHeaders(final String parentId, final String fullId, final Model rdf) {
        final var headers = createCommonHeaders(parentId, fullId, InteractionModel.ACL, rdf);
        headers.setObjectRoot(false);
        return headers;
    }

    private ResourceHeaders createBinaryHeaders(final String parentId, final String fullId, final Model rdf) {
        final var headers = createCommonHeaders(parentId, fullId, InteractionModel.NON_RDF, rdf);
        headers.setObjectRoot(true);
        headers.setContentSize(Long.valueOf(getFirstValue(RdfConstants.HAS_SIZE, rdf)));
        headers.setDigests(getAllUris(RdfConstants.HAS_MESSAGE_DIGEST, rdf));
        headers.setFilename(getFirstValue(RdfConstants.HAS_ORIGINAL_NAME, rdf));
        headers.setMimeType(getFirstValue(RdfConstants.EBUCORE_HAS_MIME_TYPE, rdf));
        headers.setStateToken(calculateStateToken(headers.getLastModifiedDate()));
        return headers;
    }

    private boolean hasVersions(final Path containerDir) {
        return Files.exists(containerDir.resolve(FCR_VERSIONS));
    }

    private boolean hasAcl(final Path containerDir) {
        return Files.exists(containerDir.resolve(FCR_ACL + TTL_EXT));
    }

    private Instant getDateValue(final Property predicate, final Model rdf) {
        final var value = getFirstValue(predicate, rdf);
        if (value == null) {
            return null;
        }
        return Instant.parse(value);
    }

    private List<URI> getAllUris(final Property predicate, final Model rdf) {
        final var values = new ArrayList<URI>();
        try {
            for (final var it = listStatements(predicate, rdf); it.hasNext();) {
                values.add(URI.create(it.nextStatement().getObject().toString()));
            }
        } catch (NoSuchElementException e) {
            // ignore
        }
        return values;
    }

    private String getFirstValue(final Property predicate, final Model rdf) {
        try {
            return listStatements(predicate, rdf)
                    .nextStatement().getObject().asLiteral().getString();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    private StmtIterator listStatements(final Property predicate, final Model rdf) {
        return rdf.listStatements(new SimpleSelector(null, predicate, (RDFNode) null));
    }

    private InputStream writeRdf(final String fullId, final Model rdf) {
        // TODO need to handle http://localhost:8080/rest/foo/fcr:acl#authorization
        final var subject = NodeFactory.createURI(fullId);

        try (final var baos = new ByteArrayOutputStream()) {
            final var writer = StreamRDFWriter.getWriterStream(baos, RDFFormat.NTRIPLES);
            writer.start();
            for (final var it = rdf.listStatements(); it.hasNext();) {
                final var statement = it.next();

                if (!isServerManagedTriple(statement)) {
                    final var triple = statement.asTriple();
                    // TODO need to translate object ids too
                    writer.triple(Triple.create(subject, triple.getPredicate(), triple.getObject()));
                }
            }
            writer.finish();
            return new ByteArrayInputStream(baos.toByteArray());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean isServerManagedTriple(final Statement statement) {
        return (statement.getPredicate().equals(RDF.type) && statement.getObject().isURIResource() &&
                statement.getObject().toString().startsWith(RdfConstants.LDP_NS)) ||
                RdfConstants.isManagedPredicate.test(statement.getPredicate());
    }

    private Model parseRdf(final Path path) {
        final var model = ModelFactory.createDefaultModel();
        try (final var is = Files.newInputStream(path)) {
            RDFDataMgr.read(model, is, Lang.TTL);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return model;
    }

    private String calculateStateToken(final Instant timestamp) {
        return DigestUtils.md5Hex(String.valueOf(timestamp.toEpochMilli())).toUpperCase();
    }

    private String joinId(final String id, final String part) {
        return id + "/" + part;
    }

    private String extractName(final String filename) {
        return filename.substring(0, filename.lastIndexOf("."));
    }

    private String decode(final String encoded) {
        return URLDecoder.decode(encoded, StandardCharsets.UTF_8);
    }

    private ExternalResource parseExternalResource(final ResourceInfo info) {
        final var file = info.getOuterDirectory().resolve(info.getNameEncoded() +EXTERNAL_EXT + HEADERS_EXT);
        try {
            final Map<String, List<String>> map = objectMapper.readValue(file.toFile(), Map.class);

            var handling = map.containsKey("Location") ? "redirect" : "proxy";
            var location = map.get("Content-Location").get(0);

            return new ExternalResource(location, handling);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static class ExternalResource {
        String location;
        String handling;

        public ExternalResource(String location, String handling) {
            this.location = location;
            this.handling = handling;
        }
    }

}
