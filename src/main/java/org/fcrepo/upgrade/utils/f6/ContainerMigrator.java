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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author pwinckles
 */
public class ContainerMigrator {

    private static final Logger LOGGER = getLogger(ContainerMigrator.class);

    private static final String TTL = ".ttl";
    private static final String BINARY = ".binary";

    private static final String FCR = "fcr%3A";
    private static final String FCR_VERSIONS = FCR + "versions";
    private static final String FCR_METADATA = FCR + "metadata";
    private static final String FCR_ACL = FCR + "acl";
    private static final String FCR_TOMBSTONE = FCR + "tombstone";

    private static final String FCR_METADATA_ID = "fcr:metadata";

    private final OcflObjectSessionFactory objectSessionFactory;

    public ContainerMigrator(final OcflObjectSessionFactory objectSessionFactory) {
        this.objectSessionFactory = objectSessionFactory;
    }

    public List<ContainerInfo> migrateContainer(final ContainerInfo info) {
        LOGGER.info("Migrating container {}", info.getFullId());
        // TODO I'm not sure what the format for a tombstoned resource is. Do the old files still exist or just the tombstone?
        // TODO external resources?
        // TODO ghost nodes?
        // TODO direct/indirect containers

        final var containerDir = info.getDirectory().resolve(info.getIdLastPart());

        try {
            if (hasVersions(containerDir)) {
                final var versions = identifyVersions(containerDir);
                LOGGER.debug("Resource {} has versions: {}", info.getFullId(), versions);

                versions.forEach(version -> {
                    migrateContainerVersion(info, containerDir,
                            containerDir.resolve(FCR_VERSIONS).resolve(version + TTL));
                });
            } else {
                migrateContainerVersion(info, containerDir,
                        info.getDirectory().resolve(info.getIdLastPart() + TTL));
            }
        } catch (RuntimeException e) {
            LOGGER.info("Failed to migration resource {}. Rolling back...", info.getFullId());
            deleteObject(info.getFullId());
            throw e;
        }

        final var binaries = identifyBinaries(containerDir);
        // TODO this is not correct -- need to inspect interaction model
        final var containers = identifyContainers(containerDir);

        try {
            binaries.forEach(binary -> {
                migrateBinary(info.getFullId(), binary, containerDir);
            });
        } catch (RuntimeException e) {
            LOGGER.info("Failed to migration resource {}. Rolling back...", info.getFullId());
            deleteObject(info.getFullId());
            binaries.forEach(binary -> {
                deleteObject(joinId(info.getFullId(), binary));
            });
            throw e;
        }

        return containers.stream().map(container -> {
            final var childId = joinId(info.getFullId(), container);
            return new ContainerInfo(info.getFullId(), childId, container, containerDir);
        }).collect(Collectors.toList());
    }

    public void close() {
        objectSessionFactory.close();
    }

    private void migrateContainerVersion(final ContainerInfo info,
                                         final Path containerDir,
                                         final Path rdfFile) {
        final var rdf = parseRdf(rdfFile);
        final var headers = createContainerHeaders(info.getParentId(), info.getFullId(), rdf);

        final var session = objectSessionFactory.newSession(info.getFullId());
        try {
            session.versionCreationTimestamp(headers.getLastModifiedDate().atOffset(ZoneOffset.UTC));
            session.writeResource(headers, writeRdf(info.getFullId(), rdf));

            if (hasAcl(containerDir)) {
                // TODO add acl
            }

            session.commit();
        } catch (RuntimeException e) {
            session.abort();
            throw new RuntimeException("Failed to migrate resource " + info.getFullId(), e);
        }
    }

    private void migrateBinary(final String parentId, final String idLastPart, final Path directory) {
        final var fullId = joinId(parentId, idLastPart);
        LOGGER.info("Migrating binary {}", fullId);

        // TODO I'm not sure what the format for a tombstoned resource is. Do the old files still exist or just the tombstone?

        final var binaryDir = directory.resolve(idLastPart);

        if (hasVersions(binaryDir)) {
            final var versions = identifyVersions(binaryDir);
            LOGGER.debug("Resource {} has versions: {}", fullId, versions);

            versions.forEach(version -> {
                migrateBinaryVersion(parentId, fullId, binaryDir,
                        binaryDir.resolve(FCR_VERSIONS).resolve(version + BINARY),
                        binaryDir.resolve(FCR_METADATA).resolve(FCR_VERSIONS).resolve(version + TTL));
            });
        } else {
            migrateBinaryVersion(parentId, fullId, binaryDir,
                    directory.resolve(idLastPart + BINARY),
                    binaryDir.resolve(FCR_METADATA + TTL));
        }
    }

    private void migrateBinaryVersion(final String parentId,
                                      final String fullId,
                                      final Path binaryDir,
                                      final Path binaryFile,
                                      final Path descFile) {
        final var rdf = parseRdf(descFile);
        final var headers = createBinaryHeaders(parentId, fullId, rdf);

        final var descId = joinId(fullId, FCR_METADATA_ID);
        final var descHeaders = createBinaryDescHeaders(fullId, descId, rdf);

        final var session = objectSessionFactory.newSession(fullId);
        try {
            session.versionCreationTimestamp(headers.getLastModifiedDate().atOffset(ZoneOffset.UTC));

            try (final var stream = Files.newInputStream(binaryFile)) {
                session.writeResource(headers, stream);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            session.writeResource(descHeaders, writeRdf(fullId, rdf));

            if (hasAcl(binaryDir)) {
                // TODO add acl
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

    private List<String> identifyBinaries(final Path containerDir) {
        try (final var children = Files.list(containerDir)) {
            return children.filter(Files::isRegularFile)
                    .map(f -> f.getFileName().toString())
                    .filter(f -> f.endsWith(BINARY))
                    .map(f -> f.substring(0, f.lastIndexOf(".")))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<String> identifyContainers(final Path containerDir) {
        try (final var children = Files.list(containerDir)) {
            return children.filter(Files::isRegularFile)
                    .map(f -> f.getFileName().toString())
                    .filter(f -> f.endsWith(TTL))
                    .filter(f -> !f.startsWith(FCR))
                    .map(f -> f.substring(0, f.lastIndexOf(".")))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<String> identifyVersions(final Path directory) {
        try (final var children = Files.list(directory.resolve(FCR_VERSIONS))) {
            return children.filter(Files::isRegularFile)
                    .map(f -> f.getFileName().toString())
                    .filter(f -> !f.endsWith(".headers"))
                    .map(f -> f.substring(0, f.lastIndexOf(".")))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private ResourceHeaders createCommonHeaders(final String parentId, final String fullId, final Model rdf) {
        final var headers = new ResourceHeaders();

        headers.setId(fullId);
        headers.setParent(parentId);
        // TODO not sure if this is correct for direct/indirect containers
        headers.setInteractionModel(identifyInteractionModel(fullId, rdf).getUri());
        headers.setArchivalGroup(false);
        headers.setDeleted(false);

        headers.setCreatedBy(getFirstValue(RdfConstants.FEDORA_CREATED_BY, rdf));
        headers.setCreatedDate(getDateValue(RdfConstants.FEDORA_CREATED_DATE, rdf));
        headers.setLastModifiedBy(getFirstValue(RdfConstants.FEDORA_LAST_MODIFIED_BY, rdf));
        headers.setLastModifiedDate(getDateValue(RdfConstants.FEDORA_LAST_MODIFIED_DATE, rdf));

        return headers;
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

    private ResourceHeaders createContainerHeaders(final String parentId, final String fullId, final Model rdf) {
        final var headers = createCommonHeaders(parentId, fullId, rdf);
        headers.setObjectRoot(true);
        return headers;
    }

    private ResourceHeaders createBinaryDescHeaders(final String parentId, final String fullId, final Model rdf) {
        final var headers = createCommonHeaders(parentId, fullId, rdf);
        headers.setInteractionModel(InteractionModel.NON_RDF_DESCRIPTION.getUri());
        headers.setObjectRoot(false);
        return headers;
    }

    private ResourceHeaders createBinaryHeaders(final String parentId, final String fullId, final Model rdf) {
        final var headers = createCommonHeaders(parentId, fullId, rdf);
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
        // TODO not sure this is right
        return Files.exists(containerDir.resolve(FCR_ACL + TTL));
    }

    private boolean hasTombstone(final Path containerDir) {
        // TODO not sure this is right
        return Files.exists(containerDir.resolve(FCR_TOMBSTONE + TTL));
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

}
