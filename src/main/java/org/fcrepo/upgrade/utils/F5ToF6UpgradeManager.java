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
 * @author dbernstein
 * @since 2020-08-05
 */
class F5ToF6UpgradeManager implements UpgradeManager {

    private static final Logger LOGGER = getLogger(F5ToF6UpgradeManager.class);

    private final Config config;
    private final OcflObjectSessionFactory objectSessionFactory;

    /**
     * Constructor
     */
    public F5ToF6UpgradeManager(final Config config, final OcflObjectSessionFactory objectSessionFactory) {
        this.config = config;
        this.objectSessionFactory = objectSessionFactory;
    }

    /**
     * Runs the upgrade util process
     */
    public void start() {
        LOGGER.info("Starting upgrade: config={}", config);

        /*

        - Migrate root?
        - Migrate root acl
        - Files ending with ID.ttl are containers (note: fcr files also end in ttl)
        - Files ending with ID.binary are binaries
        - ID/fcr%3Aversions/ contains versions
        - ID/fcr%3Ametadata.ttl contains the binary meta
        - ID/fcr%3Ametadata/fcr%3Aversions/ contains binary meta versions
        - Presumably ID/fcr%3Aacl.ttl contains web acls
        - Tombstones? ID/fcr%3Atombstone.ttl?
        - What triples should be stripped?
        - How are ghost nodes exported?
        - External references?

        Parallelize by having a fixed pool of worker threads. Work is at the container level.

        1. Process resources breadth first
            1. Check if container has any versions, if so each version must be created in order
            2. Create resource headers
            3. Transform RDF, removing any server managed triples
            4. Add to OCFL and create version, with correct timestamp
            5. If there is a web acl, it should be added in the first version
            6. If there is a tombstone, the resource should be deleted
            7. Add any child containers to work queue
            8. Process any child binaries:
                1. Check if binary has any versions, if so each version must be created in order
                2. Create resource headers
                3. Add to OCFL, but do not create version
                4. If there is a web acl, add it, but only to the first version
                5. Add the binary description
                6. Create a new version, with the correct timestamp (there is a problem here where the last modified timestamp is going to get touched...)
                7. If there is a tombstone, the resource, and its description, should be deleted

         */

        // TODO for testing
        processContainer(null, "info:fedora", "rest", config.getInputDir().toPath());

        LOGGER.info("Upgrade complete.");
        objectSessionFactory.close();
    }

    private static final String TTL = ".ttl";
    private static final String BINARY = ".binary";

    private static final String FCR = "fcr%3A";
    private static final String FCR_VERSIONS = FCR + "versions";
    private static final String FCR_METADATA = FCR + "metadata";
    private static final String FCR_ACL = FCR + "acl";
    private static final String FCR_TOMBSTONE = FCR + "tombstone";

    private static final String FCR_METADATA_ID = "fcr:metadata";

    // TODO move
    // TODO exception behavior? restarts?
    private void processContainer(final String parentId, final String fullId,
                                  final String idLastPart, final Path directory) {
        LOGGER.info("Processing container {}", fullId);
        // TODO I'm not sure what the format for a tombstoned resource is. Do the old files still exist or just the tombstone?
        // TODO external resources?
        // TODO ghost nodes?

        final var containerDir = directory.resolve(idLastPart);

        if (hasVersions(containerDir)) {
            // TODO process versions
        } else {
            final var rdf = parseRdf(directory.resolve(idLastPart + TTL));
            final var headers = createContainerHeaders(parentId, fullId, rdf);
            // TODO this could be done during the write so there's only one iter
            removeServerManagedTriples(rdf);

            final var session = objectSessionFactory.newSession(fullId);
            try {
                session.versionCreationTimestamp(headers.getLastModifiedDate().atOffset(ZoneOffset.UTC));
                session.writeResource(headers, writeRdf(fullId, rdf));

                if (hasAcl(containerDir)) {
                    // TODO add acl
                }

                session.commit();
            } catch (RuntimeException e) {
                session.abort();
                throw new RuntimeException("Failed to migrate resource " + fullId, e);
            }
        }

        final var binaries = identifyBinaries(containerDir);
        final var containers = identifyContainers(containerDir);

        binaries.forEach(binary -> {
            processBinary(fullId, binary, containerDir);
        });

        // TODO use worker pool
        // TODO add containers to work queue before processing binaries
        containers.forEach(container -> {
            final var childId = fullId + "/" + container;
            processContainer(fullId, childId, container, containerDir);
        });
    }

    private void processBinary(final String parentId, final String idLastPart, final Path directory) {
        final var fullId = parentId + "/" + idLastPart;
        LOGGER.info("Processing binary {}", fullId);

        // TODO I'm not sure what the format for a tombstoned resource is. Do the old files still exist or just the tombstone?

        final var binaryDir = directory.resolve(idLastPart);

        if (hasVersions(binaryDir)) {
            // TODO process versions
        } else {
            final var rdf = parseRdf(binaryDir.resolve(FCR_METADATA + TTL));
            final var headers = createBinaryHeaders(parentId, fullId, rdf);

            // TODO make id joiner
            final var descId = fullId + "/" + FCR_METADATA_ID;
            final var descHeaders = createBinaryDescHeaders(fullId, descId, rdf);

            // TODO this could be done during the write so there's only one iter
            removeServerManagedTriples(rdf);

            final var session = objectSessionFactory.newSession(fullId);
            try {
                session.versionCreationTimestamp(headers.getLastModifiedDate().atOffset(ZoneOffset.UTC));

                try (final var stream = Files.newInputStream(directory.resolve(idLastPart + BINARY))) {
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

    private void removeServerManagedTriples(final Model rdf) {
        final var toRemove = new ArrayList<Statement>();
        for (final var it = rdf.listStatements(); it.hasNext();) {
            final var statement = it.next();
            if ((statement.getPredicate().equals(RDF.type) && statement.getObject().isURIResource() &&
                    statement.getObject().toString().startsWith(RdfConstants.LDP_NS)) ||
                    RdfConstants.isManagedPredicate.test(statement.getPredicate())) {
                toRemove.add(statement);
            }
        }
        rdf.remove(toRemove);
    }

    private ResourceHeaders createContainerHeaders(final String parentId, final String fullId, final Model rdf) {
        final var headers = new ResourceHeaders();

        headers.setId(fullId);
        headers.setParent(parentId);
        // TODO should extract from rdf?
        headers.setInteractionModel(InteractionModel.BASIC_CONTAINER.getUri());
        headers.setArchivalGroup(false);
        headers.setObjectRoot(true);
        headers.setDeleted(false);

        headers.setCreatedBy(getFirstValue(RdfConstants.FEDORA_CREATED_BY, rdf));
        headers.setCreatedDate(getDateValue(RdfConstants.FEDORA_CREATED_DATE, rdf));
        headers.setLastModifiedBy(getFirstValue(RdfConstants.FEDORA_LAST_MODIFIED_BY, rdf));
        headers.setLastModifiedDate(getDateValue(RdfConstants.FEDORA_LAST_MODIFIED_DATE, rdf));

        return headers;
    }

    private ResourceHeaders createBinaryDescHeaders(final String parentId, final String fullId, final Model rdf) {
        final var headers = new ResourceHeaders();

        headers.setId(fullId);
        headers.setParent(parentId);
        headers.setInteractionModel(InteractionModel.NON_RDF_DESCRIPTION.getUri());
        headers.setArchivalGroup(false);
        headers.setObjectRoot(false);
        headers.setDeleted(false);

        headers.setCreatedBy(getFirstValue(RdfConstants.FEDORA_CREATED_BY, rdf));
        headers.setCreatedDate(getDateValue(RdfConstants.FEDORA_CREATED_DATE, rdf));
        headers.setLastModifiedBy(getFirstValue(RdfConstants.FEDORA_LAST_MODIFIED_BY, rdf));
        headers.setLastModifiedDate(getDateValue(RdfConstants.FEDORA_LAST_MODIFIED_DATE, rdf));

        return headers;
    }

    private ResourceHeaders createBinaryHeaders(final String parentId, final String fullId, final Model rdf) {
        final var headers = new ResourceHeaders();

        // TODO refactor to collapse shared code
        headers.setId(fullId);
        headers.setParent(parentId);
        headers.setInteractionModel(InteractionModel.NON_RDF.getUri());
        headers.setArchivalGroup(false);
        headers.setObjectRoot(true);
        headers.setDeleted(false);

        headers.setCreatedBy(getFirstValue(RdfConstants.FEDORA_CREATED_BY, rdf));
        headers.setCreatedDate(getDateValue(RdfConstants.FEDORA_CREATED_DATE, rdf));
        headers.setLastModifiedBy(getFirstValue(RdfConstants.FEDORA_LAST_MODIFIED_BY, rdf));
        headers.setLastModifiedDate(getDateValue(RdfConstants.FEDORA_LAST_MODIFIED_DATE, rdf));

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
        // TODO not sure if this parsing is correct
        return Instant.parse(value);
    }

    private List<URI> getAllUris(final Property predicate, final Model rdf) {
        return getAllValues(predicate, rdf).stream()
                .map(URI::create)
                .collect(Collectors.toList());
    }

    private String getFirstValue(final Property predicate, final Model rdf) {
        try {
            return listStatements(predicate, rdf)
                    .nextStatement().getObject().toString();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    private List<String> getAllValues(final Property predicate, final Model rdf) {
        final var values = new ArrayList<String>();
        try {
            for (final var it = listStatements(predicate, rdf); it.hasNext();) {
                values.add(it.nextStatement().getObject().toString());
            }
        } catch (NoSuchElementException e) {
            // ignore
        }
        return values;
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
                final var triple = it.next().asTriple();
                // TODO need to translate object ids too
                writer.triple(Triple.create(subject, triple.getPredicate(), triple.getObject()));
            }
            writer.finish();
            return new ByteArrayInputStream(baos.toByteArray());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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

}
