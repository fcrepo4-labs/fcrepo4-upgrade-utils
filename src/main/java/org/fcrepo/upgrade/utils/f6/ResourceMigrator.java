/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */

package org.fcrepo.upgrade.utils.f6;

import static java.time.ZoneOffset.UTC;
import static org.fcrepo.upgrade.utils.RdfConstants.FEDORA_CREATED_DATE;
import static org.fcrepo.upgrade.utils.RdfConstants.FEDORA_LAST_MODIFIED_DATE;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.riot.Lang;
import org.apache.jena.vocabulary.RDF;
import org.fcrepo.storage.ocfl.InteractionModel;
import org.fcrepo.storage.ocfl.OcflObjectSession;
import org.fcrepo.storage.ocfl.OcflObjectSessionFactory;
import org.fcrepo.storage.ocfl.ResourceHeaders;
import org.fcrepo.storage.ocfl.ResourceHeadersVersion;
import org.fcrepo.upgrade.utils.Config;
import org.fcrepo.upgrade.utils.RdfConstants;
import org.slf4j.Logger;

/**
 * Migrates a resource to F6.
 *
 * @author pwinckles
 */
public class ResourceMigrator {

    private static final Logger LOGGER = getLogger(ResourceMigrator.class);

    private static final DateTimeFormatter MEMENTO_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(UTC);

    private static final String BINARY_EXT = ".binary";
    private static final String EXTERNAL_EXT = ".external";
    private static final String HEADERS_EXT = ".headers";

    private static final String INFO_FEDORA = "info:fedora";
    private static final String FCR = "fcr%3A";
    private static final String FCR_VERSIONS = FCR + "versions";
    private static final String FCR_METADATA = FCR + "metadata";
    private static final String FCR_ACL = FCR + "acl";

    private static final String FCR_METADATA_ID = "fcr:metadata";
    private static final String FCR_ACL_ID = "fcr:acl";

    private final OcflObjectSessionFactory objectSessionFactory;
    private final ObjectMapper objectMapper;
    private final Lang srcRdfLang;
    private final String srcRdfExt;
    private final Lang dstRdfLang;
    private final String baseUri;

    /**
     * @param config the migration configuration
     * @param objectSessionFactory the OCFL object session factory
     */
    public ResourceMigrator(final Config config,
                            final OcflObjectSessionFactory objectSessionFactory) {
        this.objectSessionFactory = objectSessionFactory;
        this.objectMapper = new ObjectMapper();

        this.baseUri = stripTrailingSlash(config.getBaseUri());
        this.srcRdfLang = config.getSrcRdfLang();
        this.srcRdfExt = "." + srcRdfLang.getFileExtensions().get(0);

        // Currently, this is all F6 supports
        this.dstRdfLang = Lang.NT;
    }

    /**
     * Migrates a resource to F6 and returns a list of all of the resources direct children, if it has any.
     *
     * @param info the resource to migrate
     * @return list of direct children, if any
     */
    public List<ResourceInfo> migrate(final ResourceInfo info) {
        LOGGER.info("Migrating {}", info.getFullId());
        LOGGER.debug("Resource info: {}", info);

        try {
            if (!containsResource(info.getFullId())) {
                switch (info.getType()) {
                    case BINARY:
                        migrateBinary(info);
                        break;
                    case EXTERNAL_BINARY:
                        migrateExternalBinary(info);
                        break;
                    case CONTAINER:
                        migrateContainer(info);
                        break;
                    default:
                        throw new IllegalStateException("Unexpected resource type");
                }
            } else {
                LOGGER.info("Skipping {} because it has already been migrated", info.getFullId());
            }

            if (info.getType() == ResourceInfo.Type.CONTAINER) {
                return listAllChildren(info.getFullId(), info.getFullId(), info.getInnerDirectory());
            } else {
                return Collections.emptyList();
            }
        } catch (RuntimeException e) {
            LOGGER.info("Failed to migration resource {}. Rolling back...", info.getFullId());
            deleteObject(info.getFullId());
            throw e;
        }
    }

    /**
     * Closes the object session factory
     */
    public void close() {
        objectSessionFactory.close();
    }

    private void migrateContainer(final ResourceInfo info) {
        final var containerDir = info.getInnerDirectory();

        Instant lastVersionUpdate = null;

        if (hasVersions(info.getInnerDirectory())) {
            final var versions = identifyVersions(info.getInnerDirectory());

            for (final var version : versions) {
                LOGGER.info("Migrating {}/fcr:versions/{}", info.getFullId(), version);
                final var rdf = readRdf(containerDir.resolve(FCR_VERSIONS).resolve(rdfFile(version)));
                lastVersionUpdate = RdfUtil.getDateValue(FEDORA_LAST_MODIFIED_DATE, rdf);
                final var mementoInstant = parseMemento(version);

                migrateContainerVersion(info, containerDir, rdf, mementoInstant);
            }
        }

        final var rdf = readRdf(info.getOuterDirectory().resolve(rdfFile(info.getNameEncoded())));
        final var currentUpdate = getInstantPropertyFromRdf(info.getFullId(), FEDORA_LAST_MODIFIED_DATE, rdf,
                                                            Instant.now());
        // only migrate the state if it's different from the most recent memento
        if (lastVersionUpdate == null || !lastVersionUpdate.equals(currentUpdate)) {
            migrateContainerVersion(info, containerDir, rdf, currentUpdate);
        }
    }

    private void migrateContainerVersion(final ResourceInfo info,
                                         final Path containerDir,
                                         final Model rdf,
                                         final Instant timestamp) {
        final var interactionModel = identifyInteractionModel(info.getFullId(), rdf);

        final var headers = createContainerHeaders(info, interactionModel, rdf);

        doInSession(info.getFullId(), session -> {
            final var isFirst = !session.containsResource(info.getFullId());

            session.versionCreationTimestamp(timestamp.atOffset(ZoneOffset.UTC));
            session.writeResource(headers, writeRdf(rdf));

            if (isFirst && hasAcl(containerDir)) {
                migrateAcl(info.getFullId(), containerDir, session);
            }

            session.commit();
        });
    }

    private void migrateBinary(final ResourceInfo info) {
        final var binaryDir = info.getInnerDirectory();

        Instant lastVersionUpdate = null;

        if (hasVersions(info.getInnerDirectory())) {
            final var versions = identifyVersions(info.getInnerDirectory());

            for (final var version : versions) {
                LOGGER.info("Migrating {}/fcr:versions/{}", info.getFullId(), version);
                final var rdf = readRdf(binaryDir.resolve(FCR_METADATA)
                        .resolve(FCR_VERSIONS).resolve(rdfFile(version)));
                lastVersionUpdate = RdfUtil.getDateValue(RdfConstants.FEDORA_LAST_MODIFIED_DATE, rdf);
                final var mementoInstant = parseMemento(version);

                migrateBinaryVersion(info, binaryDir,
                        binaryDir.resolve(FCR_VERSIONS).resolve(binaryFile(version)), rdf, mementoInstant);
            }
        }

        final var rdf = readRdf(binaryDir.resolve(rdfFile(FCR_METADATA)));
        final var currentUpdate = RdfUtil.getDateValue(RdfConstants.FEDORA_LAST_MODIFIED_DATE, rdf);

        // only migrate the state if it's different from the most recent memento
        if (lastVersionUpdate == null || !lastVersionUpdate.equals(currentUpdate)) {
            migrateBinaryVersion(info, binaryDir,
                    info.getOuterDirectory().resolve(binaryFile(info.getNameEncoded())), rdf, currentUpdate);
        }
    }

    private void migrateBinaryVersion(final ResourceInfo info,
                                      final Path binaryDir,
                                      final Path binaryFile,
                                      final Model rdf,
                                      final Instant timestamp) {
        final var headers = createBinaryHeaders(info, rdf);

        final var descId = joinId(info.getFullId(), FCR_METADATA_ID);
        final var descHeaders = createBinaryDescHeaders(info.getFullId(), descId, rdf);

        try (final var stream = new BufferedInputStream(Files.newInputStream(binaryFile))) {
            writeBinary(info.getFullId(), binaryDir, headers, stream, descHeaders, rdf, timestamp);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void migrateExternalBinary(final ResourceInfo info) {
        final var rdf = readRdf(info.getInnerDirectory().resolve(rdfFile(FCR_METADATA)));
        final var headersBuilder = ResourceHeaders.builder(createBinaryHeaders(info, rdf));

        final var externalResource = parseExternalResource(info);
        headersBuilder.withExternalUrl(externalResource.location)
                .withExternalHandling(externalResource.handling);
        final var headers = headersBuilder.build();

        final var descId = joinId(info.getFullId(), FCR_METADATA_ID);
        final var descHeaders = createBinaryDescHeaders(info.getFullId(), descId, rdf);

        writeBinary(info.getFullId(), info.getInnerDirectory(), headers,
                null, descHeaders, rdf, headers.getLastModifiedDate());
    }

    private void migrateAcl(final String parentId, final Path directory, final OcflObjectSession session) {
        final var fullId = joinId(parentId, FCR_ACL_ID);
        LOGGER.info("Migrating {}", fullId);

        final var rdf = readRdf(directory.resolve(rdfFile(FCR_ACL)));
        final var headers = createAclHeaders(parentId, fullId, rdf);

        session.writeResource(headers, writeRdf(rdf));
    }

    private void writeBinary(final String fullId,
                             final Path binaryDir,
                             final ResourceHeaders contentHeaders,
                             final InputStream content,
                             final ResourceHeaders descHeaders,
                             final Model rdf,
                             final Instant timestamp) {
        doInSession(fullId, session -> {
            final var isFirst = !session.containsResource(fullId);

            session.versionCreationTimestamp(timestamp.atOffset(ZoneOffset.UTC));
            session.writeResource(contentHeaders, content);
            session.writeResource(descHeaders, writeRdf(rdf, true));

            if (isFirst && hasAcl(binaryDir)) {
                migrateAcl(fullId, binaryDir, session);
            }

            session.commit();
        });
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

    private boolean containsResource(final String fullId) {
        try (final var session = objectSessionFactory.newSession(fullId)) {
            return session.containsResource(fullId);
        }
    }

    /**
     * Lists all of the children of a container. This is complicated by the fact that a container can contain ghost
     * nodes between it and its children. This method will navigate ghost nodes down to the next concrete children.
     *
     * @param rootParentId the internal Fedora id of the container resource that was just processed
     * @param currentParentId a child of rootParentId used when expanding ghost nodes
     * @param containerDir the container's export directory
     * @return the container's direct children
     */
    private List<ResourceInfo> listAllChildren(final String rootParentId,
                                               final String currentParentId,
                                               final Path containerDir) {
        final var childMap = listDirectChildren(rootParentId, currentParentId, containerDir);
        final var children = new ArrayList<>(childMap.values());
        final var ghosts = listGhostNodes(containerDir, childMap.keySet());

        return ghosts.stream()
                .map(ghost -> {
                    final var name = decode(ghost.getFileName().toString());
                    return listAllChildren(rootParentId, joinId(currentParentId, name), ghost);
                })
                .reduce(children, (l, r) -> {
                    l.addAll(r);
                    return l;
                });
    }

    /**
     * Returns all of the children that are children of a container and not within a ghost node.
     *
     * @param rootParentId the internal Fedora id of the container resource that was just processed
     * @param currentParentId a child of rootParentId used when expanding ghost nodes
     * @param containerDir the container's export directory
     * @return the container's children not under ghost nodes
     */
    private Map<String, ResourceInfo> listDirectChildren(final String rootParentId,
                                                         final String currentParentId,
                                                         final Path containerDir) {
        if (!containerDir.toFile().exists()) {
            return Collections.emptyMap();
        }

        try (final var children = Files.list(containerDir)) {
            return children.filter(Files::isRegularFile)
                    .map(f -> f.getFileName().toString())
                    .filter(f -> !f.startsWith(FCR))
                    .filter(f -> !f.endsWith(HEADERS_EXT))
                    .map(filename -> {
                        final var stripped = extractName(filename);
                        final var decoded = decode(stripped);
                        final var fullId = joinId(currentParentId, decoded);

                        if (isBinary(filename)) {
                            return ResourceInfo.binary(rootParentId, fullId, containerDir, stripped);
                        } else if (isExternal(filename)) {
                            return ResourceInfo.externalBinary(rootParentId, fullId, containerDir, stripped);
                        } else if (isContainer(filename)) {
                            return ResourceInfo.container(rootParentId, fullId, containerDir, stripped);
                        }

                        return null;
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toMap(ResourceInfo::getNameEncoded, Function.identity()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Lists all of the ghost nodes under a container
     *
     * @param containerDir the container's export directory
     * @param children the set of concrete children in the container
     * @return list of ghost nodes
     */
    private List<Path> listGhostNodes(final Path containerDir, final Set<String> children) {
        if (!containerDir.toFile().exists()) {
            return Collections.emptyList();
        }

        final var ghosts = new ArrayList<Path>();
        try (final var list = Files.list(containerDir)) {
            list.filter(Files::isDirectory)
                    .filter(f -> !f.getFileName().toString().startsWith(FCR))
                    .forEach(file -> {
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

    private List<String> identifyVersions(final Path directory) {
        try (final var children = Files.list(directory.resolve(FCR_VERSIONS))) {
            return children.filter(Files::isRegularFile)
                    .map(f -> f.getFileName().toString())
                    .filter(f -> !f.endsWith(HEADERS_EXT))
                    .map(f -> f.substring(0, f.lastIndexOf(".")))
                    .sorted(Comparator.comparing(this::parseMemento))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Instant parseMemento(final String memento) {
        return Instant.from(MEMENTO_FORMAT.parse(memento));
    }

    private InteractionModel identifyInteractionModel(final String fullId, final Model rdf) {
        for (final var it = RdfUtil.listStatements(RDF.type, rdf); it.hasNext();) {
            final var statement = it.nextStatement();
            try {
                return InteractionModel.fromString(statement.getObject().toString());
            } catch (IllegalArgumentException e) {
                // ignore
            }
        }
        throw new IllegalStateException("Failed to identify interaction model for resource " + fullId);
    }

    private void doInSession(final String fullId, final Consumer<OcflObjectSession> runnable) {
        final var session = objectSessionFactory.newSession(fullId);
        try {
            runnable.accept(session);
        } catch (RuntimeException e) {
            session.abort();
            throw new RuntimeException("Failed to migrate resource " + fullId, e);
        }
    }

    private ResourceHeaders.Builder createCommonHeaders(final String parentId,
                                                final String fullId,
                                                final InteractionModel interactionModel,
                                                final Model rdf) {
        final var headers = ResourceHeaders.builder();
        final var now = Instant.now();
        final var created = getInstantPropertyFromRdf(fullId, FEDORA_CREATED_DATE, rdf, now);
        var lastModified = getInstantPropertyFromRdf(fullId, FEDORA_LAST_MODIFIED_DATE, rdf, null);
        if (lastModified == null || lastModified.isBefore(created)) {
            if(lastModified != null) {
                LOGGER.warn("The value of the {} property ({}) precedes the created date ({}) for {}:  Setting the " +
                            "last modified date to the value of the create date.", FEDORA_LAST_MODIFIED_DATE,
                            lastModified, created, fullId);
            }
            lastModified = now;
        }

        headers.withId(fullId)
                .withHeadersVersion(ResourceHeadersVersion.V1_0)
                .withParent(parentId)
                .withInteractionModel(interactionModel.getUri())
                .withArchivalGroup(false)
                .withDeleted(false)
                .withCreatedBy(RdfUtil.getFirstValue(RdfConstants.FEDORA_CREATED_BY, rdf))
                .withCreatedDate(created)
                .withLastModifiedBy(RdfUtil.getFirstValue(RdfConstants.FEDORA_LAST_MODIFIED_BY, rdf))
                .withLastModifiedDate(lastModified)
                .withMementoCreatedDate(lastModified)
                .withStateToken(calculateStateToken(lastModified));

        if (created == null) {
            headers.withCreatedDate(lastModified);
        }

        return headers;
    }

    private Instant getInstantPropertyFromRdf(String fullId, final Property property, Model rdf, Instant defaultValue) {
        var value = RdfUtil.getDateValue(property, rdf);
        if (value == null) {
            LOGGER.warn("The {} property is not defined for {}:  -> returning defaultValue: {}", property, fullId,
                        defaultValue);
            value = defaultValue;
        }
        return value;
    }

    private ResourceHeaders createContainerHeaders(final ResourceInfo info,
                                                   final InteractionModel interactionModel,
                                                   final Model rdf) {
        final var headers = createCommonHeaders(info.getParentId(), info.getFullId(),
                interactionModel, rdf);
        headers.withObjectRoot(true);
        return headers.build();
    }

    private ResourceHeaders createBinaryDescHeaders(final String parentId, final String fullId, final Model rdf) {
        final var headers = createCommonHeaders(parentId, fullId, InteractionModel.NON_RDF_DESCRIPTION, rdf);
        headers.withObjectRoot(false);
        return headers.build();
    }

    private ResourceHeaders createAclHeaders(final String parentId, final String fullId, final Model rdf) {
        final var headers = createCommonHeaders(parentId, fullId, InteractionModel.ACL, rdf);
        headers.withObjectRoot(false);
        return headers.build();
    }

    private ResourceHeaders createBinaryHeaders(final ResourceInfo info, final Model rdf) {
        final var headers = createCommonHeaders(info.getParentId(), info.getFullId(),
                InteractionModel.NON_RDF, rdf);
        headers.withObjectRoot(true)
                .withContentSize(Long.valueOf(RdfUtil.getFirstValue(RdfConstants.HAS_SIZE, rdf)))
                .withDigests(RdfUtil.getUris(RdfConstants.HAS_MESSAGE_DIGEST, rdf))
                .withFilename(RdfUtil.getFirstValue(RdfConstants.HAS_ORIGINAL_NAME, rdf))
                .withMimeType(RdfUtil.getFirstValue(RdfConstants.EBUCORE_HAS_MIME_TYPE, rdf));
        return headers.build();
    }

    private Model readRdf(final Path path) {
        return RdfUtil.parseRdf(path, srcRdfLang);
    }

    private InputStream writeRdf(final Model rdf, final boolean isBinaryFile) {
        return RdfUtil.writeRdfTranslateIds(rdf, dstRdfLang, baseUri, INFO_FEDORA, isBinaryFile);
    }

    private InputStream writeRdf(final Model rdf) {
        return writeRdf(rdf, false);
    }

    private boolean hasVersions(final Path containerDir) {
        return Files.exists(containerDir.resolve(FCR_VERSIONS));
    }

    private boolean hasAcl(final Path containerDir) {
        return Files.exists(containerDir.resolve(rdfFile(FCR_ACL)));
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

    private boolean isBinary(final String filename) {
        return filename.endsWith(BINARY_EXT);
    }

    private boolean isExternal(final String filename) {
        return filename.endsWith(EXTERNAL_EXT);
    }

    private boolean isContainer(final String filename) {
        return filename.endsWith(srcRdfExt);
    }

    private String rdfFile(final String name) {
        return name + srcRdfExt;
    }

    private String binaryFile(final String name) {
        return name + BINARY_EXT;
    }

    private ExternalResource parseExternalResource(final ResourceInfo info) {
        final var file = info.getOuterDirectory().resolve(info.getNameEncoded() + EXTERNAL_EXT + HEADERS_EXT);
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

    private static String stripTrailingSlash(final String value) {
        if (value.endsWith("/")) {
            return value.replaceAll("/+$", "");
        }
        return value;
    }

}
