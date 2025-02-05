/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */

package org.fcrepo.upgrade.utils.f6;

import org.apache.jena.graph.Node;
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
import org.apache.jena.riot.system.StreamRDFWriter;
import org.apache.jena.vocabulary.RDF;
import org.fcrepo.upgrade.utils.RdfConstants;
import org.slf4j.Logger;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Util class for working with RDF.
 *
 * @author pwinckles
 */
public final class RdfUtil {

    private static final Logger LOGGER = getLogger(RdfUtil.class);

    private RdfUtil() {
        // noop
    }

    /**
     * Parses an RDF document.
     *
     * @param path the path to the document
     * @param lang the language of the document
     * @return the parsed RDF
     */
    public static Model parseRdf(final Path path, final Lang lang) {
        final var model = ModelFactory.createDefaultModel();
        try (final var is = new BufferedInputStream(Files.newInputStream(path))) {
            RDFDataMgr.read(model, is, lang);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return model;
    }

    /**
     * Writes RDF to an input stream. Server managed triples are filtered out,
     * and subject and object uris are translated.
     *
     * @param rdf the RDF to serialize
     * @param lang the RDF language to output
     * @param original the uri fragment to be replaced
     * @param replacement the uri fragment to be inserted
     * @return serialized rdf input stream
     */
    public static InputStream writeRdfTranslateIds(final Model rdf,
                                                   final Lang lang,
                                                   final String original,
                                                   final String replacement,
                                                   final boolean isBinary) {
        try (final var baos = new ByteArrayOutputStream()) {
            final var writer = StreamRDFWriter.getWriterStream(baos, lang);
            writer.start();

            Predicate<Statement> dropStatement = s -> isServerManagedTriple(s) || (isBinary && RdfConstants.binaryProperties.contains(s.getPredicate()));
            rdf.listStatements().filterDrop(dropStatement)
                    .mapWith(Statement::asTriple)
                    .mapWith(triple -> {
                        return Triple.create(translateId(triple.getSubject(), original, replacement),
                                triple.getPredicate(),
                                translateId(triple.getObject(), original, replacement));
                    })
                    .forEachRemaining(writer::triple);

            writer.finish();
            return new ByteArrayInputStream(baos.toByteArray());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Returns the first object, parsed as an instant, that matches the predicate.
     *
     * @param predicate the predicate to match
     * @param rdf the RDF to search
     * @return instant
     */
    public static Instant getDateValue(final Property predicate, final Model rdf) {
        final var value = getFirstValue(predicate, rdf);
        if (value == null) {
            return null;
        }
        return Instant.parse(value);
    }

    /**
     * Returns all of the objects, parsed as URIs, that match the specified predicate.
     *
     * @param predicate the predicate to match
     * @param rdf the RDF to search
     * @return uris
     */
    public static List<URI> getUris(final Property predicate, final Model rdf) {
        try {
            return listStatements(predicate, rdf)
                    .mapWith(statement -> URI.create(statement.getObject().toString()))
                    .toList();
        } catch (NoSuchElementException e) {
            return new ArrayList<>();
        }
    }

    /**
     * Returns the first object, as a string, that matches the specified predicate.
     *
     * @param predicate the predicate to match
     * @param rdf the RDF to search
     * @return string
     */
    public static String getFirstValue(final Property predicate, final Model rdf) {
        try {
            return listStatements(predicate, rdf)
                    .nextStatement().getObject().asLiteral().getString();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    /**
     * Returns all of the statements that match a predicate.
     *
     * @param predicate the predicate to match
     * @param rdf the RDF to search
     * @return statement iter
     */
    public static StmtIterator listStatements(final Property predicate, final Model rdf) {
        return rdf.listStatements(new SimpleSelector(null, predicate, (RDFNode) null));
    }

    private static boolean isServerManagedTriple(final Statement statement) {
        return isManagedType(statement) ||
                RdfConstants.isManagedPredicate.test(statement.getPredicate());
    }

    private static boolean isManagedType(final Statement statement) {
        return statement.getPredicate().equals(RDF.type) && statement.getObject().isURIResource() &&
                (startsWith(statement.getObject(), RdfConstants.LDP_NS) ||
                        startsWith(statement.getObject(), RdfConstants.FEDORA_NS));
    }

    private static boolean startsWith(final RDFNode node, final String prefix) {
        return node.toString().startsWith(prefix);
    }

    private static Node translateId(final Node node, final String original, final String replacement) {
        if (node.isURI()) {
            final var uri = node.getURI();
            if (uri.startsWith(original)) {
                final var newUri = stripTrailingSlash(uri.replaceFirst(original, replacement));
                LOGGER.trace("Translating {} to {}", uri, newUri);
                return NodeFactory.createURI(newUri);
            }
        }
        return node;
    }

    private static String stripTrailingSlash(final String value) {
        if (value.endsWith("/")) {
            return value.replaceAll("/+$", "");
        }
        return value;
    }

}
