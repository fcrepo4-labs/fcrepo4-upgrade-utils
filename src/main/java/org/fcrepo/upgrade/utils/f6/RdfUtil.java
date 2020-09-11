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

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author pwinckles
 */
public final class RdfUtil {

    private static final Logger LOGGER = getLogger(RdfUtil.class);

    private RdfUtil() {

    }

    public static Model parseRdf(final Path path, final Lang lang) {
        final var model = ModelFactory.createDefaultModel();
        try (final var is = Files.newInputStream(path)) {
            RDFDataMgr.read(model, is, lang);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return model;
    }

    public static InputStream writeRdfTranslateIds(final Model rdf,
                                                   final Lang lang,
                                                   final String original,
                                                   final String replacement) {
        try (final var baos = new ByteArrayOutputStream()) {
            final var writer = StreamRDFWriter.getWriterStream(baos, lang);
            writer.start();

            rdf.listStatements().filterDrop(RdfUtil::isServerManagedTriple)
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

    public static Instant getDateValue(final Property predicate, final Model rdf) {
        final var value = getFirstValue(predicate, rdf);
        if (value == null) {
            return null;
        }
        return Instant.parse(value);
    }

    public static List<URI> getUris(final Property predicate, final Model rdf) {
        final var values = new ArrayList<URI>();
        try {
            listStatements(predicate, rdf)
                    .mapWith(statement -> URI.create(statement.getObject().toString()))
                    .toList();
        } catch (NoSuchElementException e) {
            // ignore
        }
        return values;
    }

    public static String getFirstValue(final Property predicate, final Model rdf) {
        try {
            return listStatements(predicate, rdf)
                    .nextStatement().getObject().asLiteral().getString();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    public static StmtIterator listStatements(final Property predicate, final Model rdf) {
        return rdf.listStatements(new SimpleSelector(null, predicate, (RDFNode) null));
    }

    private static boolean isServerManagedTriple(final Statement statement) {
        return (statement.getPredicate().equals(RDF.type) && statement.getObject().isURIResource() &&
                statement.getObject().toString().startsWith(RdfConstants.LDP_NS)) ||
                RdfConstants.isManagedPredicate.test(statement.getPredicate());
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
