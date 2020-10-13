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

import com.google.common.collect.ImmutableSet;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableSet.of;
import static org.apache.jena.rdf.model.ResourceFactory.createProperty;

/**
 * RDF-related constants
 *
 * @author dbernstein
 */
public class RdfConstants {

    private RdfConstants() {
    }

    public static final String FEDORA_NS = "http://fedora.info/definitions/v4/repository#";
    public static final String LDP_NS = "http://www.w3.org/ns/ldp#";
    public static final String MEMENTO_NS = "http://mementoweb.org/ns#";
    public static final String PREMIS_NS = "http://www.loc.gov/premis/rdf/v1#";
    public static final String EBUCORE_NS = "http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#";

    public static final Resource LDP_NON_RDF_SOURCE = ResourceFactory.createResource(LDP_NS + "NonRDFSource");
    public static final Resource LDP_RDF_SOURCE = ResourceFactory.createResource(LDP_NS + "RDFSource");

    public static final Resource NON_RDF_SOURCE_DESCRIPTION = ResourceFactory.createResource(FEDORA_NS +
                                                                                            "NonRdfSourceDescription");
    public static final Resource LDP_CONTAINER = ResourceFactory.createResource(LDP_NS + "Container");
    public static final Resource LDP_DIRECT_CONTAINER = ResourceFactory.createResource(LDP_NS + "DirectContainer");
    public static final Resource LDP_INDIRECT_CONTAINER = ResourceFactory.createResource(LDP_NS + "IndirectContainer");
    public static final Resource LDP_BASIC_CONTAINER = ResourceFactory.createResource(LDP_NS + "BasicContainer");
    public static final List<Resource> LDP_CONTAINER_TYPES = Arrays.asList(LDP_BASIC_CONTAINER,
                                                                           LDP_DIRECT_CONTAINER,
                                                                           LDP_INDIRECT_CONTAINER);

    public static final Resource FEDORA_VERSION = ResourceFactory.createResource(FEDORA_NS + "Version");
    public static final Resource MEMENTO = ResourceFactory.createResource(MEMENTO_NS + "Memento");

    public static final Property EBUCORE_HAS_MIME_TYPE =
        createProperty(EBUCORE_NS + "hasMimeType");
    public static final Property HAS_ORIGINAL_NAME =
            createProperty(EBUCORE_NS + "filename");

    public static final Property FEDORA_LAST_MODIFIED_BY =
            createProperty(FEDORA_NS + "lastModifiedBy");
    public static final Property FEDORA_LAST_MODIFIED_DATE =
            createProperty(FEDORA_NS + "lastModified");
    public static final Property FEDORA_CREATED_BY =
            createProperty(FEDORA_NS + "createdBy");
    public static final Property FEDORA_CREATED_DATE =
            createProperty(FEDORA_NS + "created");

    public static final Property HAS_FIXITY_RESULT =
            createProperty(PREMIS_NS + "hasFixity");
    public static final Property HAS_MESSAGE_DIGEST =
            createProperty(PREMIS_NS + "hasMessageDigest");
    public static final Property HAS_SIZE =
            createProperty(PREMIS_NS + "hasSize");

    public static final Property CONTAINS =
            createProperty(LDP_NS + "contains");

    private static final Predicate<Property> hasFedoraNamespace =
            p -> !p.isAnon() && p.getNameSpace().startsWith(FEDORA_NS);

    private static final Predicate<Property> hasMementoNamespace =
            p -> !p.isAnon() && p.getNameSpace().startsWith(MEMENTO_NS);

    private static final Set<Property> fixityProperties = of(
            HAS_FIXITY_RESULT, HAS_MESSAGE_DIGEST);

    private static final Set<Property> binaryProperties = of(
            HAS_SIZE, HAS_ORIGINAL_NAME, EBUCORE_HAS_MIME_TYPE);

    private static final Set<Property> ldpManagedProperties = of(CONTAINS);

    private static final Set<Property> serverManagedProperties;
    static {
        final ImmutableSet.Builder<Property> b = ImmutableSet.builder();
        b.addAll(fixityProperties).addAll(ldpManagedProperties).addAll(binaryProperties);
        serverManagedProperties = b.build();
    }

    public static final Predicate<Property> isManagedPredicate =
            hasFedoraNamespace.or(hasMementoNamespace).or(serverManagedProperties::contains);

}
