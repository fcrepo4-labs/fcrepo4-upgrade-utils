/**
 * Copyright 2015 DuraSpace, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import static org.slf4j.LoggerFactory.getLogger;

import java.util.Iterator;

import javax.inject.Inject;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import com.google.common.annotations.VisibleForTesting;

import org.fcrepo.http.commons.session.SessionFactory;
import org.fcrepo.kernel.api.models.Container;
import org.fcrepo.kernel.api.models.FedoraBinary;
import org.fcrepo.kernel.api.models.FedoraResource;
import org.fcrepo.kernel.api.services.NodeService;

import org.slf4j.Logger;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;

/**
 * Utility to migrate file technical metadata in existing repositories to comply
 * with changes made in May 2015:
 *   fedora:digest => premis:hasMessageDigest
 *   jcr:mimeType => ebucore:hasMimeType
 *   premis:hasOriginalName => ebucore:filename
 *
 * @author escowles
 * @since 2015-05-21
**/
public class TechnicalMetadataMigrator {

    private final Logger logger = getLogger(TechnicalMetadataMigrator.class);
    private boolean dryrun = false;
    private Session session;

    @Inject
    private SessionFactory sessionFactory;

    @Inject
    private NodeService nodeService;

    @VisibleForTesting
    protected TechnicalMetadataMigrator(final SessionFactory sessionFactory, final NodeService nodeService) {
        this.sessionFactory = sessionFactory;
        this.nodeService = nodeService;
    }

    /**
     * Migrate technical metadata.
     * @param args If "dryrun" is passed as an argument, the utility will print out what would be done,
     *             but no changes will be made.
    **/
    public static void main(final String[] args) {
        ConfigurableApplicationContext ctx = null;
        try {
            final boolean dryrun;
            if (args.length > 0 && "dryrun".equals(args[0])) {
                dryrun = true;
            } else {
                dryrun = false;
            }

            final TechnicalMetadataMigrator migrator = new TechnicalMetadataMigrator();
            ctx = new ClassPathXmlApplicationContext("classpath:/spring/master.xml");
            ctx.getBeanFactory().autowireBeanProperties(migrator, AutowireCapableBeanFactory.AUTOWIRE_BY_TYPE, false);
            migrator.run(dryrun);

        } catch (RepositoryException ex) {
            ex.printStackTrace();
        } finally {
            if (null != ctx) {
                ctx.close();
            }
        }
    }

    /**
     * No-argument constructor.
    **/
    public TechnicalMetadataMigrator() {
    }

    /**
     * Migrate technical metadata properties.
     * @param dryrun If true, do not modify repository, only show what would have been done.
    **/
    public void run(final boolean dryrun) throws RepositoryException {
        this.dryrun = dryrun;
        session = sessionFactory.getInternalSession();

        // register ebucore namespace
        if (!dryrun) {
            session.setNamespacePrefix("ebucore", "http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#");
        }

        processResource(nodeService.find(session, "/"));
    }

    private void processResource(final FedoraResource resource) throws RepositoryException {
        logger.debug("processResource() {}: " + resource.getPath(), resource.getClass());
        if (resource instanceof Container) {
            logger.debug("Found container: {}", resource);
            for (final Iterator<FedoraResource> children = resource.getChildren(); children.hasNext(); ) {
                processResource(children.next());
            }
        } else if (resource instanceof FedoraBinary) {
            logger.debug("Found binary: {}", resource);
            processBinary((FedoraBinary)resource);
        } else {
            logger.debug("Neither container nor binary!");
            for (final Iterator<FedoraResource> children = resource.getChildren(); children.hasNext(); ) {
                processResource(children.next());
            }
        }
    }

    private void processBinary(final FedoraBinary binary) throws RepositoryException {
        migrate(binary, "fedora:digest", "premis:hasMessageDigest");
        migrate(binary, "jcr:mimeType", "ebucore:hasMimeType");
        migrate(binary, "premis:hasOriginalName", "ebucore:filename");

        if (!dryrun) {
            logger.info(binary.getPath());
            session.save();
        } else {
            logger.info("dryrun {}", binary.getPath());
            session.refresh(false);
        }
    }

    private void migrate(final FedoraBinary binary, final String fromProp, final String toProp)
            throws RepositoryException {
        if (binary.hasProperty(fromProp)) {
            final Property p = binary.getProperty(fromProp);
            logger.debug("  {} => {}: {}", fromProp, toProp, p.getString());
            if (!dryrun) {
                binary.getNode().setProperty(toProp, p.getValue());
                p.remove();
            }
        }
    }
}
