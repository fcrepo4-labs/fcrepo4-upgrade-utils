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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.Iterator;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;

import org.fcrepo.http.commons.session.SessionFactory;
import org.fcrepo.kernel.api.models.Container;
import org.fcrepo.kernel.api.models.FedoraBinary;
import org.fcrepo.kernel.api.models.FedoraResource;
import org.fcrepo.kernel.api.services.NodeService;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

/**
 * TechnicalMetadataMigrator test
 * @author escowles
 * @sinced 2015-05-21
**/
public class TechnicalMetadataMigratorTest {

    @Mock
    SessionFactory mockSessionFactory;

    @Mock
    Session mockSession;

    @Mock
    NodeService mockService;

    @Mock
    Container mockRoot;

    @Mock
    Container mockChild;

    @Mock
    FedoraBinary mockBinary;

    @Mock
    Node mockNode;

    @Mock
    Iterator<FedoraResource> mockChildren1;

    @Mock
    Iterator<FedoraResource> mockChildren2;

    @Mock
    Property sha1Prop;

    @Mock
    Property mimeProp;

    @Mock
    Property nameProp;

    @Mock
    Value sha1Val;

    @Mock
    Value mimeVal;

    @Mock
    Value nameVal;

    private TechnicalMetadataMigrator migrator;

    @Before
    public void setup() throws RepositoryException {
        initMocks(this);

        when(mockService.find(any(Session.class), anyString())).thenReturn(mockRoot);
        when(mockRoot.getChildren()).thenReturn(mockChildren1);
        when(mockRoot.getPath()).thenReturn("/");
        when(mockChild.getChildren()).thenReturn(mockChildren2);
        when(mockChild.getPath()).thenReturn("/child");
        when(mockChildren1.hasNext()).thenReturn(true, false);
        when(mockChildren1.next()).thenReturn(mockChild);
        when(mockChildren2.hasNext()).thenReturn(true, false);
        when(mockChildren2.next()).thenReturn(mockBinary);
        when(mockBinary.getNode()).thenReturn(mockNode);
        when(mockBinary.getPath()).thenReturn("/child/binary");

        final String sha1 = "urn:sha1:f1d2d2f924e986ac86fdf7b36c94bcdf32beec15";
        final String mime = "text/plain";
        final String name = "test.txt";
        when(sha1Prop.getString()).thenReturn(sha1);
        when(mimeProp.getString()).thenReturn(mime);
        when(nameProp.getString()).thenReturn(name);
        when(sha1Prop.getValue()).thenReturn(sha1Val);
        when(mimeProp.getValue()).thenReturn(mimeVal);
        when(nameProp.getValue()).thenReturn(nameVal);

        when(mockBinary.hasProperty(eq("fedora:digest"))).thenReturn(true);
        when(mockBinary.hasProperty(eq("jcr:mimeType"))).thenReturn(true);
        when(mockBinary.hasProperty(eq("premis:hasOriginalName"))).thenReturn(true);
        when(mockBinary.getProperty(eq("fedora:digest"))).thenReturn(sha1Prop);
        when(mockBinary.getProperty(eq("jcr:mimeType"))).thenReturn(mimeProp);
        when(mockBinary.getProperty(eq("premis:hasOriginalName"))).thenReturn(nameProp);

        when(mockSessionFactory.getInternalSession()).thenReturn(mockSession);

        migrator = new TechnicalMetadataMigrator(mockSessionFactory, mockService);
    }

    @Test
    public void testMigrator() throws RepositoryException {
        migrator.run(false);

        verify(mockNode).setProperty("premis:hasMessageDigest", sha1Val);
        verify(mockNode).setProperty("ebucore:hasMimeType", mimeVal);
        verify(mockNode).setProperty("ebucore:filename", nameVal);
        verify(mockSession).save();
    }

    @Test
    public void testMigratorDryRun() throws RepositoryException {
        migrator.run(true);

        verify(mockNode, never()).setProperty("premis:hasMessageDigest", sha1Val);
        verify(mockNode, never()).setProperty("ebucore:hasMimeType", mimeVal);
        verify(mockNode, never()).setProperty("ebucore:filename", nameVal);
        verify(mockSession, never()).save();
    }
}
