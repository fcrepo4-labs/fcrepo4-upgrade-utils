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

import java.nio.file.Path;

/**
 * @author pwinckles
 */
public class ContainerInfo {

    private final String parentId;
    private final String fullId;
    private final String idLastPart;
    private final Path directory;

    public ContainerInfo(final String parentId, final String fullId, final String idLastPart, final Path directory) {
        this.parentId = parentId;
        this.fullId = fullId;
        this.idLastPart = idLastPart;
        this.directory = directory;
    }

    public String getParentId() {
        return parentId;
    }

    public String getFullId() {
        return fullId;
    }

    public String getIdLastPart() {
        return idLastPart;
    }

    public Path getDirectory() {
        return directory;
    }

    @Override
    public String toString() {
        return "MigrationInfo{" +
                "parentId='" + parentId + '\'' +
                ", fullId='" + fullId + '\'' +
                ", idLastPart='" + idLastPart + '\'' +
                ", directory=" + directory +
                '}';
    }

}
