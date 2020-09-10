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
public class ResourceInfo {

    public enum Type {
        BINARY,
        EXTERNAL_BINARY,
        CONTAINER,
    }


    private final String parentId;
    private final String fullId;
    private final String nameEncoded;
    private final Path outerDirectory;
    private final Path innerDirectory;
    private final Type type;

    public static ResourceInfo container(final String parentId,
                                         final String fullId,
                                         final Path outerDirectory,
                                         final String nameEncoded) {
        return new ResourceInfo(parentId, fullId, outerDirectory, nameEncoded, Type.CONTAINER);
    }

    public static ResourceInfo binary(final String parentId,
                                      final String fullId,
                                      final Path outerDirectory,
                                      final String nameEncoded) {
        return new ResourceInfo(parentId, fullId, outerDirectory, nameEncoded, Type.BINARY);
    }

    public static ResourceInfo externalBinary(final String parentId,
                                              final String fullId,
                                              final Path outerDirectory,
                                              final String nameEncoded) {
        return new ResourceInfo(parentId, fullId, outerDirectory, nameEncoded, Type.EXTERNAL_BINARY);
    }

    private ResourceInfo(final String parentId,
                         final String fullId,
                         final Path outerDirectory,
                         final String nameEncoded,
                         final Type type) {
        this.parentId = parentId;
        this.fullId = fullId;
        this.nameEncoded = nameEncoded;
        this.outerDirectory = outerDirectory;
        this.innerDirectory = outerDirectory.resolve(nameEncoded);
        this.type = type;
    }

    public String getParentId() {
        return parentId;
    }

    public String getFullId() {
        return fullId;
    }

    public Path getOuterDirectory() {
        return outerDirectory;
    }

    public Path getInnerDirectory() {
        return innerDirectory;
    }

    public String getNameEncoded() {
        return nameEncoded;
    }

    public Type getType() {
        return type;
    }

    @Override
    public String toString() {
        return "ResourceInfo{" +
                "parentId='" + parentId + '\'' +
                ", fullId='" + fullId + '\'' +
                ", nameEncoded='" + nameEncoded + '\'' +
                ", outerDirectory=" + outerDirectory +
                ", innerDirectory=" + innerDirectory +
                ", type=" + type +
                '}';
    }

}
