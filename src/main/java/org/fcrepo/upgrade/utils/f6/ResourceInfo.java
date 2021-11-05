/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */

package org.fcrepo.upgrade.utils.f6;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Encapsulates all of the information necessary to migrate a resource.
 *
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

    /**
     * Used by Jackson for deserialization
     *
     * @return resource info object
     */
    @JsonCreator
    public static ResourceInfo deserialize(@JsonProperty("parentId") final String parentId,
                                           @JsonProperty("fullId") final String fullId,
                                           @JsonProperty("nameEncoded") final String nameEncoded,
                                           @JsonProperty("outerDirectory") final Path outerDirectory,
                                           @JsonProperty("innerDirectory") final Path innerDirectory,
                                           @JsonProperty("type") final Type type) {
        return new ResourceInfo(parentId, fullId, outerDirectory, innerDirectory, nameEncoded, type);
    }

    /**
     * Create a ResourceInfo instance for a container resource
     *
     * @param parentId the internal Fedora id of the resource's parent
     * @param fullId the internal Fedora id of the resource
     * @param outerDirectory the export directory that contains the resource
     * @param nameEncoded the final segment of the fullId, percent encoded
     * @return resource info
     */
    public static ResourceInfo container(final String parentId,
                                         final String fullId,
                                         final Path outerDirectory,
                                         final String nameEncoded) {
        return new ResourceInfo(parentId, fullId, outerDirectory, nameEncoded, Type.CONTAINER);
    }

    /**
     * Create a ResourceInfo instance for a binary resource
     *
     * @param parentId the internal Fedora id of the resource's parent
     * @param fullId the internal Fedora id of the resource
     * @param outerDirectory the export directory that contains the resource
     * @param nameEncoded the final segment of the fullId, percent encoded
     * @return resource info
     */
    public static ResourceInfo binary(final String parentId,
                                      final String fullId,
                                      final Path outerDirectory,
                                      final String nameEncoded) {
        return new ResourceInfo(parentId, fullId, outerDirectory, nameEncoded, Type.BINARY);
    }

    /**
     * Create a ResourceInfo instance for an external binary resource
     *
     * @param parentId the internal Fedora id of the resource's parent
     * @param fullId the internal Fedora id of the resource
     * @param outerDirectory the export directory that contains the resource
     * @param nameEncoded the final segment of the fullId, percent encoded
     * @return resource info
     */
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

    private ResourceInfo(final String parentId,
                         final String fullId,
                         final Path outerDirectory,
                         final Path innerDirectory,
                         final String nameEncoded,
                         final Type type) {
        this.parentId = parentId;
        this.fullId = fullId;
        this.nameEncoded = nameEncoded;
        this.outerDirectory = outerDirectory;
        this.innerDirectory = outerDirectory.resolve(nameEncoded);
        this.type = type;
    }

    /**
     * @return the internal Fedora id of the resource's parent
     */
    public String getParentId() {
        return parentId;
    }

    /**
     * @return the internal Fedora id of the resource
     */
    public String getFullId() {
        return fullId;
    }

    /**
     * @return the export directory that contains the resource
     */
    public Path getOuterDirectory() {
        return outerDirectory;
    }

    /**
     * @return the export directory that contains the contents of the resource
     */
    public Path getInnerDirectory() {
        return innerDirectory;
    }

    /**
     * @return the final segment of the fullId, percent encoded
     */
    public String getNameEncoded() {
        return nameEncoded;
    }

    /**
     * @return the type of the resource
     */
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourceInfo that = (ResourceInfo) o;
        return Objects.equals(parentId, that.parentId)
                && Objects.equals(fullId, that.fullId)
                && Objects.equals(nameEncoded, that.nameEncoded)
                && Objects.equals(outerDirectory.toAbsolutePath(), that.outerDirectory.toAbsolutePath())
                && Objects.equals(innerDirectory.toAbsolutePath(), that.innerDirectory.toAbsolutePath())
                && type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(parentId, fullId, nameEncoded,
                outerDirectory.toAbsolutePath(), innerDirectory.toAbsolutePath(), type);
    }
}
