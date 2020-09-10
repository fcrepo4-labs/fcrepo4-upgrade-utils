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

import java.io.File;

/**
 * A class representing the configuration of the upgrade run.
 *
 * @author dbernstein
 */
public class Config {

    private FedoraVersion sourceVersion;
    private FedoraVersion targetVersion;
    private File inputDir;
    private File outputDir;

    // F4/5 -> Options
    private String baseUri;
    private Integer threads;
    private String digestAlgorithm;
    private String fedoraUser;
    private String fedoraUserAddress;

    /**
     * Set the version of the source to be transformed.
     *
     * @param sourceVersion The source version
     */
    public void setSourceVersion(final FedoraVersion sourceVersion) {
        this.sourceVersion = sourceVersion;
    }

    /**
     * The version of the source to be transformed.
     *
     * @return The source version
     */
    public FedoraVersion getSourceVersion() {
        return sourceVersion;
    }

    /**
     * The version of Fedora into which you are transforming the source.
     *
     * @return The target version
     */
    public FedoraVersion getTargetVersion() {
        return targetVersion;
    }

    /**
     * Set the version of Fedora into which you are transforming the source.
     *
     * @param targetVersion The target version
     */
    public void setTargetVersion(final FedoraVersion targetVersion) {
        this.targetVersion = targetVersion;
    }

    /**
     * The output directory
     * @return a directory
     */
    public File getOutputDir() {
        return outputDir;
    }

    /**
     * Set the output directory
     * @param outputDir a directory
     */
    public void setOutputDir(final File outputDir) {
        this.outputDir = outputDir;
    }

    /**
     * The input directory
     * @return a directory
     */
    public File getInputDir() {
        return inputDir;
    }

    /**
     * Set the input directory
     * @param inputDir a directory
     */
    public void setInputDir(final File inputDir) {
        this.inputDir = inputDir;
    }

    /**
     * @return the base uri of the existing Fedora, eg http://localhost:8080/rest
     */
    public String getBaseUri() {
        return baseUri;
    }

    /**
     * Sets the baseUri
     * @param baseUri the base uri of the existing Fedora, eg http://localhost:8080/rest
     */
    public void setBaseUri(final String baseUri) {
        this.baseUri = baseUri;
    }

    /**
     * @return the number of threads to use
     */
    public Integer getThreads() {
        return threads;
    }

    /**
     * Sets the number of threads to use
     * @param threads number of threads
     */
    public void setThreads(final Integer threads) {
        this.threads = threads;
    }

    /**
     * @return the digest algorithm to use in OCFL, sha512 or sha256
     */
    public String getDigestAlgorithm() {
        return digestAlgorithm;
    }

    /**
     * Sets the digest algorithm
     * @param digestAlgorithm sha512 or sha256
     */
    public void setDigestAlgorithm(final String digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
    }

    /**
     * @return the user to attribute OCFL versions to
     */
    public String getFedoraUser() {
        return fedoraUser;
    }

    /**
     * Sets the user to attribute OCFL versions to
     * @param fedoraUser user name
     */
    public void setFedoraUser(final String fedoraUser) {
        this.fedoraUser = fedoraUser;
    }

    /**
     * @return the address of the user OCFL versions are attributed to
     */
    public String getFedoraUserAddress() {
        return fedoraUserAddress;
    }

    /**
     * Sets the address of the user OCFL versions are attributed to
     * @param fedoraUserAddress the address of the user OCFL versions are attributed to
     */
    public void setFedoraUserAddress(final String fedoraUserAddress) {
        this.fedoraUserAddress = fedoraUserAddress;
    }

    @Override
    public String toString() {
        return "Config{" +
                "sourceVersion=" + sourceVersion +
                ", targetVersion=" + targetVersion +
                ", inputDir=" + inputDir +
                ", outputDir=" + outputDir +
                ", baseUri=" + baseUri +
                ", threads=" + threads +
                ", digestAlgorithm='" + digestAlgorithm + '\'' +
                ", fedoraUser='" + fedoraUser + '\'' +
                ", fedoraUserAddress='" + fedoraUserAddress + '\'' +
                '}';
    }

}
