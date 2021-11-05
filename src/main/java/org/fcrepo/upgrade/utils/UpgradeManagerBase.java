/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */
package org.fcrepo.upgrade.utils;

/**
 * A base class for sharing code between the UpgradeManager implementations.
 * @author dbernstein
 */
public abstract class UpgradeManagerBase implements UpgradeManager {

    protected final Config config;

    protected UpgradeManagerBase(final Config config) {
        this.config = config;
    }
}
