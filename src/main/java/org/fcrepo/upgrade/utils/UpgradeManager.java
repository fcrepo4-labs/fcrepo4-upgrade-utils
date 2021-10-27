/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */
package org.fcrepo.upgrade.utils;

/**
 * An interface to be implemented by concrete upgrade paths.
 */
public interface UpgradeManager {

    /**
     * Starts the upgrade manager.
     */
     void start();
}
