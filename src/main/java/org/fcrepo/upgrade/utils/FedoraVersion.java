/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree.
 */
package org.fcrepo.upgrade.utils;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * An enum representing supported Fedora versions
 * @author dbernstein
 */
public enum FedoraVersion {
    V_4_7_5("4.7.5"),
    V_5("5+"),
    V_6("6+");

    private String strValue;

    private FedoraVersion(final String strValue) {
        this.strValue = strValue;
    }

    public String getStringValue() {
        return this.strValue;
    }

    public static FedoraVersion fromString(final String strValue) {
        for (FedoraVersion v : FedoraVersion.values()) {
            if (v.strValue.equals(strValue)) {
                return v;
            }
        }

        throw new IllegalArgumentException(
                String.format("%s is not a valid version. Please try one of the following: %s",
                        strValue, Arrays.stream(FedoraVersion.values()).map(x -> x.getStringValue())
                                .collect(Collectors.joining(","))));
    }

}
