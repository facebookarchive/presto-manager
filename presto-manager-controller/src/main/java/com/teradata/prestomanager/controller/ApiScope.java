package com.teradata.prestomanager.controller;

public enum ApiScope
{
    CLUSTER, WORKERS, COORDINATOR;

    public static ApiScope fromString(String s)
    {
        if (s == null) {
            return null;
        }
        return valueOf(s.toUpperCase());
    }
}
