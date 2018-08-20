/*
     Copyright © 2018 - 2018 Chris Egerton <fearthecellos@gmail.com>
     This work is free. You can redistribute it and/or modify it under the
     terms of the Do What The Fuck You Want To Public License, Version 2,
     as published by Sam Hocevar. See the LICENSE file for more details.
*/

package com.github.c0urante.kafka.connect.reddit.version;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class Version {
    private static final Logger log = LoggerFactory.getLogger(Version.class);

    private static final String PATH = "/kafka-connect-reddit-version.properties";
    private static final String VERSION = findVersion();

    public static final String UNKNOWN_VERSION = "unknown";


    private static String findVersion() {
        try (InputStream stream = Version.class.getResourceAsStream(PATH)) {
            Properties props = new Properties();
            props.load(stream);
            return props.getProperty("version", UNKNOWN_VERSION).trim();
        } catch (Exception e) {
            log.warn("Error while loading version:", e);
            return UNKNOWN_VERSION;
        }
    }

    public static String get() {
        return VERSION;
    }
}
