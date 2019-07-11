package com.util;


import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;

public class Constants {

    public static final String JPPath = System.getProperty("JPPath");
    public static int proxyPort;
    public static int proxyTimeout;

    static {
        try (InputStream is = Files.newInputStream(Paths.get(JPPath, "conf/conf.properties"))) {
            Properties properties = new Properties();
            properties.load(is);
            proxyPort = Integer.parseInt(Objects.requireNonNull(properties.getProperty("proxyPort"),
                    "proxyPort is null"));
            proxyTimeout = Integer.parseInt(Objects.requireNonNull(properties.getProperty("proxyTimeout"),
                    "proxyTimeout is null"));
        } catch (IOException | NullPointerException e) {
            System.err.println("load conf error " + e);
            System.exit(1);
        }
    }
}
