package com.util;

import com.bean.ClazzLoader;
import com.google.common.cache.*;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DriverManager {

    private static Map<String, List<Map<String, Object>>> driver_conf;

    static {
        try (Reader reader = Files.newBufferedReader(Paths.get(Constants.JPPath, "conf",
                "proxyDriver.json"), StandardCharsets.UTF_8)) {
            driver_conf = new Gson().fromJson(reader, new TypeToken<Map<String, List<Map<String, Object>>>>() {
            }.getType());
        } catch (IOException e) {
            throw new RuntimeException("load proxy driver error=>", e);
        }
    }

    private static final Cache<String, Driver> drivers = CacheBuilder.newBuilder().build();

    private DriverManager() {
    }

    @SuppressWarnings("unchecked")
    public static synchronized Driver getDriver(String type, String version) throws SQLException {
        if (driver_conf.containsKey(type)) {
            for (Map<String, Object> map : driver_conf.get(type)) {
                List<String> versions = (List<String>) map.get("version");
                if (versions.contains(version)) {
                    String id = String.valueOf(map.get("id"));
                    Driver driver = drivers.getIfPresent(id);
                    if (driver == null) {
                        String driverClass = String.valueOf(map.get("driverClass"));
                        List<String> driverPath = (List<String>) map.get("driverPath");
                        URL[] urls = new URL[driverPath.size()];
                        Path rp = Paths.get(Constants.JPPath, "ext");
                        try {
                            for (int i = 0; i < driverPath.size(); i++)
                                urls[i] = new URL("jar:file:" + rp.resolve(driverPath.get(i)) + "!/");
                            ClassLoader pcl = new ClazzLoader(urls);
                            driver = (Driver) pcl.loadClass(driverClass).newInstance();
                        } catch (Exception e) {
                            throw new SQLException(e);
                        }
                        drivers.put(id, driver);
                    }
                    return driver;
                }
            }
            throw new SQLException(type + " version[" + version + "] driver undefined");
        } else throw new SQLException("DB[" + type + "," + version + "] driver undefined");
    }

}
