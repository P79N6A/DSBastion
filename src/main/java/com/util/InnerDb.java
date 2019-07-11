package com.util;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

public class InnerDb {

    private static BasicDataSource dataSource;

    private InnerDb() {

    }

    static {
        try (InputStream in = Files.newInputStream(Paths.get(Constants.JPPath, "conf", "innerdb.properties"))) {
            Properties properties = new Properties();
            properties.load(in);
            String host = Objects.requireNonNull(properties.getProperty("host"), "inner db host is null");
            String port = Objects.requireNonNull(properties.getProperty("port"), "inner db port is null");
            String dbName = Objects.requireNonNull(properties.getProperty("dbname"), "inner db dbName is null");
            String driver = Objects.requireNonNull(properties.getProperty("driver"), "inner db driver is null");
            String user = Objects.requireNonNull(properties.getProperty("user"), "inner db user is null");
            String pwd = Objects.requireNonNull(properties.getProperty("pwd"), "inner db pwd is null");

            String url = "jdbc:mysql://" + host + ":" + port +
                    "/" + dbName + "?useUnicode=true&characterEncoding=utf-8";
            dataSource = new BasicDataSource();
            dataSource.setDriverClassName(driver);
            dataSource.setUrl(url);
            dataSource.setInitialSize(1);
            dataSource.setMaxTotal(6);
            dataSource.setMaxIdle(6);
            dataSource.setMinIdle(1);
            dataSource.setUsername(user);
            dataSource.setPassword(pwd);
        } catch (IOException | NullPointerException e) {
            System.err.println("init inner db error " + e);
            System.exit(1);
        }
    }

    public static void close() {
        try {
            if (dataSource != null) dataSource.close();
        } catch (SQLException ignored) {
        }
    }

    public static Connection getConnection() throws SQLException {
        if (dataSource == null) throw new SQLException("dataSource is null ");
        return dataSource.getConnection();
    }

    public static Map<String, Object> get(String sql, Object... params) throws SQLException {
        QueryRunner runner = new QueryRunner(dataSource);
        ResultSetHandler<Map<String, Object>> h = new MapHandler();
        Map<String, Object> map = runner.query(sql, h, params);
        if (map == null) return Collections.emptyMap();
        else return map;
    }

    public static List<Map<String, Object>> query(String sql, Object... params) throws SQLException {
        QueryRunner runner = new QueryRunner(dataSource);
        ResultSetHandler<List<Map<String, Object>>> h = new MapListHandler();
        return runner.query(sql, h, params);
    }

    public static Object[] insert(String sql, Object... params) throws SQLException {
        QueryRunner runner = new QueryRunner(dataSource);
        ResultSetHandler<Object[]> h = new ArrayHandler();
        return runner.insert(sql, h, params);
    }

    public static List<Object[]> insertBatch(String sql, Object[][] params) throws SQLException {
        QueryRunner runner = new QueryRunner(dataSource);
        ResultSetHandler<List<Object[]>> h = new ArrayListHandler();
        return runner.insertBatch(sql, h, params);
    }

    public static Object[] insert(Connection conn, String sql, Object... params) throws SQLException {
        QueryRunner runner = new QueryRunner();
        ResultSetHandler<Object[]> h = new ArrayHandler();
        return runner.insert(conn, sql, h, params);
    }

    public static List<Object[]> insertBatch(Connection conn, String sql, Object[][] params) throws SQLException {
        QueryRunner runner = new QueryRunner();
        ResultSetHandler<List<Object[]>> h = new ArrayListHandler();
        return runner.insertBatch(conn, sql, h, params);
    }

    public static int update(String sql, Object... params) throws SQLException {
        QueryRunner runner = new QueryRunner(dataSource);
        return runner.update(sql, params);
    }

    public static int update(Connection conn, String sql, Object... params) throws SQLException {
        QueryRunner runner = new QueryRunner();
        return runner.update(conn, sql, params);
    }
}
