package com.util;

import com.bean.ClazzLoader;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Properties;

public class ClazzLoaderTest {

    @Test
    public void createConnect() throws MalformedURLException, ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
        String url = "jdbc:oracle:thin:@//127.0.0.1:1521/ORCLPDB1";
        Properties properties = new Properties();
        properties.put("user", "test");
        properties.put("password", "test");
        URL u1 = new URL("jar:file:" + Paths.get(Constants.JPPath, "ext")
                .resolve("ojdbc6.jar") + "!/");
        URL u2 = new URL("jar:file:" + Paths.get(Constants.JPPath, "ext")
                .resolve("protobuf-java-3.6.1.jar") + "!/");
        ClazzLoader pcl = new ClazzLoader(new URL[]{u1,u2});
        Driver driver = (Driver) pcl.loadClass("oracle.jdbc.OracleDriver").newInstance();
        Connection connect = driver.connect(url, properties);
        try (PreparedStatement ps = connect.prepareStatement("select * from houses")) {
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int cols = rsmd.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= cols; i++) {
                    String schema = rsmd.getSchemaName(i);
                    String catalog = rsmd.getCatalogName(i);
                    String table = rsmd.getTableName(i);
                    System.out.println("schema[" + schema + "]catalog[" + catalog + "]table[" + table + "]");
                    String col = rsmd.getColumnName(i);
                    System.out.println(col + "[" + rs.getObject(i) + "]");
                }
            }
            rs.close();
        }
    }
}