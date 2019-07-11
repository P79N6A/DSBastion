package com.bean;

import com.audit.AuditEvent;
import com.audit.AuditManager;
import com.google.common.cache.*;
import com.handler.DSGHandler;
import com.handler.SQLHandler;
import com.util.*;
import com.util.DriverManager;

import java.io.Closeable;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


public class WrapConnect implements Closeable {

    private final AtomicInteger COUNTER = new AtomicInteger(1);

    private final int defaultFetchSize = 1000;

    private final String rAddress;
    private final String dbUser;
    private final String ak;
    private final String platform_id;
    private final String appid;


    private final Connection dbConnect;
    private final String dbType;
    private String schema_catalog;

    final Cache<String, WrapStatement> stmtMap = CacheBuilder.newBuilder()
            .expireAfterAccess(Constants.proxyTimeout, TimeUnit.SECONDS)
            .removalListener((RemovalListener<String, WrapStatement>) notify -> {
                if (notify.getCause() == RemovalCause.EXPIRED) {
                    notify.getValue().close();
                }
            }).build();

    public WrapConnect(String rAddress, String ak) throws Exception {
        this.rAddress = rAddress;
        this.ak = ak;

        Map<String, Object> dsgInfo = DSGHandler.getAppInfo(ak);
        String rClient = String.valueOf(dsgInfo.get("client"));
        String rMac = String.valueOf(dsgInfo.get("mac"));
        String rProcess = String.valueOf(dsgInfo.get("process"));
        if (!rAddress.contains(rClient))
            throw new Exception("address[" + rAddress + "] has no permission to connect by " + ak);

        this.appid = String.valueOf(dsgInfo.get("id"));
        this.platform_id = String.valueOf(dsgInfo.get("dbms_id")).substring(0, 3);
        this.dbType = String.valueOf(dsgInfo.get("server_type")).toLowerCase();
        this.schema_catalog = String.valueOf(dsgInfo.get("default_database"));

        Properties property = new Properties();
        String properties = String.valueOf(dsgInfo.get("properties"));
        if (properties != null && !properties.isEmpty()) {
            String[] props = properties.split("&");
            for (String prop : props) {
                String[] kv = prop.split("=");
                if (kv.length != 2) throw new SQLException("properties format[" + prop + "] error");
                if (!kv[0].equals("user") && !kv[0].equals("password")) property.put(kv[0], kv[1]);
            }
        }
        this.dbUser = String.valueOf(dsgInfo.get("username"));
        property.put("user", this.dbUser);
        property.put("password", String.valueOf(dsgInfo.get("password")));
        String url;
        if ("mysql".equals(dbType)) url = "jdbc" + ":" + dbType + "://" + dsgInfo.get("server_ip")
                + ":" + dsgInfo.get("server_port") + "/" + schema_catalog;
        else if ("oracle".equals(dbType)) {
            url = "jdbc" + ":" + dbType + ":thin:@//" + dsgInfo.get("server_ip")
                    + ":" + dsgInfo.get("server_port") + "/" + schema_catalog;
            schema_catalog = property.getProperty("user");
        } else throw new SQLException("type[" + dbType + "] is not support");
        this.dbConnect = DriverManager.getDriver(this.dbType, String.valueOf(dsgInfo.get("server_version")))
                .connect(url, property);
        AuditManager.getInstance().audit(new AuditEvent(rAddress, ak, "createConnect",
                ak, property.toString()));
    }

    public String getAddress() {
        return rAddress;
    }

    private String generateStmt() {
        return Utils.md5(rAddress + COUNTER.incrementAndGet());
    }

    public String getAK() {
        return ak;
    }

    public String getDbUser() {
        return dbUser;
    }

    String getPlatform_id() {
        return platform_id;
    }

    String getAppid() {
        return appid;
    }

    public String getSchema_catalog() {
        return schema_catalog;
    }

    public String getDbType() {
        return dbType;
    }

    public Connection getDbConnect() {
        return dbConnect;
    }

    public void setCatalog(String catalog) throws SQLException {
        this.dbConnect.setCatalog(catalog);
    }

    public void setSchema(String schema) throws SQLException {
        this.dbConnect.setSchema(schema);
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.dbConnect.setAutoCommit(autoCommit);
    }

    public void commit() throws SQLException {
        this.dbConnect.commit();
    }

    public void rollback() throws SQLException {
        this.dbConnect.rollback();
    }

    public WrapStatement getStatement(String stmtId) {
        return stmtMap.getIfPresent(stmtId);
    }

    public WrapPrepareStatement getPrepareStatement(String stmtId) {
        return (WrapPrepareStatement) stmtMap.getIfPresent(stmtId);
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return this.dbConnect.getMetaData();
    }

    private String createStatement(String user, Statement stmt) throws SQLException {
        try {
            String stmtId = generateStmt();
            WrapStatement wrs = new WrapStatement(this, stmtId, user, stmt);
            wrs.setFetchSize(defaultFetchSize);
            stmtMap.put(stmtId, wrs);
            return stmtId;
        } catch (SQLException e) {
            if (stmt != null) stmt.close();
            throw e;
        }
    }

    public String createStatement(String user) throws SQLException {
        Statement stmt = this.dbConnect.createStatement();
        return createStatement(user, stmt);
    }

    public String createStatement(String user, int resultSetType, int resultSetConcurrency) throws SQLException {
        Statement stmt = this.dbConnect.createStatement(resultSetType, resultSetConcurrency);
        return createStatement(user, stmt);
    }

    public String createStatement(String user, int resultSetType, int resultSetConcurrency,
                                  int resultSetHoldability) throws SQLException {
        Statement stmt = this.dbConnect.createStatement(resultSetType, resultSetConcurrency,
                resultSetHoldability);
        return createStatement(user, stmt);
    }

    private String prepareStatement(String user, String sql, PreparedStatement pstmt) throws Exception {
        try {
            SQLHandler sqlHandler = new SQLHandler(this, user, sql);
            sqlHandler.handler();
            SQLStruct sqlStruct = sqlHandler.getSqlStruct();
            DSGHandler.checkPermission(appid, platform_id, user, sqlStruct);
            Map<Integer, MaskBean> pstmtMask = sqlHandler.getPstmtMask();
            String stmtId = generateStmt();
            WrapPrepareStatement wrs = new WrapPrepareStatement(this, stmtId, user, pstmt,
                    pstmtMask, sqlStruct);
            wrs.setFetchSize(defaultFetchSize);
            stmtMap.put(stmtId, wrs);
            return stmtId;
        } catch (Exception e) {
            if (pstmt != null) pstmt.close();
            throw e;
        }
    }

    public String prepareStatement(String user, String sql) throws Exception {
        PreparedStatement stmt = this.dbConnect.prepareStatement(sql);
        return prepareStatement(user, sql, stmt);
    }

    public String prepareStatement(String user, String sql, int[] columnIndexes) throws Exception {
        PreparedStatement stmt = this.dbConnect.prepareStatement(sql, columnIndexes);
        return prepareStatement(user, sql, stmt);
    }

    public String prepareStatement(String user, String sql, String[] columnNames) throws Exception {
        PreparedStatement stmt = this.dbConnect.prepareStatement(sql, columnNames);
        return prepareStatement(user, sql, stmt);
    }

    public String prepareStatement(String user, String sql, int autoGeneratedKeys) throws Exception {
        PreparedStatement stmt = this.dbConnect.prepareStatement(sql, autoGeneratedKeys);
        return prepareStatement(user, sql, stmt);
    }

    public String prepareStatement(String user, String sql, int resultSetType, int resultSetConcurrency)
            throws Exception {
        PreparedStatement stmt = this.dbConnect.prepareStatement(sql, resultSetType, resultSetConcurrency);
        return prepareStatement(user, sql, stmt);
    }

    public String prepareStatement(String user, String sql, int resultSetType, int resultSetConcurrency,
                                   int resultSetHoldability) throws Exception {
        PreparedStatement stmt = this.dbConnect.prepareStatement(sql, resultSetType, resultSetConcurrency,
                resultSetHoldability);
        return prepareStatement(user, sql, stmt);
    }

    @Override
    public void close() {
        stmtMap.asMap().values().forEach(WrapStatement::close);
        stmtMap.invalidateAll();
        try {
            if (dbConnect != null) dbConnect.close();
        } catch (SQLException ignored) {
        }
    }
}
