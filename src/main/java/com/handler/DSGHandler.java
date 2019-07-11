package com.handler;

import com.bean.ColStruct;
import com.bean.SQLStruct;
import com.exception.PermissionException;
import com.util.InnerDb;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.*;


public class DSGHandler {

    public static Map<String, Object> getAppInfo(String ak) throws Exception {
        String sql = "select id,client,mac,process,server_ip,server_port,server_type,server_version," +
                "username,password,default_database,dbms_id,properties " +
                "from data_cap_use_certification where ak=?";
        Map<String, Object> map = InnerDb.get(sql, ak);
        String password = String.valueOf(map.remove("password"));
        password = password.substring(2, password.length() - 1).toUpperCase();
        String k = "Encrypt@12345678";
        byte[] pbytes = IOHandler.hexToByte(password);
        Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
        SecretKeySpec keyspec = new SecretKeySpec(k.getBytes(StandardCharsets.UTF_8), "AES");
        IvParameterSpec ivspec = new IvParameterSpec(k.getBytes(StandardCharsets.UTF_8));
        cipher.init(Cipher.DECRYPT_MODE, keyspec, ivspec);
        byte[] original = cipher.doFinal(pbytes);
        password = new String(original, StandardCharsets.UTF_8).trim();
        map.put("password", password);
        return map;
    }

    /**
     * @param app_id      appid
     * @param platform_id platformid
     * @param user        username
     * @param sqlStruct   sqlStruct
     * @throws SQLException e
     */
    public static void checkPermission(String app_id, String platform_id, String user, SQLStruct sqlStruct)
            throws SQLException, PermissionException {
        Set<String> operators = new HashSet<>();
        if (sqlStruct.getFirst() != null) sqlStruct.getFirst().forEach(col -> operators.add(col.getOperator()));
        if (sqlStruct.getOther() != null) sqlStruct.getOther().forEach(col -> operators.add(col.getOperator()));
        if (operators.isEmpty()) return;
        List<Map<String, Object>> list;
        if (!user.isEmpty()) {
            StringBuilder sqlBuilder = new StringBuilder("select action_name,data_database,data_table,data_col");
            sqlBuilder.append(" from data_model where data_type=? and")
                    .append(" data_id=(select platform_unified_id from data_platform_user where")
                    .append(" platform_id=? and username=?) and action_name in(");
            for (String operator : operators) {
                sqlBuilder.append("'").append(operator.toUpperCase()).append("'").append(",");
            }
            String sql = sqlBuilder.toString();
            sql = sql.substring(0, sql.length() - 1) + ")";
            list = InnerDb.query(sql, 1, platform_id, user);
        } else {
            StringBuilder sqlBuilder = new StringBuilder("select action_name,data_database,data_table,data_col");
            sqlBuilder.append(" from data_model where ").append("data_type=? and data_id=? and action_name in(");
            for (String operator : operators) {
                sqlBuilder.append("'").append(operator.toUpperCase()).append("'").append(",");
            }
            String sql = sqlBuilder.toString();
            sql = sql.substring(0, sql.length() - 1) + ")";
            list = InnerDb.query(sql, 2, app_id);
        }
        if (list.isEmpty()) {
            if (!user.isEmpty())
                throw new PermissionException("table[data_model] has not exist user[" + user + "] record");
            else throw new PermissionException("table[data_model] has not exist app[" + app_id + "] record");
        }
        //dtc[db,table,col] mapper
        Map<String, Map<String, List<String>>> dtc = new HashMap<>(list.size());
        for (Map<String, Object> map : list) {
            String operator = String.valueOf(map.get("action_name")).toLowerCase();
            Map<String, List<String>> om = new HashMap<>(3);
            om.put("db", splitValue(map.get("data_database")));
            om.put("tb", splitValue(map.get("data_table")));
            om.put("col", splitValue(map.get("data_col")));
            dtc.put(operator, om);
        }
        checkPermission(sqlStruct.getFirst(), dtc, true, app_id, user);
        checkPermission(sqlStruct.getOther(), dtc, false, app_id, user);
    }


    private static void checkPermission(List<ColStruct> list, Map<String, Map<String, List<String>>> dtc, boolean isFirst,
                                        String app_id, String user) throws PermissionException {
        if (list == null) return;
        for (ColStruct col : list) {
            Map<String, List<String>> map = dtc.get(col.getOperator());
            if (map == null) {
                if (!user.isEmpty()) throw new PermissionException("user[" + user + "] has no permission to[" +
                        col.getOperator() + "]" + col.getSchema_catalog());
                else throw new PermissionException("app[" + app_id + "] has no permission to[" +
                        col.getOperator() + "]" + col.getSchema_catalog());
            }
            List<String> _col = map.get("col");
            List<String> _db = map.get("db");
            List<String> _tb = map.get("tb");
            if (!_db.contains(col.getSchema_catalog())) {
                if (!_tb.contains(col.getTable().toString())) {
                    if (!_col.contains(col.toString())) {
                        if (isFirst && "select".equals(col.getOperator())) col.invisible();
                        else {
                            if (!user.isEmpty())
                                throw new PermissionException("user[" + user + "] has no permission to[" +
                                        col.getOperator() + "]" + col.toString());
                            else throw new PermissionException("app[" + app_id + "] has no permission to[" +
                                    col.getOperator() + "]" + col.toString());
                        }
                    }
                }
            }
        }
    }

    /**
     * @param val &v1&v2&...
     * @return String[]
     */
    private static List<String> splitValue(Object val) {
        if (val == null) return Collections.emptyList();
        String value = String.valueOf(val);
        List<String> list = new ArrayList<>();
        value = value.substring(1, value.length() - 1);
        Collections.addAll(list, value.split("&"));
        return list;
    }

    /**
     * @param ak             connect ak
     * @param user           userName or empty
     * @param schema_catalog schema or catalog
     * @param tableName      table name
     * @return list
     * @throws SQLException e
     */
    static List<Map<String, Object>> filterRow(String ak, String user, String schema_catalog,
                                               String tableName) throws SQLException {
        String sql;
        List<Map<String, Object>> list;
        if (user.isEmpty()) {
            sql = "select colname,action,filterval,valtype from " +
                    "data_rowfilter_temp where ak=? and dbname=? and tablename=?";
            list = InnerDb.query(sql, ak, schema_catalog, tableName);
        } else {
            sql = "select colname,action,filterval,valtype from data_rowfilter_temp " +
                    "where ak=? and username=? and dbname=? and tablename=?";
            list = InnerDb.query(sql, ak, user, schema_catalog, tableName);
        }
        return list;
    }
}
