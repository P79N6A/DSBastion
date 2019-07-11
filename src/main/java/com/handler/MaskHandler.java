package com.handler;

import com.bean.*;
import com.util.InnerDb;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.*;

public class MaskHandler {

    static void fillMask(String ak, String user, SQLStruct sqlStruct) throws SQLException {
        List<ColStruct> first = sqlStruct.getFirst();
        if (first == null || first.isEmpty()) return;
        String sql;
        Map<String, Map<String, MaskBean>> alreadyQuery = new HashMap<>(1);
        if (!user.isEmpty()) sql = "select m.colname,r.name,r.type,r.srctype,m.param " +
                "from data_mask_temp m, data_maskrule_temp r " +
                "where ak=? and username=? and dbname=? and tablename=? and m.ruleid = r.id";
        else sql = "select m.colname,r.name,r.type,r.srctype,m.param " +
                "from data_mask_temp m, data_maskrule_temp r " +
                "where ak=? and dbname=? and tablename=? and m.ruleid = r.id";
        for (ColStruct colStruct : first) {
            String k = colStruct.getTable().toString();
            if (!alreadyQuery.containsKey(k)) {
                List<Map<String, Object>> list;
                if (!user.isEmpty()) list = InnerDb.query(sql, ak, user, colStruct.getSchema_catalog(),
                        colStruct.getT_name());
                else list = InnerDb.query(sql, ak, colStruct.getSchema_catalog(), colStruct.getT_name());
                Map<String, MaskBean> map_temp = new HashMap<>(list.size());
                for (Map<String, Object> map : list) {
                    String col = String.valueOf(map.get("colname"));
                    MaskBean maskBean = new MaskBean((String) map.get("name"), (int) map.get("type"),
                            (String) map.get("srctype"), (String) map.get("param"));
                    map_temp.put(col, maskBean);
                }
                if (!map_temp.isEmpty()) alreadyQuery.put(k, map_temp);
            }
        }
        if (alreadyQuery.isEmpty()) return;
        for (ColStruct colStruct : first) {
            String k = colStruct.getTable().toString();
            if (alreadyQuery.containsKey(k)) {
                Map<String, MaskBean> map_temp = alreadyQuery.get(k);
                String colName = colStruct.getName();
                if (map_temp.containsKey(colName)) {
                    colStruct.setMaskBean(map_temp.get(colName));
                } else {
                    if ("oracle".equalsIgnoreCase(sqlStruct.getDbType())
                            && map_temp.containsKey(colName.toUpperCase())) {
                        colStruct.setMaskBean(map_temp.get(colName.toUpperCase()));
                    }
                }
            }
        }
    }

    public static byte[] maskValue(byte[] bytes, MaskBean maskBean) {
        if (bytes == null || bytes.length == 0) return bytes;
        if (maskBean == null) return bytes;
        int type = maskBean.getType();
        if (type != 1) return bytes;
        String name = maskBean.getName();
        String valueType = maskBean.getValueType();
        if (valueType.equalsIgnoreCase("STRING")) {
            return maskStr(bytes, name);
        } else if (valueType.equalsIgnoreCase("TINYINT")
                || valueType.equalsIgnoreCase("SMALLINT")
                || valueType.equalsIgnoreCase("INT")
                || valueType.equalsIgnoreCase("BIGINT")
                || valueType.equalsIgnoreCase("FLOAT")
                || valueType.equalsIgnoreCase("DOUBLE")
                || valueType.equalsIgnoreCase("DECIMAL")) {
            return maskNum(bytes, name);
        } else if (valueType.equalsIgnoreCase("DATE")) {
            return maskDate(bytes, name);
        } else if (valueType.equalsIgnoreCase("TIMESTAMP")) {
            return maskTimeStamp(bytes, name);
        } else if (valueType.equalsIgnoreCase("BOOLEAN")) {
            return maskBool(bytes, name);
        } else if (valueType.equalsIgnoreCase("USERUDF")) {
            return userUDF(bytes, name);
        }
        return bytes;
    }

    private static byte[] maskStr(byte[] bytes, String name) {
        String value = new String(bytes, StandardCharsets.UTF_8);
        StringBuilder sb = new StringBuilder();
        if ("STRING_X".equalsIgnoreCase(name)) {
            for (int i = 0; i < value.length(); i++) sb.append("*");
        } else if ("STRING_ZJHM".equalsIgnoreCase(name)) {
            sb.append(value, 0, 4).append("**************");
        } else if ("STRING_NAME".equalsIgnoreCase(name)) {
            if (value.length() < 3) sb.append(value, 0, 1).append("*");
            else {
                sb.append(value, 0, 1);
                for (int i = 0; i < value.length() - 2; i++) sb.append("*");
                sb.substring(value.length() - 1);
            }
        } else if ("STRING_MAIL".equalsIgnoreCase(name)) {
            int atPosition = value.indexOf("@");
            if (atPosition == -1) return bytes;
            for (int i = 0; i < atPosition; i++) sb.append("*");
            sb.append(value.substring(atPosition));
        } else if ("STRING_FULLMAIL".equalsIgnoreCase(name)) {
            int atPosition = value.indexOf("@");
            if (atPosition == -1) return bytes;
            sb.append(value, 0, 1);
            for (int i = 0; i < atPosition - 1; i++) sb.append("*");
            sb.append(value.substring(atPosition));
        } else if ("STRING_MOBILE".equalsIgnoreCase(name)) {
            sb.append(value, 0, 3);
            for (int i = 0; i < value.length() - 5; i++) sb.append("*");
            sb.append(value.substring(value.length() - 2));
        } else if ("STRING_8X".equalsIgnoreCase(name)) {
            sb.append("********");
        } else if ("STRING_HEAD_TAIL".equalsIgnoreCase(name)) {
            sb.append("*").append(value, 1, value.length() - 1).append("*");
        } else if ("STRING_HEAD".equalsIgnoreCase(name)) {
            sb.append("*").append(value.substring(1));
        } else if ("STRING_TAIL".equalsIgnoreCase(name)) {
            sb.append(value, 0, value.length() - 1).append("*");
        } else if ("STRING_MIDDLE".equalsIgnoreCase(name)) {
            sb.append(value, 0, 1);
            for (int i = 0; i < value.length() - 2; i++) sb.append("*");
            sb.append(value.substring(value.length() - 1));
        } else if ("STRING_BANK".equalsIgnoreCase(name)) {
            for (int i = 0; i < value.length() - 4; i++) sb.append("*");
            sb.append(value.substring(value.length() - 4));
        } else if ("STRING_DATEYEAR".equalsIgnoreCase(name)) {
            sb.append(value, 0, 4);
        } else return bytes;
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] maskNum(byte[] bytes, String name) {
        if ("TINYINT_0".equalsIgnoreCase(name) || "SMALLINT_0".equalsIgnoreCase(name)
                || "INT_0".equalsIgnoreCase(name) || "BIGINT_0".equalsIgnoreCase(name)
                || "FLOAT_0".equalsIgnoreCase(name) || "DOUBLE_0".equalsIgnoreCase(name)
                || "DECIMAL_0".equalsIgnoreCase(name)) {
            return "0".getBytes(StandardCharsets.UTF_8);
        } else if ("TINYINT_RANDOM".equalsIgnoreCase(name) || "SMALLINT_RANDOM".equalsIgnoreCase(name)
                || "INT_RANDOM".equalsIgnoreCase(name) || "BIGINT_RANDOM".equalsIgnoreCase(name)
                || "FLOAT_RANDOM".equalsIgnoreCase(name) || "DOUBLE_RANDOM".equalsIgnoreCase(name)
                || "DECIMAL_RANDOM".equalsIgnoreCase(name)) {
            return String.valueOf(new Random().nextInt(10)).getBytes(StandardCharsets.UTF_8);
        }
        return bytes;
    }

    private static byte[] maskDate(byte[] bytes, String name) {
        // TODO Auto-generated method stub
        return bytes;
    }

    private static byte[] maskTimeStamp(byte[] bytes, String name) {
        // TODO Auto-generated method stub
        return bytes;
    }

    private static byte[] maskBool(byte[] bytes, String name) {
        if ("BOOLEAN_FALSE".equalsIgnoreCase(name)) {
            return "false".getBytes(StandardCharsets.UTF_8);
        } else if ("BOOLEAN_RANDOM".equalsIgnoreCase(name)) {
            return (new Random().nextInt(2) == 1 ? "true" : "false").getBytes(StandardCharsets.UTF_8);
        }
        return bytes;
    }

    private static byte[] userUDF(byte[] bytes, String name) {
        // TODO Auto-generated method stub
        return bytes;
    }


}
