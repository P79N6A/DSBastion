package com.exception;

public class NotSupportException extends Exception {

    public NotSupportException(String sql, String prefix, Class clazz) {
        this("sql[" + sql + "] " + prefix, clazz);
    }

    public NotSupportException(String prefix, Class clazz) {
        this(prefix + "[" + clazz + "] not support");
    }

    public NotSupportException(String sql, String msg) {
        this(sql + "[" + msg + "] not support");
    }
    public NotSupportException(String message) {
        super(message);
    }
}
