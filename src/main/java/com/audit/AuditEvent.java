package com.audit;

import java.util.*;

/**
 * "${eventDate}|${user}|${origin}|${method}[${param}...]";
 */
public class AuditEvent {

    private final String timestamp;
    private final String origin;
    private final String user;
    private final String method;
    private List<String> params;

    public AuditEvent(String origin, String user, String method, Object... params) {
        this.timestamp = AuditManager.dateFormat.format(new Date());
        this.origin = origin;
        this.user = user;
        this.method = method;
        if (params != null) {
            this.params = new ArrayList<>(params.length);
            for (Object param : params) {
                this.params.add(String.valueOf(param));
            }
        }
    }

    String getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder()
                .append(timestamp)
                .append('|')
                .append(user)
                .append('|')
                .append(origin)
                .append('|')
                .append(method)
                .append("==>");
        if (params != null) {
            for (int i = 0; i < params.size(); i++) {
                buff.append(params.get(i));
                if (i != params.size() - 1) buff.append(',');
            }
        }
        return buff.toString();
    }

}
