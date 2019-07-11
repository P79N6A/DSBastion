package com.audit;

import java.text.SimpleDateFormat;
import java.util.Locale;


public class AuditManager {

    final static SimpleDateFormat dateFormat = new SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss.SSS", Locale.US);

    private static volatile AuditManager auditManager;

    private AuditWriter writer;

    private AuditManager() {
        writer = new AuditWriter();
    }

    public static AuditManager getInstance() {
        AuditManager result = auditManager;
        if (result == null) {
            synchronized (AuditManager.class) {
                result = auditManager;
                if (result == null) {
                    auditManager = result = new AuditManager();
                }
            }
        }
        return result;
    }

    public void start() {
        new Thread(writer).start();
    }

    public void audit(AuditEvent event) {
        writer.write(event);
    }

    public void stop() {
        writer.stop();
    }
}
