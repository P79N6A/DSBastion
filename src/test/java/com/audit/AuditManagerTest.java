package com.audit;

import com.util.Constants;
import org.apache.log4j.PropertyConfigurator;
import org.junit.*;

import java.nio.file.Paths;

import static org.junit.Assert.*;

public class AuditManagerTest {

    @Before
    public void setUp() throws Exception {
        PropertyConfigurator.configure(Paths.get(Constants.JPPath,"conf","log4j.properties").toString());
        AuditManager.getInstance().start();
    }

    @After
    public void tearDown() throws Exception {
        AuditManager.getInstance().stop();
    }

    @Test
    public void audit() {
        for (int i = 0; i < 2500; i++) {
            new Event(i).start();
        }
    }

    private class Event extends Thread {

        private int index;

        private Event(int index) {
            this.index = index;
        }

        @Override
        public void run() {
            AuditEvent auditEvent = new AuditEvent("127.0.0.1", "test_" + index,
                    "audit", index);
            AuditManager.getInstance().audit(auditEvent);
        }
    }
}