package com.audit;

import com.util.Constants;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.concurrent.ArrayBlockingQueue;

final class AuditWriter implements Runnable {

    private final Logger logger = Logger.getLogger(AuditWriter.class);

    private final Path dir;
    private final String lineSeparator;
    private final ArrayBlockingQueue<AuditEvent> queue;

    private String fileName;

    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;

    private boolean isRunning = true;

    AuditWriter() {
        this(Paths.get(Constants.JPPath, "audit"), 1000);
    }

    private AuditWriter(Path dir, int qs) {
        this.dir = dir;
        dir.toFile().mkdirs();
        this.lineSeparator = java.security.AccessController.doPrivileged(
                new sun.security.action.GetPropertyAction("line.separator"));
        this.queue = new ArrayBlockingQueue<>(qs);
    }

    void write(AuditEvent event) {
        try {
            this.queue.put(event);
        } catch (InterruptedException e) {
            logger.error("event==>" + event, e);
        }
    }

    @Override
    public void run() {
        logger.debug("auditWriter start...");
        while (isRunning) {
            AuditEvent event = queue.poll();
            if (event == null) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
                continue;
            }
            if (fileName == null || !event.getTimestamp().startsWith(fileName)) {
                fileName = event.getTimestamp().substring(0, 10);
                close();
            }
            try {
                if (randomAccessFile == null) {
                    randomAccessFile = new RandomAccessFile(dir.resolve(fileName + ".audit").toFile(),
                            "rw");
                }
                fileChannel = randomAccessFile.getChannel();
                byte[] bytes = (event + lineSeparator).getBytes(StandardCharsets.UTF_8);
                try (ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
                     ReadableByteChannel inputChannel = Channels.newChannel(inputStream)) {
                    fileChannel.transferFrom(inputChannel, randomAccessFile.length(), bytes.length);
                }
            } catch (IOException e) {
                logger.error("event==>" + event, e);
            }
        }
    }

    void stop() {
        logger.debug("auditWriter stop...");
        while (queue.size() != 0) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException ignored) {
            }
        }
        isRunning = false;
        close();
    }

    private void close() {
        try {
            if (randomAccessFile != null) {
                randomAccessFile.close();
                fileChannel.close();
            }
        } catch (IOException e) {
            logger.error(e);
        }
        randomAccessFile = null;
        fileChannel = null;
    }
}
