package cs451;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Logger {
    private final ConcurrentLinkedQueue<String> logBuffer = new ConcurrentLinkedQueue<>();
    private String outputPath;
    private static final int BUFFER_SIZE = 1000;
    private BufferedWriter writer;

    public Logger(String outputPath) {
        this.outputPath = outputPath;
        File file = new File(outputPath);

        // ensure the parent directory exists
        File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            if (!parentDir.mkdirs()) {
                System.err.println("Failed to create directory: " + parentDir);
            }
        }

        file.delete();

        try {
            this.writer = new BufferedWriter(new FileWriter(outputPath, true));
        } catch (IOException e) {
            e.printStackTrace();
        }

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::logWriteToFile, 5, 5, TimeUnit.SECONDS);
    }

    public void logSend(int messageId) {
        logBuffer.add("b " + messageId);
        // actually write to file only after some events
        if (logBuffer.size() >= BUFFER_SIZE) {
            logWriteToFile();
        }
    }

    public void logDeliver(byte senderId, int messageId) {
        logBuffer.add("d " + ((int) senderId + 1) + " " + messageId);
        // actually write to file only after some events
        if (logBuffer.size() >= BUFFER_SIZE) {
            logWriteToFile();
        }
    }

    public synchronized void logWriteToFile() {
        try {
            while (!logBuffer.isEmpty()) {
                writer.write(logBuffer.poll() + "\n");
            }
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeWriter() {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
