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

    public Logger(String outputPath) {
        this.outputPath = outputPath;
        File file = new File(outputPath);
        file.delete();

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::logWriteToFile, 5, 5, TimeUnit.SECONDS);
    }

    public void logSend(int messageId) {
        logBuffer.add("b " + messageId);
        // actually write to file only after some events
        if (logBuffer.size() >= 100000) {
            logWriteToFile();
        }
    }

    public void logDeliver(byte senderId, int messageId) {
        logBuffer.add("d " + ((int) senderId + 1) + " " + messageId);
        // actually write to file only after some events
        if (logBuffer.size() >= 100000) {
            logWriteToFile();
        }
    }

    public void logWriteToFile() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(this.outputPath, true))) {
            while (!logBuffer.isEmpty()) {
                writer.write(logBuffer.poll());
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void logTesting(String string) {
        logBuffer.add(string);
        if (logBuffer.size() >= 100000) {
            logWriteToFile();
        }
    }
}
