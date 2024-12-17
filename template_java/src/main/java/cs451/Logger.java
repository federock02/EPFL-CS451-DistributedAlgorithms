package cs451;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Logger {
    private final ConcurrentLinkedQueue<String> logBuffer = new ConcurrentLinkedQueue<>();
    private static final int BUFFER_SIZE = 5000;
    private BufferedWriter writer;

    public Logger(String outputPath) {
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
    }

    public void logDecide(Set<Integer> values) {
        if (values == null || values.isEmpty()) {
            return;
        }

        // join the integers with a space separator
        String logEntry = String.join(" ", values.stream().map(String::valueOf).toArray(String[]::new));

        // Add the log entry to the buffer
        logBuffer.add(logEntry);

        // Write to file if the buffer reaches its limit
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
