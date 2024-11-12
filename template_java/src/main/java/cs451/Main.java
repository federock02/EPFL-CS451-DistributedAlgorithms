package cs451;

import java.io.*;
import java.util.concurrent.*;

public class Main {

    private static Host myHost;

    // handle termination signals
    private static void handleSignal() {
        Process process;
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");
        myHost.stopProcessing();

        //write/flush output file if necessary
        System.out.println("Writing output.");
        myHost.flushLog();
    }

    // initializes a shutdown hook, thread that runs while Java VM is shutting down, for graceful shutdown
    private static void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal();
            }
        });
    }

    public static void main(String[] args) throws InterruptedException {
        // feeding cmd line arguments to parser, to extract the various files
        Parser parser = new Parser(args);
        // parsing
        parser.parse();

        // initialize signal handler
        initSignalHandlers();

        // thread pool for multithreading, with limit at 8
        ExecutorService threadPool = Executors.newFixedThreadPool(8);

        // example
        long pid = ProcessHandle.current().pid();
        System.out.println("My PID: " + pid + "\n");
        System.out.println("From a new terminal type `kill -SIGINT " + pid + "` or `kill -SIGTERM " + pid + "` to stop processing packets\n");

        System.out.println("My ID: " + parser.myId() + "\n");
        System.out.println("List of resolved hosts is:");
        System.out.println("==========================");
        for (Host host: parser.hosts()) {
            System.out.println(host.getId());
            System.out.println("Human-readable IP: " + host.getIp());
            System.out.println("Human-readable Port: " + host.getPort());
            System.out.println();
        }
        System.out.println();

        System.out.println("Path to output:");
        System.out.println("===============");
        System.out.println(parser.output() + "\n");

        System.out.println("Path to config:");
        System.out.println("===============");
        System.out.println(parser.config() + "\n");

        System.out.println("Doing some initialization\n");

        // getting my host
        myHost = parser.hosts().get(parser.myId() - 1);

        // giving the host the output file path
        parser.hosts().get(parser.myId() - 1).setOutputPath(parser.output());

        // list of messages that will need to be sent, thread safe
        ConcurrentLinkedQueue<Message> messages = new ConcurrentLinkedQueue<>();
        Host receiverHost = null;
        boolean flagReceiver = false;

        // getting config file path
        String configFile = parser.config();

        int numMessages = 0;
        int receiverId;

        // setting up the sending phase or the receiving phase
        try(BufferedReader br = new BufferedReader(new FileReader(configFile))) {
            int lineNum = 1;

            // read all lines in the config file
            for(String line; (line = br.readLine()) != null; lineNum++) {
                if (line.isBlank()) {
                    continue;
                }

                // dividing number of messages to send and who to send them
                String[] splits = line.split(" ");
                if (splits.length != 1) {
                    System.err.println("Problem with the line " + lineNum + " in the configuration file!");
                }
                numMessages = Integer.parseInt(splits[0]);
            }
        } catch (IOException e) {
            System.err.println("Problem with the configuration file!");
        }


        System.out.println("Broadcasting and delivering messages...\n");

        myHost.startURBBroadcaster(parser.hosts());

        // initialize all the message objects that need to be sent
        int myId = parser.myId();
        for (int i = 1; i <= numMessages; i++) {
            myHost.broadcastMessage(new Message(i, i, myId));
            // System.out.println("Added new message for host " + myId);
        }
        System.out.println("Done");


        threadPool.shutdown();

        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.
        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }
}
