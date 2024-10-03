package cs451;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    // handle termination signals
    private static void handleSignal() {
        Process process;
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");

        //write/flush output file if necessary
        System.out.println("Writing output.");
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
        Host myHost = parser.hosts().get(parser.myId() - 1);

        // giving the host the output file path
        parser.hosts().get(parser.myId() - 1).sendOutputPath(parser.output());

        // list of messages that will need to be sent, thread safe
        ConcurrentLinkedQueue<Object[]> messagesToSend = new ConcurrentLinkedQueue<>();

        // getting config file path
        String configFile = parser.config();

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
                if (splits.length != 2) {
                    System.err.println("Problem with the line " + lineNum + " in the configuration file!");
                }
                int num = Integer.parseInt(splits[0]);
                int receiver = Integer.parseInt(splits[1]);
                Host receiverHost = parser.hosts().get(receiver - 1);

                // receiver process does not send any message
                if (receiver == parser.myId() && receiverHost.getIp().equals(myHost.getIp())
                        && receiverHost.getPort() == myHost.getPort()) {
                    continue;
                }

                // initialize all the message objects that need to be sent
                // by initializing all the messages needed I can guarantee property PL3 - no creation
                for (int i = 1; i <= num; i++) {
                    Message message = new Message(i, i, parser.myId());
                    Object[] messagePack = {message, receiverHost};
                    messagesToSend.add(messagePack);
                }
            }
        } catch (IOException e) {
            System.err.println("Problem with the configuration file!");
        }

        System.out.println("Broadcasting and delivering messages...\n");
        myHost.sendMessages(messagesToSend);


        threadPool.shutdown();

        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.
        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }
}
