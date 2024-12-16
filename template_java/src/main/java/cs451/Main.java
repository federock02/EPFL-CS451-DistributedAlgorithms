package cs451;

import java.io.*;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.*;

public class Main {

    private static Host myHost;

    // handle termination signals
    private static void handleSignal() {
        Process process;
        //immediately stop network packet processing
        myHost.stopProcessing();
        System.out.println("Immediately stopping network packet processing.");

        //write/flush output file if necessary
        myHost.flushLog();
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

        // getting config file path
        String configFile = parser.config();

        int p = 0;
        int vs = 0;
        int ds = 0;

        Queue<Integer[]> proposals = null;

        // setting up the sending phase or the receiving phase
        try(BufferedReader br = new BufferedReader(new FileReader(configFile))) {
            String firstLine = br.readLine();
            if (firstLine != null && !firstLine.isBlank()) {
                String[] splits = firstLine.split(" ");

                if (splits.length != 3) {
                    System.err.println("Problem with the line 1 in the configuration file!");
                }

                p = Integer.parseInt(splits[0]);
                System.out.println("p = " + p);
                proposals = new LinkedList<>();

                vs = Integer.parseInt(splits[1]);
                System.out.println("vs = " + vs);
                ds = Integer.parseInt(splits[2]);
                System.out.println("ds = " + ds);
            }
            else {
                System.err.println("Problem with the line 1 in the configuration file!");
                return;
            }

            int lineNum = 2;

            // read all lines in the config file
            for(String line; (line = br.readLine()) != null; lineNum++) {
                if (line.isBlank()) {
                    continue;
                }

                String[] splits = line.split(" ");
                if (splits.length > vs) {
                    System.err.println("Too many values proposed in line " + lineNum + " in the configuration file!");
                    continue;
                }

                // creating a new proposal of appropriate length
                Integer[] proposal = new Integer[splits.length];

                for (int i = 0; i < splits.length; i++) {
                    proposal[i] = Integer.parseInt(splits[i]);
                }
                proposals.add(proposal);
            }
        } catch (IOException e) {
            System.err.println("Problem with the configuration file!");
        }


        System.out.println("Proposing...\n");

        myHost.startLatticeAgreement(parser.hosts(), p, vs, ds);

        if (proposals == null) {
            return;
        }

        for (Integer[] proposal : proposals) {
            myHost.propose(proposal);
        }

        // after a process finishes broadcasting,
        // it waits forever for the delivery of messages.
        while (true) {
            // sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }
}
