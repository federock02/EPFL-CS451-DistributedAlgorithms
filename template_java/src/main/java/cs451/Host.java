package cs451;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ExecutorService;

// single process in the system
public class Host {

    private static final String IP_START_REGEX = "/";

    private int id;
    private String ip;
    private int port = -1;

    // output path
    private String outputPath = "";

    // log buffer
    // private List<String> logBuffer = new ArrayList<>();
    // safer logging to be used with concurrency
    private final ConcurrentLinkedQueue<String> logBuffer = new ConcurrentLinkedQueue<>();

    // socket for UDP connection
    private DatagramSocket socket;

    // thread pool for sending, resending, and receiving
    private ScheduledExecutorService threadPool;

    // structure for messages that need to be sent
    private Queue<Message> messageQueue = new ConcurrentLinkedQueue<>();
    // structure for messages that haven't been acknowledged yet
    private Map<Integer, Message> unacknowledgedMessages = new ConcurrentHashMap<>();
    // structure for messages that have been acknowledged
    private Set<Integer> receivedAcks = ConcurrentHashMap.newKeySet();

    // delay before resending unacknowledged messages
    private static final long RESEND_DELAY = 300;

    // pseudo constructor with boolean return
    public boolean populate(String idString, String ipString, String portString) {
        try {
            // id
            id = Integer.parseInt(idString);

            // IP address with validity check
            String ipTest = InetAddress.getByName(ipString).toString();
            if (ipTest.startsWith(IP_START_REGEX)) {
                ip = ipTest.substring(1);
            } else {
                ip = InetAddress.getByName(ipTest.split(IP_START_REGEX)[0]).getHostAddress();
            }

            // port with validity check
            port = Integer.parseInt(portString);
            if (port <= 0) {
                System.err.println("Port in the hosts file must be a positive number!");
                return false;
            }
        } catch (NumberFormatException e) {
            if (port == -1) {
                System.err.println("Id in the hosts file must be a number!");
            } else {
                System.err.println("Port in the hosts file must be a number!");
            }
            return false;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        // opening the socket
        try {
            socket = new DatagramSocket();
        }
        catch (SocketException e) {
            e.printStackTrace();
        }

        return true;
    }

    public int getId() {
        return id;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    // logic for sending message from host to another host
    public void sendMessage(Message message, Host receiver) {
        // convert message to bytes
        // uniting id and payload
        String data = message.getId() + ":" + message.getPayload() + ":" + message.getSenderId();
        byte[] byteData = data.getBytes();

        // getting the receiver IP address in the right format
        InetAddress receiverAddress = null;
        try {
            receiverAddress = InetAddress.getByName(receiver.getIp());
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        // create packet with data, size of data  and receiver info
        DatagramPacket packet = new DatagramPacket(byteData, byteData.length, receiverAddress, receiver.getPort());

        // send the packet through the UDP socket
        try {
            socket.send(packet);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // send to logger
        logger("b " + message.getId());

        // System.out.println("Sent message: " + message.getId() + " to " + receiver.getIp() + ":" + receiver.getPort());
    }

    // logic for receiving messages, by listening for incoming messages on UDP socket
    public void receiveMessages() {
        Thread receiverThread = new Thread(() -> receiverThreadMethod());
        receiverThread.start();
    }

    private void receiverThreadMethod() {
        try {
            // opening Datagram socket on the host port
            DatagramSocket socket = new DatagramSocket(this.port);

            // allocating buffer for incoming messages
            byte[] buffer = new byte[1024];

            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                // extract message and split with ":"
                String receivedMessage = new String(packet.getData(), 0, packet.getLength());
                String[] splits = receivedMessage.split(":");

                if (splits.length == 2) {
                    String messageId = splits[0];
                    String payload = splits[1];

                    // logging delivery
                    logger("d " + messageId + " " + payload);
                } else {
                    System.err.println("Invalid message received!");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void logger(String event) {
        logBuffer.add(event);
        // actually write to file only after some events
        if (logBuffer.size() >= 100) {
            logWriteToFile();
        }
    }

    public void sendOutputPath(String path) {
        this.outputPath = path;
    }

    private void logWriteToFile() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(this.outputPath, true))) {
            while (!logBuffer.isEmpty()) {
                writer.write(logBuffer.poll());
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
