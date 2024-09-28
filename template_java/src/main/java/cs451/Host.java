package cs451;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

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
    private ConcurrentLinkedQueue<String> logBuffer = new ConcurrentLinkedQueue<>();

    // socket for UDP connection
    private DatagramSocket socket;

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
    public void sendMessages(int count, Host receiver) {
        // receiver process does not send any message
        if (receiver.getId() == this.id && receiver.getIp().equals(this.ip)
                && receiver.getPort() == this.port) {
            return;
        }

        // initialize all the message objects that need to be sent
        // by initializing all the messages needed I can guarantee property PL3 - no creation
        Message toSend[] = new Message[count];
        for (int i = 1; i <= count; i++) {
            toSend[i-1] = new Message(i, i);
        }

        for (Message message : toSend) {
            try {
                // convert message to bytes
                // uniting id and payload
                String data = message.getId() + ":" + message.getPayload();
                byte[] byteData = data.getBytes();

                // getting the receiver IP address in the right format
                InetAddress receiverAddress = InetAddress.getByName(receiver.getIp());

                // create packet with data, size of data  and receiver info
                DatagramPacket packet = new DatagramPacket(byteData, byteData.length, receiverAddress, receiver.getPort());

                // send the packet through the UDP socket
                socket.send(packet);

                // send to logger
                logger("b " + message.getId());

                // System.out.println("Sent message: " + message.getId() + " to " + receiver.getIp() + ":" + receiver.getPort());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
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
