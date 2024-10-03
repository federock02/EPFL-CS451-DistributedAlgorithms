package cs451;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

// single process in the system
public class Host {

    private static final String IP_START_REGEX = "/";
    private static final int MAX_NUM_MESSAGES_PER_PACKAGE = 8;
    private static final int THREAD_POOL_SIZE_SENDING = 7;
    private static final int THREAD_POOL_SIZE = 8;
    private static final int RESEND_TIMEOUT = 300;

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

    // thread pools for sending, resending and acks
    private ExecutorService threadPool;
    private ExecutorService ackListener;

    // structure for messages that haven't been acknowledged yet
    private Map<Integer, Object[]> unacknowledgedMessages = new ConcurrentHashMap<>();
    // structure for messages that have been acknowledged
    private Set<Integer> receivedAcks = ConcurrentHashMap.newKeySet();

    private boolean sendingDone = false;

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

    // when I get the whole list of messages to send, I divide them in packages of 8
    public void sendMessages(ConcurrentLinkedQueue<Object[]> messagesToSend) {

        // starting thread that listens for acks
        listenForAcks();

        threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE_SENDING);
        ackListener = Executors.newFixedThreadPool(THREAD_POOL_SIZE - THREAD_POOL_SIZE_SENDING);

        HashMap<Host, Queue<Message>> messagesPackage = new HashMap<>();
        while (!messagesToSend.isEmpty()) {
            Object[] messagePack = messagesToSend.poll();
            Host receiver = (Host) messagePack[1];
            Message message = (Message) messagePack[0];

            unacknowledgedMessages.put(message.getId(), new Object[]{message, receiver});

            if (messagesPackage.containsKey(receiver)) {
                Queue<Message> messageQueue = messagesPackage.get(receiver);
                messageQueue.add(message);
                if (messageQueue.size() == MAX_NUM_MESSAGES_PER_PACKAGE) {
                    Queue<Message> queue = new LinkedList<>(messageQueue);
                    threadPool.submit(() -> sendMessagesBatch(queue, receiver));
                    messageQueue.clear();
                }
            }
            else {
                Queue<Message> messageQueue = new LinkedList<>();
                messageQueue.add(message);
                messagesPackage.put(receiver, messageQueue);
            }
        }

        // send remaining packages
        for (Host host : messagesPackage.keySet()) {
            Queue<Message> messageQueue = messagesPackage.get(host);
            if (!messageQueue.isEmpty()) {
                threadPool.submit(() -> sendMessagesBatch(messageQueue, host));
            }
        }

        sendingDone = true;
        startResendScheduler();
    }

    // logic for creating the package and then sending it message from host to another host
    public void sendMessagesBatch(Queue<Message> messageQueue, Host receiver) {
        StringBuilder data = new StringBuilder();

        // one package will contain 8 messages
        while (!messageQueue.isEmpty()) {
            Message message = messageQueue.poll();
            data.append(message.getId()).append(":").append(message.getPayload()).append(":").append(message.getSenderId()).append("|");
            logger("b " + receiver.getId());
        }

        // convert message to bytes
        byte[] byteData = data.toString().getBytes();

        // getting the receiver IP address in the right format
        InetAddress receiverAddress = null;
        try {
            receiverAddress = InetAddress.getByName(receiver.getIp());

            // create packet with data, size of data  and receiver info
            DatagramPacket packet = new DatagramPacket(byteData, byteData.length, receiverAddress, receiver.getPort());

            // send the packet through the UDP socket
            socket.send(packet);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // System.out.println("Sent message: " + message.getId() + " to " + receiver.getIp() + ":" + receiver.getPort());
    }

    // scheduler to resend unacknowledged messages after the timeout
    private void startResendScheduler() {
        threadPool.submit(() -> {
            // will stop only if sending is done and the queue of unacknowledged messages is empty
            while (!sendingDone || !unacknowledgedMessages.isEmpty()) {
                for (Object[] messagePack : unacknowledgedMessages.values()) {
                    threadPool.submit(() -> resendMessage((Message) messagePack[0], (Host) messagePack[1]));
                }
                try {
                    // wait before sending again
                    Thread.sleep(RESEND_TIMEOUT);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    // resending unacknowledged messages
    private void resendMessage(Message message, Host receiver) {
        try {
            String data = message.getId() + ":" + message.getPayload() + ":" + message.getSenderId();
            byte[] byteData = data.getBytes();
            InetAddress receiverAddress = InetAddress.getByName(receiver.getIp());
            DatagramPacket packet = new DatagramPacket(byteData, byteData.length, receiverAddress, receiver.getPort());
            socket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // logic for receiving acknowledgments from receivers
    public void listenForAcks() {
        ackListener.submit(() -> {
            try {
                byte[] buffer = new byte[1024];
                while (true) {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);
                    String receivedData = new String(packet.getData(), 0, packet.getLength());

                    // Extract acknowledgment
                    String[] receivedMessages = receivedData.split("|");
                    for (String messageData : receivedMessages) {
                        String[] parts = messageData.split(":");
                        // acknowledged message id
                        int messageId = Integer.parseInt(parts[0]);
                        if (unacknowledgedMessages.containsKey(messageId)) {
                            // message has been acknowledged
                            unacknowledgedMessages.remove(messageId);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
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
