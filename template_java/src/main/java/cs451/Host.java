package cs451;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.nio.ByteBuffer;

// single process in the system
public class Host {

    private static final String IP_START_REGEX = "/";
    private static final int MAX_NUM_MESSAGES_PER_PACKAGE = 8;
    private static final int THREAD_POOL_SIZE_SENDING = 7;
    private static final int THREAD_POOL_SIZE = 8;
    private static final int RESEND_TIMEOUT = 100;

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
    private final Map<Integer, Object[]> unacknowledgedMessages = new ConcurrentHashMap<>();

    // structure for already delivered messages in receiving phase
    private Map<Integer, Set<Integer>> deliveredMap = new ConcurrentHashMap<>();

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
            socket = new DatagramSocket(null);
            socket.setReuseAddress(true);
            InetAddress address = InetAddress.getByName(ip);
            socket.bind(new InetSocketAddress(address, port));
            System.out.println("Socket bound to " + socket.getLocalAddress().getHostAddress() + " : " + socket.getLocalPort());
        }
        catch (SocketException | UnknownHostException e) {
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

    public void setOutputPath(String path) {
        this.outputPath = path;
        File file = new File(path);
        file.delete();
    }

    // -----------------------------------------------------------------------------------------------------------------
    // SENDING
    // -----------------------------------------------------------------------------------------------------------------

    // when I get the whole list of messages to send, I divide them in packages of 8
    public void sendMessages(ConcurrentLinkedQueue<Object[]> messagesToSend) {
        threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE_SENDING);
        ackListener = Executors.newFixedThreadPool(THREAD_POOL_SIZE - THREAD_POOL_SIZE_SENDING);

        // starting thread that listens for acks
        listenForAcks();

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
        // total size of the package
        int totalSize = 0;
        for (Message message : messageQueue) {
            totalSize += 4 + message.serialize().length;
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        // serialize each message and add to buffer
        while (!messageQueue.isEmpty()) {
            Message message = messageQueue.poll();
            byte[] serializedMessage = message.serialize();

            buffer.putInt(serializedMessage.length);

            buffer.put(serializedMessage);

            logger("b " + receiver.getId());
            System.out.println("Sent message: " + message.getId() + " to " + receiver.getIp() + ":" + receiver.getPort());
        }

        byte[] byteData = buffer.array();

        /*
        byte[] byteData = {};
        byte[] newData;
        byte[] serializedMessage;


        // one package will contain 8 messages
        while (!messageQueue.isEmpty()) {
            Message message = messageQueue.poll();
            serializedMessage = message.serialize();

            int messageSize = serializedMessage.length;
            byte[] messageSizeBytes = new byte[4];
            messageSizeBytes[0] = (byte) (messageSize >> 24);
            messageSizeBytes[1] = (byte) (messageSize >> 16);
            messageSizeBytes[2] = (byte) (messageSize >> 8);
            messageSizeBytes[3] = (byte) (messageSize);

            newData = new byte[byteData.length + 4 + serializedMessage.length];

            System.arraycopy(byteData, 0, newData, 0, byteData.length);
            System.arraycopy(messageSizeBytes, 0, newData, byteData.length, 4);
            System.arraycopy(serializedMessage, 0, newData, byteData.length + 4, serializedMessage.length);
            byteData = newData;

            logger("b " + receiver.getId());
            System.out.println("Sent message: " + message.getId() + " to " + receiver.getIp() + ":" + receiver.getPort());
        }
        */

        // getting the receiver IP address in the right format
        InetAddress receiverAddress;
        try {
            receiverAddress = InetAddress.getByName(receiver.getIp());

            // create packet with data, size of data  and receiver info
            DatagramPacket packet = new DatagramPacket(byteData, byteData.length, receiverAddress, receiver.getPort());
            System.out.println("Sending message of length " + byteData.length + " to: " + receiver.getIp() + " : " + receiver.getPort());

            // send the packet through the UDP socket
            socket.send(packet);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    // scheduler to resend unacknowledged messages after the timeout
    private void startResendScheduler() {
        threadPool.submit(() -> {
            // will stop only if sending is done and the queue of unacknowledged messages is empty
            while (!sendingDone || !unacknowledgedMessages.isEmpty()) {
                try {
                    // wait before sending again
                    Thread.sleep(RESEND_TIMEOUT);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                for (Object[] messagePack : unacknowledgedMessages.values()) {
                    threadPool.submit(() -> resendMessage((Message) messagePack[0], (Host) messagePack[1]));
                }
            }
        });
    }

    // resending unacknowledged messages
    private void resendMessage(Message message, Host receiver) {
        try {
            byte[] serializedMessage = message.serialize();
            int messageSize = serializedMessage.length;

            ByteBuffer buffer = ByteBuffer.allocate(4 + messageSize);

            buffer.putInt(messageSize);
            buffer.put(serializedMessage);

            byte[] byteData = buffer.array();

            /*
            // create byte array for size (4 bytes) and serialized message
            byte[] byteData = new byte[4 + messageSize];

            byteData[0] = (byte) (messageSize >> 24);
            byteData[1] = (byte) (messageSize >> 16);
            byteData[2] = (byte) (messageSize >> 8);
            byteData[3] = (byte) (messageSize);
            System.arraycopy(serializedMessage, 0, byteData, 4, messageSize);
            */

            InetAddress receiverAddress = InetAddress.getByName(receiver.getIp());

            DatagramPacket packet = new DatagramPacket(byteData, byteData.length, receiverAddress, receiver.getPort());
            socket.send(packet);
            System.out.println("Resent message: " + message.getId() + " to " + receiver.getIp() + ":" + receiver.getPort());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // logic for receiving acknowledgments from receivers
    public void listenForAcks() {
        ackListener.submit(() -> {
            try {
                byte[] buffer = new byte[8];
                while (true) {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);

                    ByteBuffer byteBuffer = ByteBuffer.wrap(packet.getData());
                    int messageId = byteBuffer.getInt();

                    /*
                    int messageId = ((buffer[0] & 0xFF) << 24) |
                            ((buffer[1] & 0xFF) << 16) |
                            ((buffer[2] & 0xFF) << 8) |
                            (buffer[3] & 0xFF);
                    */

                    System.out.println("Received ack for message " + messageId);

                    unacknowledgedMessages.remove(messageId);

                    /*
                    String receivedData = new String(packet.getData(), 0, packet.getLength());

                    // extract ack
                    String[] receivedMessages = receivedData.split("\\|");
                    for (String messageData : receivedMessages) {
                        String[] parts = messageData.split(":");
                        // acknowledged message id
                        int messageId = Integer.parseInt(parts[0]);
                        // message has been acknowledged
                        unacknowledgedMessages.remove(messageId);
                    }
                    */
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    // -----------------------------------------------------------------------------------------------------------------
    // RECEIVING
    // -----------------------------------------------------------------------------------------------------------------

    // logic for receiving messages, by listening for incoming messages on UDP socket
    public void receiveMessages() {
        threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        threadPool.submit(this::receiverThreadMethod);
    }

    private void receiverThreadMethod() {
        try {
            // buffer for incoming messages
            byte[] buffer = new byte[4096];

            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                /*
                byte[] packetData = packet.getData();
                int packetLength = packet.getLength();
                int currentPos = 0;
                */

                ByteBuffer byteBuffer = ByteBuffer.wrap(packet.getData());
                int packetLength = packet.getLength();

                while (byteBuffer.position() < packetLength) {
                    // extract size of next message
                    int messageSize = byteBuffer.getInt();

                    if (byteBuffer.position() + messageSize > packetLength) {
                        System.err.println("Incomplete message");
                        break;
                    }

                    // extract message content based on size
                    byte[] messageBytes = new byte[messageSize];
                    byteBuffer.get(messageBytes, 0, messageSize);

                    // deserialize and process the message
                    Message message = Message.deserialize(messageBytes);
                    int id = message.getId();
                    int senderId = message.getSenderId();

                    // check if the message is already delivered from sender
                    Set<Integer> deliveredMessages;
                    if ((deliveredMessages = deliveredMap.get(senderId)) != null) {
                        if (!deliveredMessages.contains(id)) {
                            // add to delivered
                            deliveredMessages.add(id);

                            // logging equals to delivering
                            logger("d " + senderId + " " + message.getPayload().getValue());
                            System.out.println("Delivered message from " + senderId);
                        }
                    }
                    else {
                        // never received from sender, add to delivered and add sender
                        deliveredMessages = ConcurrentHashMap.newKeySet();

                        // add to delivered
                        deliveredMessages.add(id);
                        deliveredMap.put(senderId, deliveredMessages);

                        // logging equals to delivering
                        logger("d " + senderId + " " + message.getPayload().getValue());
                        System.out.println("Delivered message from " + senderId);
                    }


                    // send ack, both if it was delivered ot not
                    threadPool.submit(() -> sendAck(id, packet.getAddress(), packet.getPort()));
                }

                /*
                // split multiple messages in the same package
                String[] messages = data.split("\\|");

                // split the data in the single message
                for (String message : messages) {
                    String[] splits = message.split(":");
                    int id = Integer.parseInt(splits[0]);
                    int payload = Integer.parseInt(splits[1]);
                    int senderId = Integer.parseInt(splits[2]);

                    // check if the message is already delivered from sender
                    Set<Integer> deliveredMessages;
                    if ((deliveredMessages = deliveredMap.get(senderId)) != null) {
                        if (!deliveredMessages.contains(id)) {
                            // add to delivered
                            deliveredMessages.add(id);

                            // logging equals to delivering
                            logger("d " + senderId + " " + payload);
                            System.out.println("Delivered message from " + senderId);
                        }
                    }
                    else {
                        // never received from sender, add to delivered and add sender
                        deliveredMessages = ConcurrentHashMap.newKeySet();

                        // add to delivered
                        deliveredMessages.add(id);
                        deliveredMap.put(senderId, deliveredMessages);

                        // logging equals to delivering
                        logger("d " + senderId + " " + payload);
                        System.out.println("Delivered message from " + senderId);
                    }


                    // send ack, both if it was delivered ot not
                    threadPool.submit(() -> sendAck(id, packet.getAddress(), packet.getPort()));
                }
                */
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // sending ack for received message
    private void sendAck(int messageId, InetAddress receiverAddress, int receiverPort) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);

        // convert messageId and receiverId to bytes
        byteBuffer.putInt(messageId);
        byteBuffer.putInt(this.id);

        byte[] byteAckData = byteBuffer.array();

        System.out.println("Acknowledged message: " + messageId + " to " + receiverAddress + ":" + receiverPort);

        DatagramPacket ackPacket = new DatagramPacket(byteAckData, byteAckData.length, receiverAddress, receiverPort);
        try {
            socket.send(ackPacket);
        } catch (IOException e) {
            e.printStackTrace();
        }

        /*
        // easy way, because receiver only receives and sender only sends, doesn't need to signal it is an ack
        String ackData = messageId + ":" + this.id;
        byte[] byteAckData = ackData.getBytes();
        System.out.println("Acknowledged message: " + messageId + " to " + receiverAddress + ":" + receiverPort);

        DatagramPacket ackPacket = new DatagramPacket(byteAckData, byteAckData.length, receiverAddress, receiverPort);
        try {
            socket.send(ackPacket);
        } catch (IOException e) {
            e.printStackTrace();
        }
        */
    }

    // -----------------------------------------------------------------------------------------------------------------
    // LOGGING
    // -----------------------------------------------------------------------------------------------------------------

    public void logger(String event) {
        logBuffer.add(event);
        // actually write to file only after some events
        if (logBuffer.size() >= 1) {
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

}
