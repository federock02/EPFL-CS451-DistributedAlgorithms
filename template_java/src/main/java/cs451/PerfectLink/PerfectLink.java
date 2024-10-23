package cs451.PerfectLink;

import cs451.Host;
import cs451.Message;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.*;

public class PerfectLink {
    private final byte myId;
    private final String myIp;
    private final int myPort;
    private final Host myHost;

    private byte receiverId;
    private String receiverIp;
    private int receiverPort;

    private DatagramSocket mySocket;

    // RTT estimation variables
    // estimation of RTT
    private double estimatedRTT = 250;
    // deviation of RTT
    private double devRTT = 0;
    // smoothing factor for RTT
    private final double alpha = 0.125;
    // smoothing factor for deviation
    private final double beta = 0.25;

    // thread pools for sending, resending and acks
    private ThreadPoolExecutor threadPool;
    private ThreadPoolExecutor ackListener;

    private boolean flagStopProcessing = false;

    // message queue for generating a package of maxNumPerPackage messages
    private final Queue<Message> messagePackage = new ConcurrentLinkedQueue<>();
    private int maxNumPerPackage = 8;
    private static final long SEND_TIMER = 150;
    private final Object queueLock = new Object();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private ScheduledFuture<?> timeoutTask;

    // structure for messages that haven't been acknowledged yet
    private final Map<Integer, Object[]> unacknowledgedMessages = new ConcurrentHashMap<>();

    // structure for already delivered messages in receiving phase
    private final Map<Byte, Set<Integer>> deliveredMap = new ConcurrentHashMap<>();

    // flag for sending done
    private boolean sendingDone = false;


    public PerfectLink(Host myHost) {
        this.myId = (byte) (myHost.getId() - 1);
        this.myIp = myHost.getIp();
        this.myPort = myHost.getPort();
        this.myHost = myHost;
    }

    public void startPerfectLinkSender(Host receiver, int maxNumPerPackage) {
        this.receiverId = (byte) (receiver.getId() - 1);
        this.receiverIp = receiver.getIp();
        this.receiverPort = receiver.getPort();

        this.maxNumPerPackage = maxNumPerPackage;

        try {
            this.mySocket = new DatagramSocket(null);
            this.mySocket.setReuseAddress(true);
            InetAddress address = InetAddress.getByName(myIp);
            this.mySocket.bind(new InetSocketAddress(address, myPort));
        }
        catch (SocketException | UnknownHostException e) {
            e.printStackTrace();
        }

        // bounded queues with a maximum of 100 and 50 pending tasks
        BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>(100);
        BlockingQueue<Runnable> ackQueue = new LinkedBlockingQueue<>(50);

        threadPool = new ThreadPoolExecutor(
                2, 3, 60L, TimeUnit.SECONDS, taskQueue,
                new ThreadPoolExecutor.CallerRunsPolicy());
        ackListener = new ThreadPoolExecutor(
                1, 2, 60L, TimeUnit.SECONDS, ackQueue,
                new ThreadPoolExecutor.CallerRunsPolicy());

        listenForAcks();
        startResendScheduler();
    }

    public void startPerfectLinkSender(Host receiver) {
        this.receiverId = (byte) (receiver.getId() - 1);
        this.receiverIp = receiver.getIp();
        this.receiverPort = receiver.getPort();

        try {
            this.mySocket = new DatagramSocket(null);
            this.mySocket.setReuseAddress(true);
            InetAddress address = InetAddress.getByName(myIp);
            this.mySocket.bind(new InetSocketAddress(address, myPort));
        }
        catch (SocketException | UnknownHostException e) {
            e.printStackTrace();
        }

        // bounded queues with a maximum of 100 and 50 pending tasks
        BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>(100);
        BlockingQueue<Runnable> ackQueue = new LinkedBlockingQueue<>(50);

        threadPool = new ThreadPoolExecutor(
                8, 8, 60L, TimeUnit.SECONDS, taskQueue,
                new ThreadPoolExecutor.CallerRunsPolicy());
        ackListener = new ThreadPoolExecutor(
                4, 4, 60L, TimeUnit.SECONDS, ackQueue,
                new ThreadPoolExecutor.CallerRunsPolicy());

        listenForAcks();
        startResendScheduler();

        listenForAcks();
        startResendScheduler();
    }

    public void startPerfectLinkReceiver() {
        try {
            this.mySocket = new DatagramSocket(null);
            this.mySocket.setReuseAddress(true);
            InetAddress address = InetAddress.getByName(myIp);
            this.mySocket.bind(new InetSocketAddress(address, myPort));
        }
        catch (SocketException | UnknownHostException e) {
            e.printStackTrace();
        }

        // bounded queue for managing pending tasks
        BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>(50);

        threadPool = new ThreadPoolExecutor(2,  2, 60L, TimeUnit.SECONDS,
                taskQueue, new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public void stopProcessing() {
        this.flagStopProcessing = true;
        threadPool.shutdown();
        if (ackListener != null) {
            ackListener.shutdown();
        }
        if (mySocket != null && !mySocket.isClosed()) {
            try {
                if (!threadPool.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                    threadPool.shutdownNow();
                }
                if (ackListener != null && !ackListener.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                    ackListener.shutdownNow();
                }
            } catch (InterruptedException e) {
                threadPool.shutdownNow();
                if (ackListener != null) {
                    ackListener.shutdownNow();
                }
                Thread.currentThread().interrupt();
            } finally {
                if (mySocket != null && !mySocket.isClosed()) {
                    mySocket.close();
                    System.out.println("Socket closed.");
                }
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // SENDING
    // -----------------------------------------------------------------------------------------------------------------

    // send primitive for p2p perfect link
    public void send(Message message) {
        // System.out.println("Sending " + message.getId());
        myHost.logTesting("Sending " + message.getId());
        synchronized (queueLock) {
            messagePackage.add(message);
            unacknowledgedMessages.put(message.getId(), new Object[]{message, receiverId, System.currentTimeMillis()});
            if (messagePackage.size() >= maxNumPerPackage) {
                sendMessagesBatch(new LinkedList<>(messagePackage));
                messagePackage.clear();
                resetTimeout();
            } else {
                if (timeoutTask == null || timeoutTask.isCancelled()) {
                    timeoutTask = scheduler.schedule(() -> sendMessagesBatch(messagePackage), SEND_TIMER, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    // resend primitive, implements the same logic as sending
    public void resend(Message message) {
        // System.out.println("Resending " + message.getId());
        myHost.logTesting("Resending " + message.getId());
        synchronized (queueLock) {
            messagePackage.add(message);
            if (messagePackage.size() >= maxNumPerPackage) {
                sendMessagesBatch(new LinkedList<>(messagePackage));
                messagePackage.clear();
                resetTimeout();
            } else {
                if (timeoutTask == null || timeoutTask.isCancelled()) {
                    timeoutTask = scheduler.schedule(() -> sendMessagesBatch(messagePackage), SEND_TIMER, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    private void resetTimeout() {
        if (timeoutTask != null) {
            timeoutTask.cancel(false);
        }
    }

    // logic for creating the package and then sending it message from host to another host
    private void sendMessagesBatch(Queue<Message> messagePackage) {
        // total size of the package
        int totalSize = 0;
        for (Message message : messagePackage) {
            totalSize += 4 + message.serialize().length;
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        // serialize each message and add to buffer
        while (!messagePackage.isEmpty()) {
            if (!flagStopProcessing) {
                Message message = messagePackage.poll();
                byte[] serializedMessage = message.serialize();

                buffer.putInt(serializedMessage.length);

                buffer.put(serializedMessage);
                //System.out.println("Sent message: " + message.getId() + " to " + receiver.getIp() + ":" + receiver.getPort());
            }
        }
        byte[] byteData = buffer.array();

        // getting the receiver IP address in the right format
        InetAddress receiverAddress;

        try {
            receiverAddress = InetAddress.getByName(this.receiverIp);

            // create packet with data, size of data  and receiver info
            DatagramPacket packet = new DatagramPacket(byteData, byteData.length, receiverAddress, this.receiverPort);
            // System.out.println("Sending message of length " + byteData.length + " to: " + receiver.getIp() + " : " + receiver.getPort());

            // send the packet through the UDP socket
            if (!flagStopProcessing) {
                mySocket.send(packet);
            }
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
                    // wait before sending again, with adaptive timeout
                    long dynamicTimeout = calculateTimeout();
                    Thread.sleep(dynamicTimeout);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                for (Object[] messagePack : unacknowledgedMessages.values()) {
                    threadPool.submit(() -> resend((Message) messagePack[0]));
                }
            }
        });
    }

    // adaptive timeout calculation
    private long calculateTimeout() {
        long timeout = (long) (estimatedRTT + 4 * devRTT);
        return Math.max(timeout, 100);
    }

    // logic for receiving acknowledgments from receivers
    public void listenForAcks() {
        ackListener.submit(() -> {
            try {
                byte[] buffer = new byte[5];
                while (true) {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    try {
                        if (!flagStopProcessing) {
                            mySocket.receive(packet);
                        }
                    } catch (SocketException e) {
                        if (flagStopProcessing) {
                            // socket is closed during shutdown, exit gracefully
                            break;
                        } else {
                            // unexpected socket exception
                            e.printStackTrace();
                        }
                    }

                    ByteBuffer byteBuffer = ByteBuffer.wrap(packet.getData());
                    int messageId = byteBuffer.getInt();
                    byte receiver = byteBuffer.get();

                    if (receiver != receiverId) {
                        continue;
                    }

                    long currentTime = System.currentTimeMillis();
                    if (unacknowledgedMessages.containsKey(messageId)) {
                        Object[] messagePack = unacknowledgedMessages.get(messageId);
                        unacknowledgedMessages.remove(messageId);
                        long sendTime = (long) messagePack[2];
                        long sampleRTT = currentTime - sendTime;

                        // update estimated RTT and deviation
                        this.estimatedRTT = (1 - this.alpha) * this.estimatedRTT + this.alpha * sampleRTT;
                        this.devRTT = (1 - this.beta) * this.devRTT + this.beta * Math.abs(sampleRTT - this.estimatedRTT);
                    }
                    // System.out.println("Received ack for message " + messageId);
                    myHost.logTesting("Received ack for message " + messageId);
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
        // System.out.println("Receiver " + this.ip + " : " + this.port);
        threadPool.submit(this::receiverThreadMethod);
    }

    private void receiverThreadMethod() {
        try {
            // buffer for incoming messages
            byte[] buffer = new byte[2048];

            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                try {
                    if (!flagStopProcessing) {
                        mySocket.receive(packet);
                    }
                } catch (SocketException e) {
                    if (flagStopProcessing) {
                        // socket is closed during shutdown, exit gracefully
                        break;
                    } else {
                        // unexpected socket exception
                        e.printStackTrace();
                    }
                }
                InetAddress senderAddress = packet.getAddress();
                int senderPort = packet.getPort();

                ByteBuffer byteBuffer = ByteBuffer.wrap(packet.getData());
                int packetLength = packet.getLength();

                if (!flagStopProcessing) {
                    // process
                    threadPool.submit(() -> processMessage(byteBuffer, packetLength, senderAddress, senderPort));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void processMessage(ByteBuffer byteBuffer, int packetLength, InetAddress senderAddress, int senderPort) {
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
            int messageId = message.getId();
            byte senderId = message.getByteSenderId();
            int payloadAsInt = ByteBuffer.wrap(message.getPayload()).getInt();

            threadPool.submit(() -> sendAck(messageId, senderAddress, senderPort));

            // check if the message is already delivered from sender
            Set<Integer> deliveredMessages;
            if ((deliveredMessages = deliveredMap.get(senderId)) != null) {
                if (!deliveredMessages.contains(messageId)) {
                    // add to delivered
                    deliveredMessages.add(messageId);

                    threadPool.submit(() -> myHost.logDeliver(senderId, messageId));
                    // System.out.println("Delivered message from " + senderId);
                }
            } else {
                // never received from sender, add to delivered and add sender
                deliveredMessages = ConcurrentHashMap.newKeySet();

                // add to delivered
                deliveredMessages.add(messageId);
                deliveredMap.put(senderId, deliveredMessages);

                threadPool.submit(() -> myHost.logDeliver(senderId, messageId));
                // System.out.println("Delivered message from " + senderId);
            }
        }
    }

    // sending ack for received message
    private void sendAck(int messageId, InetAddress senderAddress, int senderPort) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(5);

        // convert messageId and myId to bytes
        byteBuffer.putInt(messageId);
        byteBuffer.put(this.myId);

        byte[] byteAckData = byteBuffer.array();

        // System.out.println("Acknowledged message: " + messageId + " to " + senderAddress + ":" + senderPort);

        DatagramPacket ackPacket = new DatagramPacket(byteAckData, byteAckData.length, senderAddress, senderPort);

        if (!flagStopProcessing) {
            try {
                mySocket.send(ackPacket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
