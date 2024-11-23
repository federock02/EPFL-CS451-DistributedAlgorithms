package cs451.PerfectLink;

import cs451.FIFOURB.URB;
import cs451.Host;
import cs451.Message;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import static java.lang.Thread.sleep;

public class PerfectLink {
    // host parameters
    private final byte myId;
    private final String myIp;
    private final int myPort;

    // URB broadcaster
    private URB broadcaster;

    // socket
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

    // thread pool
    private ThreadPoolExecutor threadPool;
    // thread for not blocking caller when sending
    private SendThread sendThread;
    // private ThreadPoolExecutor ackListener;

    // flag used to manage termination signals
    private boolean flagStopProcessing = false;

    // message sending parameters
    // message queue for uniting messages in packages of maxNumPerPackage
    private final Map<Host, Queue<Message>> messagePackages = new ConcurrentHashMap<>();
    // maximum number of messages per package
    private int maxNumPerPackage = 8;
    // timeout for sending a package, even if it was not filled with maxNumPerPackage messages
    private static final long SEND_TIMER = 150;
    // lock for managing access to queue of messages to send
    private final Object queueLock = new Object();
    // executor that manages the timeout for sending the package
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    // task for sending the package
    private ScheduledFuture<?> timeoutTask;

    // map from all the other hosts to a map from long encoding the messageId and senderId to the messagePack
    private final Map<Byte, Map<Long, Object[]>> unacknowledgedMessages = new ConcurrentHashMap<>();
    // map byte id to host
    private final Map<Byte, Host> hostMapping = new HashMap<>();

    // messages list pool (avoiding constant creation and destruction with garbage collector)
    private final Queue<Queue<Message>> listPool = new LinkedList<>();

    // structure for already delivered messages in receiving phase
    // notice that it uses the sender ports as keys, and not the senderId, since messages are relayed
    private final Map<Short, Map<Byte, LinkedList<int[]>>> deliveredMap = new ConcurrentHashMap<>();
    // pool for ByteBuffers to avoid frequent allocations when receiving
    private final ArrayBlockingQueue<ByteBuffer> byteBufferPoolReceiving = new ArrayBlockingQueue<>(10);
    // receiving buffer size
    private static final int RECEIVING_BUFF_SIZE = 2048;
    // pool for ByteBuffers to avoid frequent allocations when sending acks
    private final ArrayBlockingQueue<ByteBuffer> byteBufferPoolAcks = new ArrayBlockingQueue<>(10);
    // ack buffer size
    private static final int ACK_BUFF_SIZE = 6;
    // pool of DatagramPackets for sending acks
    private final ArrayBlockingQueue<DatagramPacket> datagramPacketsPool = new ArrayBlockingQueue<>(10);

    // constructor for perfect link
    public PerfectLink(Host myHost) {
        this.myId = (byte) (myHost.getId() - 1);
        this.myIp = myHost.getIp();
        this.myPort = myHost.getPort();
    }

    // constructor for perfect link under URB broadcaster
    public PerfectLink(Host myHost, URB broadcaster) {
        this(myHost);
        this.broadcaster = broadcaster;
    }

    // starting perfect link sender, with default maxNumPerPackage
    public void startPerfectLinkSender() {
        try {
            this.mySocket = new DatagramSocket(null);
            this.mySocket.setReuseAddress(true);
            InetAddress address = InetAddress.getByName(myIp);
            this.mySocket.bind(new InetSocketAddress(address, (myPort + 128)));
        }
        catch (SocketException | UnknownHostException e) {
            e.printStackTrace();
        }

        sendThread = new SendThread();
        sendThread.start();

        listenForAcks();
        startResendScheduler();
    }

    // starting perfect link receiver
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
        BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>(100);
        threadPool = new ThreadPoolExecutor(
                0,  10, 5L, TimeUnit.SECONDS, taskQueue,
                new ThreadPoolExecutor.CallerRunsPolicy());

        // initialize the buffer pools
        for (int i = 0; i < byteBufferPoolReceiving.remainingCapacity(); i++) {
            byteBufferPoolReceiving.offer(ByteBuffer.allocate(RECEIVING_BUFF_SIZE));
        }
        for (int i = 0; i < byteBufferPoolAcks.remainingCapacity(); i++) {
            byteBufferPoolAcks.offer(ByteBuffer.allocate(ACK_BUFF_SIZE));
        }
        for (int i = 0; i < datagramPacketsPool.remainingCapacity(); i++) {
            datagramPacketsPool.offer(new DatagramPacket(new byte[ACK_BUFF_SIZE], ACK_BUFF_SIZE));
        }
    }

    // handle termination
    public void stopProcessing() {
        this.flagStopProcessing = true;
        if (threadPool != null) {
            threadPool.shutdown();
        }
        if (mySocket != null && !mySocket.isClosed()) {
            try {
                if (threadPool != null && !threadPool.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                    threadPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                if (threadPool != null) {
                    threadPool.shutdownNow();
                }
                Thread.currentThread().interrupt();
            } finally {
                if (mySocket != null && !mySocket.isClosed()) {
                    mySocket.close();
                    // System.out.println("Socket closed.");
                }
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // SENDING
    // -----------------------------------------------------------------------------------------------------------------

    // send primitive for p2p perfect link
    public void send(Message message, Host host) {
        sendThread.send(message, host);
    }

    // resend primitive, implements the same logic as sending
    public void resend(Message message, Host host) {
        // System.out.println("Resending");
        synchronized (queueLock) {
            // get the queue corresponding to the host to send to
            Queue<Message> messagePackage = messagePackages.get(host);
            // add the message to the queue
            messagePackage.add(message);
            // System.out.println("plResending " + message.getId() + " from " + message.getSenderId() + " to " + host.getId());

            // check if queue for this host har reached the size
            if (messagePackage.size() >= maxNumPerPackage) {
                Queue<Message> toSend = borrowList();
                toSend.addAll(messagePackage);
                sendMessagesBatch(toSend, host);
                messagePackage.clear();
                resetTimeout();
            } else {
                // otherwise, if there is none, start the timer
                if (timeoutTask == null || timeoutTask.isCancelled()) {
                    timeoutTask = scheduler.schedule(() -> {
                        Queue<Message> toSend = borrowList();
                        toSend.addAll(messagePackages.get(host));
                        sendMessagesBatch(toSend, host);
                        messagePackages.get(host).clear();
                    }, SEND_TIMER, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    class SendThread extends Thread {
        Queue<Object[]> outgoing = new ConcurrentLinkedQueue<>();
        Host host;
        Message message;

        public void send(Message message, Host host) {
            outgoing.add(new Object[]{host, message});
        }

        public void run() {
            while (true) {
                Object[] entry = outgoing.poll();
                if (entry != null) {
                    host = (Host) entry[0];
                    message = (Message) entry[1];
                    synchronized (queueLock) {
                        // if there is none, create the mapping between byte id and host
                        hostMapping.putIfAbsent(host.getByteId(), host);
                        // get the queue corresponding to the host to send to
                        Queue<Message> messagePackage = messagePackages.computeIfAbsent(host, k -> new LinkedList<>());
                        // add the message to the queue
                        messagePackage.add(message);
                        // System.out.println("plSending " + message.getId() + " from " + message.getSenderId() + " to " + host.getId());

                        // add message to unacknowledged ones
                        Map<Long, Object[]> unacknowledgedFromSender = unacknowledgedMessages.computeIfAbsent(host.getByteId(),
                                k -> new ConcurrentHashMap<>());
                        unacknowledgedFromSender.put(encodeMessageKey(message.getId(), message.getByteSenderId()),
                                new Object[]{message, System.currentTimeMillis()});

                        // check if queue for this host har reached the size
                        if (messagePackage.size() >= maxNumPerPackage) {
                            Queue<Message> toSend = borrowList();
                            toSend.addAll(messagePackage);
                            sendMessagesBatch(toSend, host);
                            messagePackage.clear();
                            resetTimeout();
                        } else {
                            // otherwise, if there is none, start the timer
                            if (timeoutTask == null || timeoutTask.isCancelled()) {
                                timeoutTask = scheduler.schedule(() -> {
                                    Queue<Message> toSend = borrowList();
                                    toSend.addAll(messagePackages.get(host));
                                    sendMessagesBatch(toSend, host);
                                    messagePackages.get(host).clear();
                                }, SEND_TIMER, TimeUnit.MILLISECONDS);
                            }
                        }
                    }
                    // if the host has too many unacknowledged messages, wait a bit
                    while (unacknowledgedMessages.get(host.getByteId()).size() >= 100) {
                        try {
                            Thread.sleep(20);
                        } catch (InterruptedException e) {
                            // exit gracefully if the thread was interrupted
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
            }
        }
    }


    private Queue<Message> borrowList() {
        return listPool.isEmpty() ? new LinkedList<>() : listPool.poll();
    }

    private void returnList(Queue<Message> list) {
        list.clear();
        listPool.offer(list);
    }

    private void resetTimeout() {
        if (timeoutTask != null) {
            timeoutTask.cancel(false);
        }
    }

    // logic for creating the package and then sending it message from host to another host
    private void sendMessagesBatch(Queue<Message> messagePackage, Host host) {
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
            }

        }
        returnList(messagePackage);
        byte[] byteData = buffer.array();

        try {
            // create packet with data, size of data  and receiver info
            DatagramPacket packet = new DatagramPacket(byteData, byteData.length,
                    InetAddress.getByName(host.getIp()), host.getPort());

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
        new Thread(() -> {
            // will stop only if sending is done and the queue of unacknowledged messages is empty
            while (true) {
                try {
                    // wait before sending again, with adaptive timeout
                    long dynamicTimeout = calculateTimeout();
                    sleep(dynamicTimeout + 100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                for (Byte host : unacknowledgedMessages.keySet()) {
                    for (Long key : unacknowledgedMessages.get(host).keySet()) {
                        Object[] messagePack = unacknowledgedMessages.get(host).get(key);
                        if (messagePack != null) {
                            resend((Message) messagePack[0], hostMapping.get(host));
                        }
                    }
                }
            }
        }).start();
    }

    // adaptive timeout calculation
    private long calculateTimeout() {
        long timeout = (long) (estimatedRTT + 4 * devRTT);
        return Math.min(Math.max(timeout, 100), 1000);
    }

    // logic for receiving acknowledgments from receivers
    public void listenForAcks() {
        DatagramPacket packet = new DatagramPacket(new byte[ACK_BUFF_SIZE], ACK_BUFF_SIZE);
        ByteBuffer byteBuffer = ByteBuffer.allocate(ACK_BUFF_SIZE);
        new Thread(() -> {
            try {
                while (true) {
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
                    byteBuffer.clear();
                    byteBuffer.put(packet.getData());
                    byteBuffer.flip();
                    // id of the message
                    int messageId = byteBuffer.getInt();
                    // original sender of the message
                    byte senderId = byteBuffer.get();
                    // sender of the ack
                    byte receiver = byteBuffer.get();
                    // System.out.println("Received ack for " + messageId);

                    // System.out.println("Received ack " + messageId + " from " + (senderId + 1) + " from " + (receiver + 1));
                    /*
                    for (Byte host : unacknowledgedMessages.keySet()) {
                        System.out.println("Sender " + (s + 1));
                        System.out.println("Messages");
                        for (Integer m : unacknowledgedMessages.get(s).keySet()) {
                            System.out.println(m);
                        }
                    }
                    */

                    long currentTime = System.currentTimeMillis();
                    long key = encodeMessageKey(messageId, senderId);

                    if (unacknowledgedMessages.get(receiver).containsKey(key)) {
                        Object[] messagePack = unacknowledgedMessages.get(receiver).get(key);
                        unacknowledgedMessages.get(receiver).remove(key);
                        long sendTime = (long) messagePack[1];
                        long sampleRTT = currentTime - sendTime;

                        // update estimated RTT and deviation
                        this.estimatedRTT = (1 - this.alpha) * this.estimatedRTT + this.alpha * sampleRTT;
                        this.devRTT = (1 - this.beta) * this.devRTT + this.beta * Math.abs(sampleRTT - this.estimatedRTT);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    // -----------------------------------------------------------------------------------------------------------------
    // RECEIVING
    // -----------------------------------------------------------------------------------------------------------------

    // logic for receiving messages, by listening for incoming messages on UDP socket
    public void receiveMessages() {
        new Thread(() -> {
            try {
                DatagramPacket packet = new DatagramPacket(new byte[RECEIVING_BUFF_SIZE], RECEIVING_BUFF_SIZE);

                while (!flagStopProcessing) {
                    try {
                        mySocket.receive(packet);
                        InetAddress senderAddress = packet.getAddress();
                        int senderPort = packet.getPort();

                        ByteBuffer byteBuffer = byteBufferPoolReceiving.poll();
                        if (byteBuffer == null) {
                            // if the pool is empty, allocate a new one
                            byteBuffer = ByteBuffer.allocate(RECEIVING_BUFF_SIZE);
                        } else {
                            // clear the buffer before reuse
                            byteBuffer.clear();
                        }

                        int packetLength = packet.getLength();

                        // System.out.println("NEW PL PACKAGE, SIZE " + packetLength);

                        // wrap the packet data into the ByteBuffer
                        byteBuffer.put(packet.getData(), 0, packetLength);
                        byteBuffer.flip();

                        if (!flagStopProcessing) {
                            // process
                            processMessage(byteBuffer, packetLength, senderAddress, senderPort);

                            // give back the byte buffer
                            byteBufferPoolReceiving.offer(byteBuffer);
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
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private void processMessage(ByteBuffer byteBuffer, int packetLength, InetAddress senderAddress, int senderPort) {
        int i;
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

            // System.out.println("PLReceived message " + messageId + " from " + (senderId + 1) + " sent by " + senderPort);

            // check if the message is already delivered from sender
            // sender of the message is indicated by the senderPort
            Map<Byte, LinkedList<int[]>> deliveredFromSender;
            LinkedList<int[]> deliveredMessages;
            synchronized (deliveredMap) {
                deliveredFromSender = deliveredMap.get((short) senderPort);
                if (deliveredFromSender != null) {
                    deliveredMessages = deliveredFromSender.get(senderId);
                    if (deliveredMessages != null) {
                        i = addMessage(deliveredMessages, messageId);
                        if (i != -1) {
                            // message wasn't already delivered
                            broadcaster.plDeliver(message);
                        }
                    } else {
                        // never received from sender from this port, add to delivered and add sender
                        // System.out.println("never from senderId");
                        deliveredMessages = new LinkedList<>();

                        // add to delivered
                        deliveredMessages.add(new int[]{messageId, messageId});
                        deliveredFromSender.put(senderId, deliveredMessages);

                        broadcaster.plDeliver(message);
                    }
                }
                else {
                    // never received from this port
                    // System.out.println("never from senderPort");
                    deliveredMessages = new LinkedList<>();
                    deliveredFromSender = new HashMap<>();

                    // add to delivered
                    deliveredMessages.add(new int[]{messageId, messageId});
                    deliveredFromSender.put(senderId, deliveredMessages);
                    deliveredMap.put((short) senderPort, deliveredFromSender);

                    broadcaster.plDeliver(message);
                }

            }

            // threadPool.submit(() -> sendAck(messageId, senderId, senderAddress, senderPort));
            sendAck(messageId, senderId, senderAddress, senderPort);
        }
    }

    // sending ack for received message
    private void sendAck(int messageId, byte senderId, InetAddress senderAddress, int senderPort) {
        ByteBuffer byteBuffer = byteBufferPoolAcks.poll();
        if (byteBuffer == null) {
            // if the pool is empty, allocate a new one
            byteBuffer = ByteBuffer.allocate(ACK_BUFF_SIZE);
        } else {
            // clear the buffer before reuse
            byteBuffer.clear();
        }

        // convert messageId and myId to bytes
        byteBuffer.putInt(messageId);
        byteBuffer.put(senderId);
        byteBuffer.put(this.myId);

        byte[] byteAckData = byteBuffer.array();

        DatagramPacket ackPacket = datagramPacketsPool.poll();
        if (ackPacket == null) {
            ackPacket = new DatagramPacket(byteAckData, byteAckData.length, senderAddress, senderPort);
        } else {
            // Reuse the packet by updating its data, address, and port
            ackPacket.setData(byteAckData);
            ackPacket.setAddress(senderAddress);
            ackPacket.setPort(senderPort);
            // System.out.println("Acking " + messageId + " from " + (senderId + 1) + " to " + senderAddress + ":" + senderPort);
        }

        if (!flagStopProcessing) {
            try {
                mySocket.send(ackPacket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        byteBufferPoolAcks.offer(byteBuffer);
        datagramPacketsPool.offer(ackPacket);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // UTILS
    // -----------------------------------------------------------------------------------------------------------------

    private int addMessage(LinkedList<int[]> deliveredMessages, int messageId) {
        int start = 0;
        int end = deliveredMessages.size() - 1;
        int middle = 0;

        // binary search to find if a message has been delivered in the compressed structure
        while (start <= end) {
            middle = (start + end) / 2;

            int[] range = deliveredMessages.get(middle);
            if (range[0] <= messageId && messageId <= range[1]) {
                // message is in one of the delivered ranges
                return -1;
            }
            else if (range[0] > messageId) {
                // lower bound of range is higher than message id
                if (range[0] == messageId + 1) {
                    // message id can extend the range by 1 on the left
                    range[0] = messageId;
                    if (middle > 0) {
                        // getting previous range to check if the two adjacent ranges can be compressed
                        int[] range2 = deliveredMessages.get(middle - 1);
                        if (range2[1] == range[0] - 1) {
                            range2[1] = range[1];
                            deliveredMessages.remove(middle);
                            middle = middle - 1;
                        }
                    }
                    return middle;
                }
                end = middle - 1;
            }
            else {
                // upper bound of range is lower than message id
                if (range[1] == messageId - 1) {
                    // message id can extend the range by 1 on the right
                    range[1] = messageId;
                    if (middle < (deliveredMessages.size() - 1)) {
                        // getting following range to check if the two adjacent ranges can be compressed
                        int[] range2 = deliveredMessages.get(middle + 1);
                        if (range2[0] == range[1] + 1) {
                            range2[0] = range[0];
                            deliveredMessages.remove(middle);
                        }
                    }
                    return middle;
                }
                middle += 1;
                start = middle;
            }
        }
        // message not found and cannot extend any range, just add it
        deliveredMessages.add(middle, new int[] {messageId, messageId});
        return middle;
    }

    public static long encodeMessageKey(int messageId, int senderId) {
        // shift the senderId to the upper bits and combine with messageId
        return ((long) senderId << 31) | (messageId & 0x7FFFFFFF);
    }

    public static int getSenderId(long key) {
        // extract the upper 7 bits
        return (int) (key >>> 31);
    }

    public static int getMessageId(long key) {
        // extract the lower 31 bits
        return (int) (key & 0x7FFFFFFF);
    }

}