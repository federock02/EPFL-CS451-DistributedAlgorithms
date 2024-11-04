package cs451.PerfectLink;

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
    private final Host myHost;

    // receiver parameters
    private byte receiverId;
    private String receiverIp;
    private int receiverPort;

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
    // private ThreadPoolExecutor ackListener;

    // flag used to manage termination signals
    private boolean flagStopProcessing = false;

    // message sending parameters
    // message queue for uniting messages in packages of maxNumPerPackage
    private final Queue<Message> messagePackage = new LinkedList<>();
    // maximum number of messages per package
    private int maxNumPerPackage = 8;
    // timeout for sending a package, even if it was not filled with maxNumPerPackage messages
    private static final long SEND_TIMER = 150;
    // amount of times each message will be resent before dropping it
    private static final byte RESEND_TIMES = 10;
    // lock for managing access to queue of messages to send
    private final Object queueLock = new Object();
    // executor that manages the timeout for sending the package
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    // task for sending the package
    private ScheduledFuture<?> timeoutTask;
    // structure for messages that haven't been acknowledged yet
    private final Map<Integer, Object[]> unacknowledgedMessages = new ConcurrentHashMap<>();
    // messages list pool (avoiding constant creation and destruction with garbage collector)
    private final Queue<Queue<Message>> listPool = new LinkedList<>();

    // structure for already delivered messages in receiving phase
    // private final Map<Byte, Set<Integer>> deliveredMap = new ConcurrentHashMap<>();
    private final Map<Byte, LinkedList<int[]>> deliveredMap = new ConcurrentHashMap<>();
    // pool for ByteBuffers to avoid frequent allocations when receiving
    ArrayBlockingQueue<ByteBuffer> byteBufferPoolReceiving = new ArrayBlockingQueue<>(10);
    // receiving buffer size
    private static final int RECEIVING_BUFF_SIZE = 2048;
    // pool for ByteBuffers to avoid frequent allocations when sending acks
    ArrayBlockingQueue<ByteBuffer> byteBufferPoolAcks = new ArrayBlockingQueue<>(10);
    // ack buffer size
    private static final int ACK_BUFF_SIZE = 5;
    // pool of DatagramPackets for sending acks
    ArrayBlockingQueue<DatagramPacket> datagramPacketsPool = new ArrayBlockingQueue<>(10);

    // flag for sending done
    private boolean sendingDone = false;

    // constructor for perfect link
    public PerfectLink(Host myHost) {
        this.myId = (byte) (myHost.getId() - 1);
        this.myIp = myHost.getIp();
        this.myPort = myHost.getPort();
        this.myHost = myHost;
    }

    // starting perfect link sender, with personalized maxNumPerPackage
    public void startPerfectLinkSender(Host receiver, int maxNumPerPackage) {
        startPerfectLinkSender(receiver);
        this.maxNumPerPackage = maxNumPerPackage;
    }

    // starting perfect link sender, with default maxNumPerPackage
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

        /*
        // bounded queues with a maximum of 100 and 50 pending tasks
        BlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>(5);
        BlockingQueue<Runnable> ackQueue = new LinkedBlockingQueue<>(5);
        threadPool = new ThreadPoolExecutor(
                0, 2, 5L, TimeUnit.SECONDS, taskQueue,
                new ThreadPoolExecutor.CallerRunsPolicy());
        ackListener = new ThreadPoolExecutor(
                0, 2, 5L, TimeUnit.SECONDS, ackQueue,
                new ThreadPoolExecutor.CallerRunsPolicy());
        */

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
        /*
        if (ackListener != null) {
            ackListener.shutdown();
        }
        */
        if (mySocket != null && !mySocket.isClosed()) {
            try {
                if (threadPool != null && !threadPool.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                    threadPool.shutdownNow();
                }
                /*
                if (ackListener != null && !ackListener.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                    ackListener.shutdownNow();
                }
                */
            } catch (InterruptedException e) {
                if (threadPool != null) {
                    threadPool.shutdownNow();
                }
                /*
                if (ackListener != null) {
                    ackListener.shutdownNow();
                }
                */
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
        // myHost.logTesting("Sending " + message.getId());
        synchronized (queueLock) {
            messagePackage.add(message);
            unacknowledgedMessages.put(message.getId(), new Object[]{message, receiverId, RESEND_TIMES, System.currentTimeMillis()});
            if (messagePackage.size() >= maxNumPerPackage) {
                Queue<Message> toSend = borrowList();
                toSend.addAll(messagePackage);
                sendMessagesBatch(toSend);
                messagePackage.clear();
                resetTimeout();
            } else {
                if (timeoutTask == null || timeoutTask.isCancelled()) {
                    timeoutTask = scheduler.schedule(() -> {
                        Queue<Message> toSend = borrowList();
                        toSend.addAll(messagePackage);
                        sendMessagesBatch(toSend);
                        messagePackage.clear();
                    }, SEND_TIMER, TimeUnit.MILLISECONDS);
                }
            }
        }
        while (unacknowledgedMessages.size() >= 500) {
            try {
                sleep(20);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // resend primitive, implements the same logic as sending
    public void resend(Message message) {
        // System.out.println("Resending " + message.getId());
        // myHost.logTesting("Resending " + message.getId());
        synchronized (queueLock) {
            messagePackage.add(message);
            if (messagePackage.size() >= maxNumPerPackage) {
                Queue<Message> toSend = borrowList();
                toSend.addAll(messagePackage);
                sendMessagesBatch(toSend);
                messagePackage.clear();
                resetTimeout();
            } else {
                if (timeoutTask == null || timeoutTask.isCancelled()) {
                    timeoutTask = scheduler.schedule(() -> {
                        Queue<Message> toSend = borrowList();
                        toSend.addAll(messagePackage);
                        sendMessagesBatch(toSend);
                        messagePackage.clear();
                    }, SEND_TIMER, TimeUnit.MILLISECONDS);
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
                //System.out.println("Sending " + message.getId());
                byte[] serializedMessage = message.serialize();

                buffer.putInt(serializedMessage.length);

                buffer.put(serializedMessage);
                //System.out.println("Sent message: " + message.getId() + " to " + receiver.getIp() + ":" + receiver.getPort());
            }
        }
        returnList(messagePackage);
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
        new Thread(() -> {
            // will stop only if sending is done and the queue of unacknowledged messages is empty
            while (!sendingDone || !unacknowledgedMessages.isEmpty()) {
                try {
                    // wait before sending again, with adaptive timeout
                    long dynamicTimeout = calculateTimeout();
                    // System.out.println((dynamicTimeout + 100));
                    sleep(dynamicTimeout + 100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                for (Integer messageId : unacknowledgedMessages.keySet()) {
                    Object[] messagePack = unacknowledgedMessages.get(messageId);
                    // threadPool.submit(() -> resend((Message) messagePack[0]));
                    // System.out.println("Resend for: " + messageId);
                    byte counter = (byte) messagePack[2];
                    messagePack[2] = (byte) (counter - 1);
                    if ((byte) messagePack[2] == 0) {
                        resend(new Message((Message) messagePack[0]));
                        unacknowledgedMessages.remove(messageId);
                    }
                    else {
                        resend((Message) messagePack[0]);
                    }
                }
            }
        }).start();
    }

    // adaptive timeout calculation
    private long calculateTimeout() {
        long timeout = (long) (estimatedRTT + 4 * devRTT);
        return Math.min(Math.max(timeout, 100), 500);
    }

    // logic for receiving acknowledgments from receivers
    public void listenForAcks() {
        DatagramPacket packet = new DatagramPacket(new byte[5], 5);
        ByteBuffer byteBuffer = ByteBuffer.allocate(5);
        new Thread(() -> {
            try {
                // byte[] buffer = new byte[5];
                while (true) {
                    // DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
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

                    // ByteBuffer byteBuffer = ByteBuffer.wrap(packet.getData());
                    byteBuffer.clear();
                    byteBuffer.put(packet.getData());
                    byteBuffer.flip();
                    int messageId = byteBuffer.getInt();
                    byte receiver = byteBuffer.get();

                    // skip processing if the receiver doesn't match
                    if (receiver != receiverId) {
                        continue;
                    }

                    long currentTime = System.currentTimeMillis();
                    if (unacknowledgedMessages.containsKey(messageId)) {
                        Object[] messagePack = unacknowledgedMessages.get(messageId);
                        unacknowledgedMessages.remove(messageId);
                        long sendTime = (long) messagePack[3];
                        long sampleRTT = currentTime - sendTime;

                        // update estimated RTT and deviation
                        this.estimatedRTT = (1 - this.alpha) * this.estimatedRTT + this.alpha * sampleRTT;
                        this.devRTT = (1 - this.beta) * this.devRTT + this.beta * Math.abs(sampleRTT - this.estimatedRTT);
                    }
                    // System.out.println("Received ack for message " + messageId);
                    // myHost.logTesting("Received ack for message " + messageId);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    // -----------------------------------------------------------------------------------------------------------------
    // RECEIVING
    // -----------------------------------------------------------------------------------------------------------------

    /*
    // receiving thread
    private class ReceivingThread extends Thread {
        BlockingQueue<DatagramPacket> messageQueue;

        public ReceivingThread(BlockingQueue<DatagramPacket> messageQueue) {
            this.messageQueue = messageQueue;
        }

        public void run() {
            try {
                // buffer for incoming messages
                byte[] buffer = new byte[2048];

                while (!flagStopProcessing) {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    try {
                        mySocket.receive(packet);
                        if (!flagStopProcessing) {
                            //System.out.println("Putting new package");
                            messageQueue.put(packet);
                        }
                    } catch (SocketException e) {
                        if (flagStopProcessing) {
                            // socket is closed during shutdown, exit gracefully
                            break;
                        } else {
                            // unexpected socket exception
                            e.printStackTrace();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public class AckThread extends Thread {
        BlockingQueue<DatagramPacket> messageQueue;

        public AckThread(BlockingQueue<DatagramPacket> messageQueue) {
            this.messageQueue = messageQueue;
        }

        public void run() {
            while (!flagStopProcessing) {
                try {
                    DatagramPacket packet = messageQueue.take();
                    InetAddress senderAddress = packet.getAddress();
                    int senderPort = packet.getPort();

                    ByteBuffer byteBuffer = ByteBuffer.wrap(packet.getData());
                    int packetLength = packet.getLength();

                    if (!flagStopProcessing) {
                        // process
                        // threadPool.submit(() -> processMessage(byteBuffer, packetLength, senderAddress, senderPort));
                        processMessage(byteBuffer, packetLength, senderAddress, senderPort);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
    */

    // logic for receiving messages, by listening for incoming messages on UDP socket
    public void receiveMessages() {
        /*
        BlockingQueue<DatagramPacket> messageQueue = new LinkedBlockingQueue<>(100);
        //System.out.println("Receiver " + this.myIp + " : " + this.myPort);
        ReceivingThread receivingThread1 = new ReceivingThread(messageQueue);
        ReceivingThread receivingThread2 = new ReceivingThread(messageQueue);
        receivingThread1.start();
        receivingThread2.start();

        AckThread ackThread1 = new AckThread(messageQueue);
        AckThread ackThread2 = new AckThread(messageQueue);
        AckThread ackThread3 = new AckThread(messageQueue);
        ackThread1.start();
        ackThread2.start();
        ackThread3.start();
        */

        threadPool.submit(() -> {
            try {
                DatagramPacket packet = new DatagramPacket(new byte[RECEIVING_BUFF_SIZE], RECEIVING_BUFF_SIZE);

                while (!flagStopProcessing) {
                    try {
                        mySocket.receive(packet);
                        InetAddress senderAddress = packet.getAddress();
                        int senderPort = packet.getPort();

                        // ByteBuffer byteBuffer = ByteBuffer.wrap(packet.getData());
                        ByteBuffer byteBuffer = byteBufferPoolReceiving.poll();
                        if (byteBuffer == null) {
                            // if the pool is empty, allocate a new one
                            byteBuffer = ByteBuffer.allocate(RECEIVING_BUFF_SIZE);
                        } else {
                            // clear the buffer before reuse
                            byteBuffer.clear();
                        }

                        int packetLength = packet.getLength();

                        // Wrap the packet data into the ByteBuffer
                        byteBuffer.put(packet.getData(), 0, packetLength);
                        byteBuffer.flip();

                        if (!flagStopProcessing) {
                            // process
                            // threadPool.submit(() -> processMessage(byteBuffer, packetLength, senderAddress, senderPort));
                            processMessage(byteBuffer, packetLength, senderAddress, senderPort);
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
        });
    }

    private void processMessage(ByteBuffer byteBuffer, int packetLength, InetAddress senderAddress, int senderPort) {
        int i;
        while (byteBuffer.position() < packetLength) {
            //System.out.println("Processing package");
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
            // System.out.println("Got message " + messageId);
            byte senderId = message.getByteSenderId();
            // int payloadAsInt = ByteBuffer.wrap(message.getPayload()).getInt();

            threadPool.submit(() -> sendAck(messageId, senderAddress, senderPort));
            // sendAck(messageId, senderAddress, senderPort);

            // check if the message is already delivered from sender
            // Set<Integer> deliveredMessages;
            LinkedList<int[]> deliveredMessages;
            synchronized (deliveredMap) {
                if ((deliveredMessages = deliveredMap.get(senderId)) != null) {
                /*
                if (!deliveredMessages.contains(messageId)) {
                    // add to delivered
                    deliveredMessages.add(messageId);

                    // threadPool.submit(() -> myHost.logDeliver(senderId, messageId));
                    myHost.logDeliver(senderId, messageId);
                    // System.out.println("Delivered message " + messageId + " from " + (senderId + 1));
                }
                */
                    i = -1;
                    i = addMessage(deliveredMessages, messageId);
                    // System.out.println(senderId + " list size: " + deliveredMessages.size());
                    if (i != -1) {
                        // message wasn't already delivered
                        // deliveredMessages.set(messageId);

                        threadPool.submit(() -> myHost.logDeliver(senderId, messageId));
                        // myHost.logDeliver(senderId, messageId);
                        // System.out.println("Delivered message " + messageId + " from " + (senderId + 1));
                    }
                } else {
                    // never received from sender, add to delivered and add sender
                    // deliveredMessages = ConcurrentHashMap.newKeySet();
                    deliveredMessages = new LinkedList<>();

                    // add to delivered
                    deliveredMessages.add(new int[] {messageId, messageId});
                    deliveredMap.put(senderId, deliveredMessages);
                    // correction since messages ids start from 1

                    // threadPool.submit(() -> myHost.logDeliver(senderId, messageId));
                    myHost.logDeliver(senderId, messageId);
                    // System.out.println("Delivered message " + messageId + " from " + (senderId + 1));
                }
            }
        }

        // give back the byte buffer
        byteBufferPoolReceiving.offer(byteBuffer);
    }

    // sending ack for received message
    private void sendAck(int messageId, InetAddress senderAddress, int senderPort) {
        ByteBuffer byteBuffer = byteBufferPoolAcks.poll();
        if (byteBuffer == null) {
            // if the pool is empty, allocate a new one
            byteBuffer = ByteBuffer.allocate(5);
        } else {
            // clear the buffer before reuse
            byteBuffer.clear();
        }

        // convert messageId and myId to bytes
        byteBuffer.putInt(messageId);
        byteBuffer.put(this.myId);

        byte[] byteAckData = byteBuffer.array();

        byteBufferPoolAcks.offer(byteBuffer);

        // System.out.println("Acknowledged message: " + messageId + " to " + senderAddress + ":" + senderPort);

        // DatagramPacket ackPacket = new DatagramPacket(byteAckData, byteAckData.length, senderAddress, senderPort);
        DatagramPacket ackPacket = datagramPacketsPool.poll();
        if (ackPacket == null) {
            ackPacket = new DatagramPacket(byteAckData, byteAckData.length, senderAddress, senderPort);
        } else {
            // Reuse the packet by updating its data, address, and port
            ackPacket.setData(byteAckData);
            ackPacket.setAddress(senderAddress);
            ackPacket.setPort(senderPort);
        }

        if (!flagStopProcessing) {
            try {
                mySocket.send(ackPacket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        datagramPacketsPool.offer(ackPacket);
    }

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
                // upper bound of range is higher than message id
                if (range[1] == messageId - 1) {
                    // message id can extend the range by 1 on the right
                    range[1] = messageId;
                    if (middle < deliveredMessages.size() - 1) {
                        // getting following range to check if the two adjacent ranges can be compressed
                        int[] range2 = deliveredMessages.get(middle + 1);
                        if (range2[0] == range[1] + 1) {
                            range2[0] = range[0];
                            deliveredMessages.remove(middle);
                        }
                    }
                    return middle;
                }
                start = middle + 1;
            }
        }
        // message not found and cannot extend any range, just add it
        deliveredMessages.add(middle, new int[] {messageId, messageId});
        return middle;
    }
}
