package cs451.FIFOURB;

import com.sun.source.tree.Tree;
import cs451.Host;
import cs451.Message;
import cs451.PerfectLink.PerfectLink;

import java.util.*;
import java.util.concurrent.*;

public class URB {
    // host parameters
    private final Host myHost;

    private List<Host> otherHosts;

    // number of original processes (up to 128, 1 byte)
    private short N = 0;

    // my perfect link receiver
    private PerfectLink myPerfectLinkReceiver;

    // my perfect link sender
    private PerfectLink myPerfectLinkSender;

    // pending list
    // encoding of senderId and messageId as key and message as value
    private TreeMap<Long, Object[]> pending;

    // delivered map
    // keep the last delivered for each sender
    private final Map<Byte, Integer> deliveredMap = new ConcurrentHashMap<>();

    // lock for delivery
    private final Object deliveryLock = new Object();

    // thread for broadcasting
    private BebBroadcastThread broadcastThread;

    private boolean flagStopProcessing = false;

    private static final int MAX_PENDING_MESSAGES = 10;

    // create an URB host with the attribute from the host above
    public URB(Host myHost) {
        this.myHost = myHost;
    }

    // initialize the URB host with the info of all correct processes to which it will broadcast
    public void startURBBroadcaster(List<Host> otherHosts) {
        myPerfectLinkReceiver = new PerfectLink(myHost, this);
        myPerfectLinkReceiver.startPerfectLinkReceiver();
        myPerfectLinkReceiver.receiveMessages();

        this.otherHosts = otherHosts;

        myPerfectLinkSender = new PerfectLink(myHost, this);
        myPerfectLinkSender.startPerfectLinkSender();

        // calculate half of initial processes, rounding up
        N = (short) (otherHosts.size()/2);
        // System.out.println("N = " + N);

        pending = new TreeMap<>();

        // thread for broadcasting
        broadcastThread = new BebBroadcastThread(myHost, otherHosts);
        broadcastThread.start();
    }

    public void stopProcessing() {
        flagStopProcessing = true;
        broadcastThread.terminate();
        myPerfectLinkReceiver.stopProcessing();
        myPerfectLinkSender.stopProcessing();
    }

    // -----------------------------------------------------------------------------------------------------------------
    // BROADCASTING
    // -----------------------------------------------------------------------------------------------------------------

    // URB broadcast primitive
    public boolean urbBroadcast(Message message) {
        if (!flagStopProcessing && pending.size() < MAX_PENDING_MESSAGES) {
            long key = encodeMessageKey(message.getId(), message.getByteSenderId());
            // add to pending, with 0 acks
            pending.put(key, new Object[]{message, (short) 0});

            /*
            for (Long k : pending.keySet()) {
                System.out.println("Pending: " + getMessageId(k) + " from " + (getSenderId(k) + 1));
            }
            */
            bebBroadcastNew(message);
            return true;
        }
        else {
            return false;
        }
    }

    private void bebBroadcastNew(Message message) {
        if (!flagStopProcessing) {
            // System.out.println("bebBroadcast " + message.getId() + " from " + message.getSenderId());
            broadcastThread.enqueueMessage(message);
        }
    }

    private void bebBroadcastForward(Message message) {
        if (!flagStopProcessing) {
            broadcastThread.stackMessage(message);
        }
    }

    class BebBroadcastThread extends Thread {
        private final ConcurrentLinkedDeque<Message> messageQueue;
        private final List<Host> receivers;
        private final Host sender;
        private boolean exit = false;

        public BebBroadcastThread(Host sender, List<Host> receivers) {
            this.sender = sender;
            this.receivers = receivers;
            messageQueue = new ConcurrentLinkedDeque<>();
        }

        public void enqueueMessage(Message message) {
            messageQueue.addLast(message);
        }

        public void stackMessage(Message message) {
            messageQueue.addFirst(message);
        }

        @Override
        public void run() {
            Message message;
            while (!exit) {
                message = messageQueue.pollFirst();
                if (message != null) {
                    for (Host host : receivers) {
                        if (!flagStopProcessing && host != this.sender) {
                            // System.out.println("to " + host.getId());
                            myPerfectLinkSender.send(message, host);
                        }
                    }
                }
            }
        }

        public void terminate() {
            exit = true;
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // DELIVERING
    // -----------------------------------------------------------------------------------------------------------------

    public void plDeliver(Message message) {
        int messageId = message.getId();
        byte senderId = message.getByteSenderId();
        long key = encodeMessageKey(messageId, senderId);

        System.out.println("plDeliver " + messageId + " from " + (senderId + 1));
        synchronized (deliveryLock) {
            Object[] pend = pending.get(key);
            // System.out.println("Pend null? " + (pend==null));

            if (pend == null) {
                // message from sender is not yet in pending, need to add it
                LinkedList<int[]> deliveredMessages;
                Integer deliveredLast;
                boolean delivered = false;

                // check if the message has already been FIFO delivered
                deliveredLast = deliveredMap.get(senderId);
                if (deliveredLast != null && deliveredLast >= messageId) {
                    delivered = true;
                }

                // if it wasn't FIFO delivered, check if it was already pending for delivery
                if (!delivered) {
                    // the message is not yet FIFO delivered, and it was not in pending, so add it
                    pend = new Object[]{message, (short) 1};
                    pending.put(key, pend);
                    bebBroadcastForward(message);
                }
            } else {
                // message is already in pending
                // System.out.println("+1 ack");
                pend[1] = (short) ((short) pend[1] + 1);
                // System.out.println("Acks: " + pend[1]);
            }

            // check if the message has majority ack
            if (pend != null && (short) pend[1] >= N) {
                Integer deliveredLast = deliveredMap.get(senderId);
                // check if the message is the next in line
                if (deliveredLast == null || messageId == deliveredLast + 1) {
                    // if it is, FIFO deliver it
                    fifoDeliver(messageId, senderId);
                }
                // can remove from pending because it checks if message was already delivered when it gets one
                pending.remove(key);
            }
        }
    }

    private void fifoDeliver(int messageId, byte senderId) {
        // edit the last delivered
        myHost.logDeliver(senderId, messageId);

        // check if the first pending for delivery are next in line

        int nextMessage = messageId + 1;
        Long key = encodeMessageKey(nextMessage, senderId);
        Object[] pend;
        // check if the next message is in pending and has majority ack
        while ((pend = pending.get(key)) != null && (short) pend[1] >= N) {
            myHost.logDeliver(senderId, nextMessage);
            // remove from pending when delivered
            pending.remove(key);
            // increment next message
            nextMessage++;
            key = encodeMessageKey(senderId, nextMessage);
        }

        // add the last FIFO delivered message to the map for the sender
        // -1 because I increment and then check if it exists, and have to go back if it doesn't
        deliveredMap.put(senderId, (nextMessage - 1));
    }

    // -----------------------------------------------------------------------------------------------------------------
    // UTILS
    // -----------------------------------------------------------------------------------------------------------------
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
