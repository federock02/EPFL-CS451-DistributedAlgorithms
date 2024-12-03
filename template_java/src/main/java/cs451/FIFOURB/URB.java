package cs451.FIFOURB;

import cs451.Host;
import cs451.Message;
import cs451.PerfectLink.PerfectLink;

import java.util.*;
import java.util.concurrent.*;

public class URB {
    // host parameters
    private final Host myHost;

    // used then for calculating the number for majority ack
    private short N = 0;

    // my perfect link receiver
    private PerfectLink myPerfectLinkReceiver;

    // my perfect link sender
    private PerfectLink myPerfectLinkSender;

    // pending list
    // long encoding of senderId and messageId as key and message as value
    private Map<Long, Object[]> pending;

    // delivered map
    // keep the last delivered for each sender
    private final Map<Byte, Integer> deliveredMap = new ConcurrentHashMap<>();

    // lock for delivery
    private final Object deliveryLock = new Object();

    // thread for broadcasting
    private BebBroadcastThread broadcastThread;

    // thread for delivery from PL
    private PlDeliveryThread plDeliveryThread;

    // flag for stopping at shutdown
    private boolean flagStopProcessing = false;

    // limit on size of pending list
    private static final int MAX_PENDING_MESSAGES = 100;

    // int used to force broadcast of new personal messages when pending is constantly full
    int i = 10;

    // create a URB host with the attribute from the host above
    public URB(Host myHost) {
        this.myHost = myHost;
    }

    // initialize the URB host with the info of all correct processes to which it will broadcast
    public void startURBBroadcaster(List<Host> otherHosts) {
        // calculate half of initial processes, rounding up
        N = (short) (otherHosts.size()/2);
        // System.out.println("N = " + N);

        pending = new HashMap<>();

        // thread for perfect link delivery in URB abstraction
        plDeliveryThread = new PlDeliveryThread();
        plDeliveryThread.start();

        // PL receiver
        myPerfectLinkReceiver = new PerfectLink(myHost, this);
        myPerfectLinkReceiver.startPerfectLinkReceiver();
        myPerfectLinkReceiver.receiveMessages();

        // thread for broadcasting
        broadcastThread = new BebBroadcastThread(myHost, otherHosts);
        broadcastThread.start();

        // perfect link sender
        myPerfectLinkSender = new PerfectLink(myHost, this);
        myPerfectLinkSender.startPerfectLinkSender();
    }

    // method for terminating the processing
    public void stopProcessing() {
        flagStopProcessing = true;
        if (broadcastThread != null) {
            broadcastThread.interrupt();
        }
        if (plDeliveryThread != null) {
            plDeliveryThread.interrupt();
        }
        if (myPerfectLinkReceiver != null) {
            myPerfectLinkReceiver.stopProcessing();
        }
        if (myPerfectLinkSender != null) {
            myPerfectLinkSender.stopProcessing();
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // BROADCASTING
    // -----------------------------------------------------------------------------------------------------------------

    // URB broadcast primitive
    public boolean urbBroadcast(Message message) {
        if (!flagStopProcessing && (pending.size() < MAX_PENDING_MESSAGES || i == 0)) {
            long key = encodeMessageKey(message.getId(), message.getByteSenderId());
            // add to pending, with 0 acks
            pending.put(key, new Object[]{message, (short) 0});
            i = 10;

            // System.out.println("URB broadcast " + message.getId());
            bebBroadcastNew(message);
            return true;
        }
        else {
            i--;
            return false;
        }
    }

    // broadcasting new messages
    private void bebBroadcastNew(Message message) {
        if (!flagStopProcessing && broadcastThread != null) {
            // System.out.println("bebBroadcast " + message.getId() + " from " + message.getSenderId());
            broadcastThread.enqueueNewMessage(message);
        }
    }

    // relaying other processes' messages
    private void bebBroadcastForward(Message message) {
        if (!flagStopProcessing && broadcastThread != null) {
            broadcastThread.enqueueRelayMessage(message);
        }
    }

    class BebBroadcastThread extends Thread {
        private final ConcurrentLinkedQueue<Message> messageQueue;
        private final ConcurrentLinkedQueue<Message> relayMessageQueue;
        private final List<Host> receivers;
        private final Host sender;

        public BebBroadcastThread(Host sender, List<Host> receivers) {
            this.sender = sender;
            this.receivers = receivers;
            messageQueue = new ConcurrentLinkedQueue<>();
            relayMessageQueue = new ConcurrentLinkedQueue<>();
        }

        public void enqueueNewMessage(Message message) {
            messageQueue.add(message);
        }

        public void enqueueRelayMessage(Message message) {
            relayMessageQueue.add(message);
        }

        @Override
        public void run() {
            Message message;
            while (true) {
                // prefers the messages that need to be relayed
                message = relayMessageQueue.poll();

                if (message == null) {
                    // if no message to relay, send a new one
                    message = messageQueue.poll();
                }

                if (message != null) {
                    for (Host host : receivers) {
                        if (!flagStopProcessing && !host.equals(sender)) {
                            // System.out.println("to " + host.getId());
                            myPerfectLinkSender.send(message, host);
                        }
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // DELIVERING
    // -----------------------------------------------------------------------------------------------------------------

    // the PL abstraction will call this method for signaling a PL delivery
    class PlDeliveryThread extends Thread {
        private final Queue<Message> deliveryQueue;

        int messageId;
        byte senderId;
        long key;

        public PlDeliveryThread() {
            deliveryQueue = new ConcurrentLinkedQueue<>();
        }

        // PL delivery will add to a queue of messages that will need to be processed
        public void plDeliver(Message message) {
            deliveryQueue.add(message);
        }

        @Override
        public void run() {
            Message message;
            while (true) {
                message = deliveryQueue.poll();
                if (message != null) {
                    messageId = message.getId();
                    senderId = message.getByteSenderId();
                    key = encodeMessageKey(messageId, senderId);

                    // System.out.println("plDeliver " + messageId + " from " + (senderId + 1));
                    synchronized (deliveryLock) {
                        Object[] pend = pending.get(key);
                        // System.out.println("Pend null? " + (pend==null));

                        // check if the message is pending
                        if (pend == null) {
                            // check if the message has already been FIFO delivered
                            Integer deliveredLast = deliveredMap.get(senderId);
                            boolean delivered = deliveredLast != null && deliveredLast >= messageId;

                            if (!delivered) {
                                // the message is not yet FIFO delivered, and it was not in pending, so add it
                                pend = new Object[]{message, (short) 1};
                                pending.put(key, pend);
                                // then relay it to the other processes
                                bebBroadcastForward(message);
                            }
                        } else {
                            // message is already in pending
                            // increase the acknowledgment number
                            // System.out.println("+1 ack");
                            pend[1] = (short) ((short) pend[1] + 1);
                            // System.out.println("Acks: " + pend[1]);
                        }

                        // check if the message has majority ack
                        // check again if pend is null because message could have been already FIFO delivered
                        if (pend != null && (short) pend[1] >= N) {
                            Integer deliveredLast = deliveredMap.get(senderId);
                            // check if the message is the next in line
                            // it has to be either the first or the following of the last delivered
                            if (messageId == 1 || (deliveredLast != null && messageId == deliveredLast + 1)) {
                                // if it is, FIFO deliver it
                                fifoDeliver(messageId, senderId);
                                // can remove from pending because it checks if message was already delivered when it gets one
                                pending.remove(key);
                            }
                        }
                    }
                }
            }
        }
    }

    public void plDeliver(Message message) {
        if (plDeliveryThread != null) {
            plDeliveryThread.plDeliver(message);
        }
    }

    private void fifoDeliver(int messageId, byte senderId) {
        // edit the last delivered
        myHost.logDeliver(senderId, messageId);
        // System.out.println("First FIFO delivery: " + messageId + " from " + (senderId + 1));
        // System.out.println("Key: " + encodeMessageKey(messageId, senderId));

        // check if the first pending for delivery are next in line

        int nextMessage = messageId + 1;
        Long key = encodeMessageKey(nextMessage, senderId);
        // System.out.println("Trying next, key: " + key);
        // System.out.println(pending.get(key) == null);
        Object[] pend;

        // check if the next message is in pending and has majority ack
        while ((pend = pending.get(key)) != null && (short) pend[1] >= N) {
            myHost.logDeliver(senderId, nextMessage);
            // System.out.println("Also delivered: " + nextMessage + " from " + (senderId + 1));
            // remove from pending when delivered
            pending.remove(key);
            // increment next message
            nextMessage++;
            key = encodeMessageKey(nextMessage, senderId);
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
}
