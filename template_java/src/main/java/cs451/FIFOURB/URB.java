package cs451.FIFOURB;

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
    private ConcurrentHashMap<Long, Object[]> pending;

    // delivered map
    // keep one delivered list for each host
    private final Map<Byte, LinkedList<int[]>> deliveredMap = new ConcurrentHashMap<>();

    // thread for broadcasting
    private BebBroadcastThread broadcastThread;

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

        pending = new ConcurrentHashMap<>();

        // thread for broadcasting
        broadcastThread = new BebBroadcastThread(myHost, otherHosts);
        broadcastThread.start();
    }

    public void stopProcessing() {
        myPerfectLinkReceiver.stopProcessing();
        myPerfectLinkSender.stopProcessing();
    }

    // -----------------------------------------------------------------------------------------------------------------
    // BROADCASTING
    // -----------------------------------------------------------------------------------------------------------------

    // URB broadcast primitive
    public void urbBroadcast(Message message) {
        long key = encodeMessageKey(message.getId(), message.getByteSenderId());
        // add to pending, with 0 acks
        pending.put(key, new Object[]{message, (short) 0});

        /*
        for (Long k : pending.keySet()) {
            System.out.println("Pending: " + getMessageId(k) + " from " + (getSenderId(k) + 1));
        }
        */
        bebBroadcast(message);
    }

    private void bebBroadcast(Message message) {
        // System.out.println("bebBroadcast " + message.getId() + " from " + message.getSenderId());
        broadcastThread.enqueueMessage(message);
    }

    class BebBroadcastThread extends Thread {
        private Queue<Message> messageQueue;
        private List<Host> receivers;
        private Host sender;

        public BebBroadcastThread(Host sender, List<Host> receivers) {
            this.sender = sender;
            this.receivers = receivers;
            messageQueue = new ConcurrentLinkedQueue<>();
        }

        public void enqueueMessage(Message message) {
            messageQueue.add(message);
        }

        @Override
        public void run() {
            Message message;
            while (true) {
                message = messageQueue.poll();
                if (message != null) {
                    for (Host host : receivers) {
                        if (host != this.sender) {
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

    public void plDeliver(Message message) {
        int messageId = message.getId();
        byte senderId = message.getByteSenderId();
        long key = encodeMessageKey(messageId, senderId);

        // System.out.println("plDeliver " + messageId + " from " + (senderId + 1));

        Object[] pend = pending.get(key);
        // System.out.println("Pend null? " + (pend==null));

        if (pend == null) {
            // message from sender is not yet in pending, need to add it
            // check if it wasn't already urb-delivered
            // check here so that I can remove the pending message once I deliver it
            LinkedList<int[]> deliveredMessages;
            synchronized (deliveredMap) {
                deliveredMessages = deliveredMap.get(senderId);
                if (deliveredMessages != null) {
                    // previously received messages from the senderId
                    if (!checkIfDelivered(deliveredMessages, messageId)) {
                        // System.out.println("Not delivered");
                        // message wasn't already delivered
                        // add to pending with ack = 1
                        pend = new Object[]{message, (short) 1};
                        pending.put(key, pend);
                        bebBroadcast(message);
                    }
                }
                else {
                    // never received any message from the same sender previously
                    // so message was not delivered
                    // System.out.println("Not delivered");
                    pend = new Object[]{message, (short) 1};
                    pending.put(key, pend);
                    bebBroadcast(message);
                }
            }
        }
        else {
            // message is already in pending
            // System.out.println("+1 ack");
            pend[1] = (short) ((short) pend[1] + 1);
            // System.out.println("Acks: " + pend[1]);
        }

        if (pend != null && (short) pend[1] >= N) {
            // urb-deliver when majority ack, majority is >= than half because sender can count itself
            urbDeliver(message, messageId, senderId);
            // can remove from pending because it checks if message was already delivered when it gets one
            pending.remove(key);
        }
    }

    private void urbDeliver(Message message, int messageId, byte senderId) {
        // System.out.println("urbDeliver " + messageId + " - " + (senderId + 1));
        LinkedList<int[]> deliveredMessages;
        // add to delivered map
        synchronized (deliveredMap) {
            deliveredMessages = deliveredMap.get(senderId);
            if (deliveredMessages == null) {
                deliveredMessages = new LinkedList<>();
                deliveredMessages.add(new int[]{messageId, messageId});
                deliveredMap.put(senderId, deliveredMessages);
            }
            else {
                addMessage(deliveredMessages, messageId);
            }
        }
        // deliver
        myHost.logDeliver(senderId, messageId);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // UTILS
    // -----------------------------------------------------------------------------------------------------------------

    private boolean checkIfDelivered(LinkedList<int[]> deliveredMessages, int messageId) {
        int start = 0;
        int end = deliveredMessages.size() - 1;
        int middle;

        // binary search to find if a message has been delivered in the compressed structure
        while (start <= end) {
            middle = (start + end) / 2;

            int[] range = deliveredMessages.get(middle);
            if (range[0] <= messageId && messageId <= range[1]) {
                // message is in one of the delivered ranges
                return true;
            }
            else if (range[0] > messageId) {
                // lower bound of range is higher than message id
                end = middle - 1;
            }
            else {
                // upper bound of range is lower than message id
                start = middle + 1;
            }
        }
        // message not found
        return false;
    }

    private void addMessage(LinkedList<int[]> deliveredMessages, int messageId) {
        int start = 0;
        int end = deliveredMessages.size() - 1;
        int middle = 0;

        // binary search to find if a message has been delivered in the compressed structure
        while (start <= end) {
            middle = (start + end) / 2;

            int[] range = deliveredMessages.get(middle);
            if (range[0] <= messageId && messageId <= range[1]) {
                // message is in one of the delivered ranges
                return;
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
                    return;
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
                    return;
                }
                middle += 1;
                start = middle;
            }
        }
        // message not found and cannot extend any range, just add it
        deliveredMessages.add(middle, new int[] {messageId, messageId});
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
