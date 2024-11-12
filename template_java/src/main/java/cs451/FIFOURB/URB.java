package cs451.FIFOURB;

import cs451.Host;
import cs451.Message;
import cs451.PerfectLink.PerfectLink;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

public class URB {
    // host parameters
    private final byte myId;
    private final String myIp;
    private final int myPort;
    private final Host myHost;

    // number of original processes (up to 128, 1 byte)
    private short N = 0;

    // correct hosts list
    // private ConcurrentMap<Byte, Object[]> correctHosts;
    private ConcurrentLinkedQueue<PerfectLink> correctHosts;

    // my perfect link receiver
    private PerfectLink myPerfectLinkReceiver;

    // pending list
    // keep messageId as key and message object and number of acks received for it as values
    // don't need the actual hosts and hostsIds that sent the ack since perfect links can only deliver once
    // private ConcurrentHashMap<Object[], Object[]> pending;
    private ConcurrentHashMap<Short, Map<Integer, Object[]>> pending;

    // delivered map
    // keep one delivered list for each host
    private final Map<Byte, LinkedList<int[]>> deliveredMap = new ConcurrentHashMap<>();

    // create an URB host with the attribute from the host above
    public URB(Host myHost) {
        this.myId = (byte) (myHost.getId() - 1);
        this.myIp = myHost.getIp();
        this.myPort = myHost.getPort();
        this.myHost = myHost;
    }

    // initialize the URB host with the info of all correct processes to which it will broadcast
    public void startURBBroadcaster(List<Host> otherHosts) {
        this.correctHosts = new ConcurrentLinkedQueue<>();
        myPerfectLinkReceiver = new PerfectLink(myHost, this);
        myPerfectLinkReceiver.startPerfectLinkReceiver();
        myPerfectLinkReceiver.receiveMessages();

        for (Host host : otherHosts) {
            if (!host.equals(myHost)) {
                PerfectLink perfectLinkSender = new PerfectLink(myHost, this);
                perfectLinkSender.startPerfectLinkSender(host);
                correctHosts.add(perfectLinkSender);
            }
            N += 1;
        }
        // calculate half of initial processes, rounding up
        N = (short) (N/2);
        System.out.println("N = " + N);

        pending = new ConcurrentHashMap<>();
    }

    public void stopProcessing() {
        myPerfectLinkReceiver.stopProcessing();
        for (PerfectLink sender : correctHosts) {
            sender.stopProcessing();
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // BROADCASTING
    // -----------------------------------------------------------------------------------------------------------------

    // URB broadcast primitive
    public void urbBroadcast(Message message) {
        Map<Integer, Object[]> map = new HashMap<>();
        map.put(message.getId(), new Object[]{message, (short) 0});
        pending.put((short) message.getSenderId(), map);
        bebBroadcast(message);
    }

    private void bebBroadcast(Message message) {
        System.out.println("bebBroadcast " + message.getId());
        for (PerfectLink sender : correctHosts) {

            sender.send(message);
            // System.out.println("Sending message " + message.getId() + " sender " + message.getSenderId() + " to " + sender.getReceiverId());
        }
    }

    private void bebBroadcast(Message message, short senderId) {
        System.out.println("bebBroadcastSender " + message.getId() + " from " + senderId);
        for (PerfectLink sender : correctHosts) {
            if ((short) sender.getReceiverId() != senderId) {
                sender.send(message);
                // System.out.println("Sending message " + message.getId() + " sender " + message.getSenderId() + " to " + sender.getReceiverId());
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // DELIVERING
    // -----------------------------------------------------------------------------------------------------------------

    public void plDeliver(Message message) {
        int messageId = message.getId();
        short senderId = (short) message.getSenderId();
        System.out.println("plDeliver " + messageId + " - " + senderId);
        Map<Integer, Object[]> pendMap = pending.computeIfAbsent(senderId, k -> new HashMap<>());
        Object[] pend = pendMap.get(messageId);
        if (pend == null) {
            // check if it wasn't already urb-delivered
            // check here so that I can remove the pending message once I deliver it
            LinkedList<int[]> deliveredMessages;
            synchronized (deliveredMap) {
                deliveredMessages = deliveredMap.get((byte) (senderId - 1));
                if (deliveredMessages != null) {
                    if (!checkIfDelivered(deliveredMessages, messageId)) {
                        System.out.println("Not delivered");
                        // message wasn't already delivered
                        pend = new Object[]{message, (short) 1};
                        pendMap.put(messageId, pend);
                        bebBroadcast(message, senderId);
                    }
                    else {
                        System.out.println("Delivered");
                    }
                }
                else {
                    System.out.println("Not delivered");
                    // message wasn't already delivered
                    pend = new Object[]{message, (short) 1};
                    pendMap.put(messageId, pend);
                    bebBroadcast(message, senderId);
                }

            }
        }
        else {
            System.out.println("+1");
            pend[1] = (short) ((short) pend[1] + 1);
            System.out.println(pend[1]);
        }

        if (pend != null && (short) pend[1] >= N) {
            // urb-deliver when majority ack, majority is >= than half because sender can count itself
            urbDeliver(message, messageId, senderId);
            // can remove from pending because it checks if message was already delivered when it gets one
            pendMap.remove(messageId);
        }
    }

    private void urbDeliver(Message message, int messageId, short senderId) {
        System.out.println("urbDeliver " + messageId + " - " + senderId);
        LinkedList<int[]> deliveredMessages;
        // add to delivered map
        synchronized (deliveredMap) {
            deliveredMessages = deliveredMap.get((byte) (senderId - 1));
            if (deliveredMessages == null) {
                deliveredMessages = new LinkedList<>();
                deliveredMessages.add(new int[]{messageId, messageId});
                deliveredMap.put((byte) (senderId - 1), deliveredMessages);
            }
            else {
                addMessage(deliveredMessages, messageId);
            }
        }
        // deliver
        myHost.logDeliver((byte) (senderId - 1), messageId);
    }

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
}
