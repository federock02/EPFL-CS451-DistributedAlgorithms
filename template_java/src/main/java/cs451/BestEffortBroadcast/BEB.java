package cs451.BestEffortBroadcast;

import cs451.Host;
import cs451.Message;
import cs451.PerfectLink.PerfectLink;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class BEB {
    // host parameters
    private final Host myHost;

    // my perfect link receiver
    private PerfectLink myPerfectLinkReceiver;

    // my perfect link sender
    private PerfectLink myPerfectLinkSender;

    // thread for broadcasting
    private BebBroadcastThread broadcastThread;

    // flag for stopping at shutdown
    private boolean flagStopProcessing = false;

    // create a BEB host with the attribute from the host above
    public BEB(Host myHost) {
        this.myHost = myHost;
    }

    // initialize the URB host with the info of all correct processes to which it will broadcast
    public void startBEBBroadcaster(List<Host> otherHosts) {
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

    // broadcasting new messages
    private void bebBroadcast(Message message) {
        if (!flagStopProcessing && broadcastThread != null) {
            // System.out.println("bebBroadcast " + message.getId() + " from " + message.getSenderId());
            broadcastThread.enqueueNewMessage(message);
        }
    }

    class BebBroadcastThread extends Thread {
        private final ConcurrentLinkedQueue<Message> messageQueue;
        private final List<Host> receivers;
        private final Host sender;

        public BebBroadcastThread(Host sender, List<Host> receivers) {
            this.sender = sender;
            this.receivers = receivers;
            messageQueue = new ConcurrentLinkedQueue<>();
        }

        public void enqueueNewMessage(Message message) {
            messageQueue.add(message);
        }

        @Override
        public void run() {
            Message message;
            while (true) {
                message = messageQueue.poll();

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

    // plDelvery calls bebDelivery
    public void bebDeliver(Message message) {

    }
}
