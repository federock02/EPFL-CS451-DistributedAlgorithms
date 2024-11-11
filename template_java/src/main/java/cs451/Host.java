package cs451;

import cs451.PerfectLink.PerfectLink;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.nio.ByteBuffer;

// single process in the system
public class Host {
    // host attributes
    private int id;
    private String ip;
    private int port = -1;

    private Map<Integer, PerfectLink> p2pLinks = new HashMap();
    private PerfectLink p2pReceiver;

    private static final String IP_START_REGEX = "/";
    private static final int MAX_NUM_MESSAGES_PER_PACKAGE = 8;

    private boolean flagStopProcessing = false;

    // logger
    private Logger logger;

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

    // -----------------------------------------------------------------------------------------------------------------
    // UTILS
    // -----------------------------------------------------------------------------------------------------------------

    public void stopProcessing() {
        flagStopProcessing = true;
        for (PerfectLink perfectLink : p2pLinks.values()) {
            perfectLink.stopProcessing();
        }
        if (p2pReceiver != null) {
            p2pReceiver.stopProcessing();
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // SETUP
    // -----------------------------------------------------------------------------------------------------------------

    public void setOutputPath(String outputPath) {
        logger = new Logger(outputPath);
    }

    public void startSender(Host receiver) {
        PerfectLink p2pLink = new PerfectLink(this);
        p2pLinks.put(receiver.getId(), p2pLink);
        p2pLink.startPerfectLinkSender(receiver, MAX_NUM_MESSAGES_PER_PACKAGE);
    }

    public void startReceiver() {
        p2pReceiver = new PerfectLink(this);
        p2pReceiver.startPerfectLinkReceiver();
    }

    // -----------------------------------------------------------------------------------------------------------------
    // SENDING
    // -----------------------------------------------------------------------------------------------------------------

    public void sendMessage(Message message, Host receiver) {
        if (!flagStopProcessing) {
            p2pLinks.get(receiver.getId()).send(message);
            logSend(message.getId());
        }
    }

    public void sendMessages(ConcurrentLinkedQueue<Message> messagesToSend, Host receiver) {
        for (Message message : messagesToSend) {
            sendMessage(message, receiver);
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // RECEIVING
    // -----------------------------------------------------------------------------------------------------------------

    // logic for receiving messages, by listening for incoming messages on UDP socket
    public void receiveMessages() {
        p2pReceiver.receiveMessages();
    }

    // -----------------------------------------------------------------------------------------------------------------
    // LOGGING
    // -----------------------------------------------------------------------------------------------------------------

    public void logDeliver(byte senderId, int messageId) {
        logger.logDeliver(senderId, messageId);
    }

    private void logSend(int messageId) {
        logger.logSend(messageId);
    }

    public void flushLog() {
        logger.logWriteToFile();
        logger.closeWriter();
    }
}
