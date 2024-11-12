package cs451;

import cs451.FIFOURB.URB;
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

    private URB broadcaster;

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
        broadcaster.stopProcessing();
    }

    // -----------------------------------------------------------------------------------------------------------------
    // SETUP
    // -----------------------------------------------------------------------------------------------------------------

    public void setOutputPath(String outputPath) {
        logger = new Logger(outputPath);
    }

    public void startURBBroadcaster(List<Host> hosts) {
        broadcaster = new URB(this);
        broadcaster.startURBBroadcaster(hosts);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // SENDING
    // -----------------------------------------------------------------------------------------------------------------

    public void broadcastMessage(Message message) {
        if (!flagStopProcessing) {
            broadcaster.urbBroadcast(message);
            logBroadcast(message.getId());
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // LOGGING
    // -----------------------------------------------------------------------------------------------------------------

    public void logDeliver(byte senderId, int messageId) {
        logger.logDeliver(senderId, messageId);
    }

    private void logBroadcast(int messageId) {
        logger.logBroadcast(messageId);
    }

    public void flushLog() {
        logger.logWriteToFile();
        logger.closeWriter();
    }

    // -----------------------------------------------------------------------------------------------------------------
    // LOGGING
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Host host = (Host) o;
        return id == host.id && port == host.port && Objects.equals(ip, host.ip);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, ip, port);
    }
}
