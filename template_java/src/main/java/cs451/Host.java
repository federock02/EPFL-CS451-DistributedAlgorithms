package cs451;

//import cs451.FIFOURB.URB;
import cs451.LatticeAgreement.LatticeAgreement;

import javax.print.attribute.IntegerSyntax;
import java.net.*;
import java.util.*;

// single process in the system
public class Host {
    // host attributes
    private int id;
    private String ip;
    private int port = -1;

    private Set<Integer> proposalSet;

    private LatticeAgreement latticeAgreement;

    private static final String IP_START_REGEX = "/";

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

    public byte getByteId() {
        return (byte) (id - 1);
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
        latticeAgreement.stopProcessing();
    }

    // -----------------------------------------------------------------------------------------------------------------
    // SETUP
    // -----------------------------------------------------------------------------------------------------------------

    public void setOutputPath(String outputPath) {
        logger = new Logger(outputPath);
    }

    public void startLatticeAgreement(List<Host> hosts, int p, int vs, int ds) {
        this.latticeAgreement = new LatticeAgreement(this);
        this.latticeAgreement.startLatticeAgreement(hosts, p, vs, ds);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // SENDING
    // -----------------------------------------------------------------------------------------------------------------

    public void propose(Integer[] proposal) {
        proposalSet = new HashSet<>();
        proposalSet.addAll(List.of(proposal));
        this.latticeAgreement.propose(proposalSet);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // LOGGING
    // -----------------------------------------------------------------------------------------------------------------

    public void logDecide(Set<Integer> values) {
        logger.logDecide(values);
    }

    public void flushLog() {
        logger.logWriteToFile();
        logger.closeWriter();
    }

    // -----------------------------------------------------------------------------------------------------------------
    // UTILS
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
