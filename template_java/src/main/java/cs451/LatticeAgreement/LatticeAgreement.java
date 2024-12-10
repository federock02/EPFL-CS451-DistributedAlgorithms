package cs451.LatticeAgreement;

import cs451.BestEffortBroadcast.BEB;
import cs451.Host;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LatticeAgreement {
    private Host myHost;

    // max number of accepted failures
    private byte f;
    // number of proposals
    private int p;
    // max number of proposed values
    private int vs;
    // max number of distinct elements across proposals
    private int ds;

    private Map<Integer, LatticeAgreementInstance> instances;

    // beb broadcaster
    private BEB broadcaster;

    public LatticeAgreement(Host host) {
        this.myHost = host;
    }

    public void startLatticeAgreement(List<Host> hosts, int p, int vs, int ds) {
        this.p = p;
        this.vs = vs;
        this.ds = ds;

        this.instances = new ConcurrentHashMap<>();

        broadcaster = new BEB(this.myHost);
        broadcaster.startBEBBroadcaster(hosts);

        this.f = (byte) ((hosts.size() - 1) / 2);
    }

    private static class LatticeAgreementInstance {
        boolean active;
        byte acks;
        byte nacks;
        byte activeProposal;

        private Set<String> proposedValues;
        private Set<String> acceptedValues;

        public LatticeAgreementInstance() {
            this.active = false;
            this.acks = 0;
            this.nacks = 0;
            this.activeProposal = 0;
            this.proposedValues = new HashSet<>();
            this.acceptedValues = new HashSet<>();
        }

    }

    // -----------------------------------------------------------------------------------------------------------------
    // PROPOSER
    // -----------------------------------------------------------------------------------------------------------------
    public void propose(int instanceId, Set<Integer> proposal) {

    }

    // -----------------------------------------------------------------------------------------------------------------
    // ACCEPTOR
    // -----------------------------------------------------------------------------------------------------------------

    // -----------------------------------------------------------------------------------------------------------------
    // UTILS
    // -----------------------------------------------------------------------------------------------------------------
    public static long encodeMessageKey(int messageId, int senderId) {
        // shift the senderId to the upper bits and combine with messageId
        return ((long) senderId << 31) | (messageId & 0x7FFFFFFF);
    }
}
