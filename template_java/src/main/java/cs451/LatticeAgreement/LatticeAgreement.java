package cs451.LatticeAgreement;

import cs451.BestEffortBroadcast.BEB;
import cs451.Host;
import cs451.Message;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

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

    // map that holds info for each instance of lattice agreement
    private Map<Integer, LatticeAgreementInstance> instances;
    // decided instances
    private Set<Integer> decided;

    int messageId = 1;

    // beb broadcaster
    private BEB broadcaster;

    private messageDeliveryThread deliveryThread;

    private enum MessageType {
        NACK((byte) 0),
        ACK((byte) 1),
        PROPOSAL((byte) 2);

        private final byte code;

        MessageType(byte code) {
            this.code = code;
        }

        public byte getCode() {
            return this.code;
        }

        public static MessageType fromCode(byte code) {
            for (MessageType type : values()) {
                if (type.code == code) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown MessageType code: " + code);
        }
    }

    public LatticeAgreement(Host host) {
        this.myHost = host;
    }

    public void startLatticeAgreement(List<Host> hosts, int p, int vs, int ds) {
        this.p = p;
        this.vs = vs;
        this.ds = ds;

        deliveryThread = new messageDeliveryThread();
        deliveryThread.start();

        this.instances = new ConcurrentHashMap<>();
        this.decided = new HashSet<>();

        broadcaster = new BEB(this, this.myHost);
        broadcaster.startBEBBroadcaster(hosts);

        this.f = (byte) ((hosts.size() - 1) / 2);
    }

    private static class LatticeAgreementInstance {
        boolean active;
        byte acks;
        byte nacks;
        byte activeProposal;

        private Set<Integer> proposedValues;
        private Set<Integer> acceptedValues;

        public LatticeAgreementInstance() {
            this.active = false;
            this.acks = 0;
            this.nacks = 0;
            this.activeProposal = 0;
            this.proposedValues = new HashSet<>();
            this.acceptedValues = new HashSet<>();
        }

    }

    public void deliverMessage(Message message) {
        deliveryThread.addMessage(message);
    }

    private class messageDeliveryThread extends Thread {
        private final Queue<Message> queue;

        public messageDeliveryThread() {
            queue = new ConcurrentLinkedQueue<>();
        }

        @Override
        public void run() {
            Message message;
            while (true) {
                message = queue.poll();
                if (message != null) {
                    processMessage(message);
                }
            }
        }

        public void addMessage(Message message) {
            queue.add(message);
        }

        private void processMessage(Message message) {
            byte[] payload = message.getPayload();
            int senderId = message.getSenderId();

            if (senderId != myHost.getId()) {
                ByteBuffer buffer = ByteBuffer.wrap(payload);
                MessageType type = MessageType.fromCode(buffer.get());
                int instanceId = buffer.getInt();
                byte activeProposal = buffer.get();

                if (type.equals(MessageType.ACK)) {
                    // if it's an ack I do not care about the proposal
                    ack(instanceId, activeProposal);
                }
                else {
                    // get the proposal set
                    Set<Integer> proposal = new HashSet<>();
                    while (buffer.remaining() >= 4) {
                        proposal.add(buffer.getInt());
                    }

                    if (type.equals(MessageType.NACK)) {
                        nack(instanceId, activeProposal, proposal);
                    }
                    else {
                        receiveProposal(instanceId, activeProposal, proposal);
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // PROPOSER
    // -----------------------------------------------------------------------------------------------------------------
    public void propose(int instanceId, Set<Integer> proposal) {
        LatticeAgreementInstance instance = instances.computeIfAbsent(instanceId, id -> new LatticeAgreementInstance());

        instance.proposedValues = new HashSet<>(proposal);
        instance.active = true;


        // broadcasting through the beb abstraction
        // broadcasts a message of type PROPOSAL
        Message message = new Message(messageId, packageProposal(MessageType.PROPOSAL.getCode(),
                instanceId, instance.activeProposal, proposal), myHost.getId());
        broadcaster.bebBroadcast(message);

        // adding own proposal
        deliveryThread.addMessage(message);
    }

    private void ack(int instance, byte proposalNumber){
        if (proposalNumber == instances.get(instance).activeProposal) {
            instances.get(instance).acks += 1;

            if (instances.get(instance).active && instances.get(instance).acks >= f + 1) {
                instances.get(instance).active = false;
                decide(instances.get(instance).acceptedValues);
            }
        }
    }

    private void nack(int instance, byte proposalNumber, Set<Integer> proposal) {
        if (proposalNumber == instances.get(instance).activeProposal) {

        }
    }

    private void decide(Set<Integer> acceptedValues) {

    }

    // -----------------------------------------------------------------------------------------------------------------
    // ACCEPTOR
    // -----------------------------------------------------------------------------------------------------------------

    private void receiveProposal(int instanceId, byte proposalNumber, Set<Integer> proposal) {
        if (decided.contains(instanceId)) {
            // received a proposal for an instance that has already been decided
            return;
        }
        LatticeAgreementInstance instance = instances.computeIfAbsent(instanceId, id -> new LatticeAgreementInstance());

        if (isIncluded(instance.acceptedValues, proposal)) {
            instance.acceptedValues = proposal;
            sendAck(proposalNumber);
        }
        else {
            instance.acceptedValues.addAll(proposal);
            sendNack(proposalNumber, instance.acceptedValues);
        }
    }

    private void sendAck(byte proposalNumber) {

    }

    private void sendNack(byte proposalNumber, Set<Integer> proposal) {

    }

    // -----------------------------------------------------------------------------------------------------------------
    // UTILS
    // -----------------------------------------------------------------------------------------------------------------
    private static long encodeMessageKey(int messageId, int senderId) {
        // shift the senderId to the upper bits and combine with messageId
        return ((long) senderId << 31) | (messageId & 0x7FFFFFFF);
    }

    // serialize the type of message (proposal, nack, ack), the proposal and the number of proposal
    private byte[] packageProposal(byte type, int instanceId, byte activeProposalNum, Set<Integer> set) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 1 + set.size() * 4);

        buffer.put(type);
        buffer.putInt(instanceId);
        for (int value : set) {
            buffer.putInt(value);
        }
        buffer.put(activeProposalNum);

        return buffer.array();
    }

    private boolean isIncluded(Set<Integer> set1, Set<Integer> set2) {
        if (set2.containsAll(set1)) {
            return true;
        }
        else {
            return false;
        }
    }
}
