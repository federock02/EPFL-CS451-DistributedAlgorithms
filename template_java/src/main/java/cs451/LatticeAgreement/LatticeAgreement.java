package cs451.LatticeAgreement;

import cs451.BestEffortBroadcast.BEB;
import cs451.Host;
import cs451.Message;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

public class LatticeAgreement {
    private final Host myHost;

    // max number of accepted failures
    private byte f;
    // number of hosts
    private byte n;
    // number of proposals
    private int p;
    // max number of proposed values
    private int vs;
    // max number of distinct elements across proposals
    private int ds;
    // current lattice agreement instance
    private int instanceId;

    // map that holds info for each instance of lattice agreement
    private Map<Integer, LatticeAgreementInstance> instances;

    // decided instances
    private Map<Integer, Set<Integer>> decided;
    // last decided instance for keeping them in order
    private int lastDecided;

    int messageId = 1;

    // beb broadcaster
    private BEB broadcaster;

    private boolean flagStopProcessing = false;

    private messageDeliveryThread deliveryThread;

    public LatticeAgreement(Host host) {
        this.myHost = host;
    }

    public void startLatticeAgreement(List<Host> hosts, int p, int vs, int ds) {
        this.p = p;
        this.vs = vs;
        this.ds = ds;
        this.instanceId = 0;
        this.lastDecided = 0;

        deliveryThread = new messageDeliveryThread();
        deliveryThread.start();

        this.instances = new ConcurrentHashMap<>();
        this.decided = new ConcurrentHashMap<>();

        broadcaster = new BEB(this, this.myHost);
        broadcaster.startBEBBroadcaster(hosts);

        this.n = (byte) (hosts.size() - 1);

        this.f = (byte) ((hosts.size() - 1) / 2);
        //System.out.println("f = " + f);
    }

    public void stopProcessing() {
        this.flagStopProcessing = true;
        if (deliveryThread != null) {
            deliveryThread.interrupt();
        }
        broadcaster.stopProcessing();
    }

    // deliver message from BEB
    public void deliverMessage(Message message) {
        if (!flagStopProcessing && deliveryThread != null) {
            // System.out.println("Received message from " + message.getSenderId());
            deliveryThread.addMessage(message);
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // PROPOSER
    // -----------------------------------------------------------------------------------------------------------------
    public void propose(Set<Integer> proposal) {
        this.instanceId += 1;
        // System.out.println("Proposing instance " + instanceId);
        LatticeAgreementInstance instance = instances.computeIfAbsent(instanceId, id -> new LatticeAgreementInstance());

        instance.proposedValues = new HashSet<>(proposal);
        instance.active = true;
        instance.activeProposal += 1;
        instance.acks += 1;


        // broadcasting through the beb abstraction
        // broadcasts a message of type PROPOSAL
        Message message = new Message(messageId, packageProposal(MessageType.PROPOSAL.getCode(),
                instanceId, instance.activeProposal, proposal), myHost.getId());
        this.messageId += 1;
        // System.out.println(Arrays.toString(message.getPayload()));

        // adding own proposal
        // deliveryThread.addMessage(message);

        broadcaster.bebBroadcast(message);

        // check if the proposal already contains all the different values
        if (proposal.size() == ds) {
            // System.out.println("Predeciding");
            instance.active = false;
            decide(instance.proposedValues, instanceId);
        }
    }

    private void ack(int instance, byte proposalNumber){
        if (decided.containsKey(instance)) {
            return;
        }
        // System.out.println("Acked instance " + instance + " proposalNumber " + proposalNumber);
        LatticeAgreementInstance inst = instances.get(instance);
        if (proposalNumber == inst.activeProposal) {
            inst.acks += 1;

            // System.out.println("Acks: " + inst.acks);

            // check if enough acks received
            if (inst.active && inst.acks >= f + 1) {
                inst.active = false;
                decide(inst.proposedValues, instance);
            }
            if (inst.active && inst.acks + inst.nacks >= f + 1) {
                riPropose(instance);
            }
        }
    }

    private void nack(int instance, byte proposalNumber, Set<Integer> proposal) {
        if (decided.containsKey(instance)) {
            return;
        }
        // System.out.println("Nacked instance " + instance + " proposalNumber " + proposalNumber);
        LatticeAgreementInstance inst = instances.get(instance);
        if (proposalNumber == inst.activeProposal) {
            // union of proposals
            inst.proposedValues.addAll(proposal);
            inst.nacks += 1;
            // System.out.println("Acks: " + inst.acks);
            // System.out.println("Nacks: " + inst.nacks);

            /*
            System.out.println("Updated proposal:");
            for (Integer value : inst.proposedValues) {
                System.out.println(value);
            }
            */
            if (inst.proposedValues.size() == ds) {
                riPropose(instance);
                return;
            }

            // check if enough nacks + acks received
            if (inst.active && inst.nacks + inst.acks >= f + 1) {
                riPropose(instance);
            }
        }
    }

    private void riPropose(int instance) {
        LatticeAgreementInstance inst = instances.get(instance);

        inst.nacks = 0;
        // proposal is initialized with ack from proposer
        inst.acks = 1;
        inst.activeProposal += 1;

        Message message = new Message(messageId, packageProposal(MessageType.PROPOSAL.getCode(),
                instance, inst.activeProposal, inst.proposedValues), myHost.getId());
        this.messageId += 1;

        /*
        System.out.println("Adjusting proposal instance " + instance);
        System.out.println("Updated proposal:");
        for (Integer value : inst.proposedValues) {
            System.out.println(value);
        }
        */
        broadcaster.bebBroadcast(message);

        // check if the proposal already contains all the different values
        if (inst.proposedValues.size() == ds) {
            inst.active = false;
            decide(inst.proposedValues, instance);
        }
    }

    private void decide(Set<Integer> acceptedValues, int instanceId) {
        if (!flagStopProcessing && !decided.containsKey(instanceId)) {
            // System.out.println("Deciding instance " + instanceId);
            this.decided.put(instanceId, instances.get(instanceId).proposedValues);
            instances.remove(instanceId);
            if (instanceId == lastDecided + 1) {
                lastDecided += 1;
                this.myHost.logDecide(acceptedValues);

                // check if there's other waiting to be decided in order
                while (decided.containsKey(lastDecided + 1)) {
                    lastDecided += 1;
                    this.myHost.logDecide(decided.get(lastDecided));
                }
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // ACCEPTOR
    // -----------------------------------------------------------------------------------------------------------------
    private void receiveProposal(int instanceId, byte proposalNumber, Set<Integer> proposal, int senderId) {
        if (senderId == this.myHost.getId()) {
            // System.out.println(instances.get(instanceId).acks);
            // System.out.println("Proposal not processed");
            return;
        }

        // check if instance has already been decided
        if (decided.containsKey(instanceId)) {
            if (isIncluded(decided.get(instanceId), proposal)) {
                if (proposal.size() < ds) {
                    sendAck(instanceId, proposalNumber, senderId);
                }
            }
            else {
                // only send the difference in the proposal
                Set<Integer> difference = decided.get(instanceId).stream()
                        .filter(e -> !proposal.contains(e))
                        .collect(Collectors.toSet());
                sendNack(instanceId, proposalNumber, difference, senderId);
            }
            return;
        }

        LatticeAgreementInstance instance = instances.computeIfAbsent(instanceId, id -> new LatticeAgreementInstance());

        if (isIncluded(instance.proposedValues, proposal)) {
            // my current proposal is included in the proposal received
            instance.proposedValues = proposal;
            // don't send ack if the proposal I received already has all the possible values
            if (proposal.size() < ds) {
                sendAck(instanceId, proposalNumber, senderId);
            }
            else {
                // the proposal I received contains all the values possible
                // System.out.println("Received full proposal");
                instance.active = false;
                decide(instance.proposedValues, instanceId);
            }
        }
        else {
            // only send the difference in the proposal
            Set<Integer> difference = instance.proposedValues.stream()
                    .filter(e -> !proposal.contains(e))
                    .collect(Collectors.toSet());
            instance.proposedValues.addAll(proposal);
            sendNack(instanceId, proposalNumber, instance.proposedValues, senderId);
            if (instance.proposedValues.size() == ds) {
                // now the proposal contains all the possible different values
                instance.active = false;
                decide(instance.proposedValues, instanceId);
            }
        }
    }

    private void sendAck(int instanceId, byte proposalNumber, int senderId) {
        Message ack = new Message(messageId, packageProposal(MessageType.ACK.getCode(), instanceId,
                proposalNumber, new HashSet<>()), myHost.getId());
        this.messageId += 1;
        // System.out.println("Sending ack to " + senderId + " for instance " + instanceId + " proposalNumber " + proposalNumber);
        broadcaster.plSendTo(ack, senderId);
    }

    private void sendNack(int instanceId, byte proposalNumber, Set<Integer> proposal, int senderId) {
        Message nack = new Message(messageId, packageProposal(MessageType.NACK.getCode(), instanceId,
                proposalNumber, proposal), myHost.getId());
        this.messageId += 1;
        // System.out.println("Sending nack to " + senderId + " for instance " + instanceId + " proposalNumber " + proposalNumber);
        broadcaster.plSendTo(nack, senderId);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // UTILS
    // -----------------------------------------------------------------------------------------------------------------
    // serialize the type of message (proposal, nack, ack), the proposal and the number of proposal
    private byte[] packageProposal(byte type, int instanceId, byte activeProposalNum, Set<Integer> set) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 1 + set.size() * 4);

        //System.out.println("Packaging message");

        //System.out.println("Type: " + MessageType.fromCode(type));
        buffer.put(type);
        //System.out.println("InstanceId: " + instanceId);
        buffer.putInt(instanceId);
        //System.out.println("ActiveProposalNumber: " + activeProposalNum);
        buffer.put(activeProposalNum);
        //System.out.println("Proposal: ");
        for (int value : set) {
            //System.out.println(value);
            buffer.putInt(value);
        }

        // System.out.println(Arrays.toString(buffer.array()));

        return buffer.array();
    }

    private boolean isIncluded(Set<Integer> set1, Set<Integer> set2) {
        for (Integer value : set1) {
            if (!set2.contains(value)) {
                // System.out.println("Missing " + value);
            }
        }
        return set2.containsAll(set1);
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
            // System.out.println("---------Processing message");
            byte[] payload = message.getPayload();
            // System.out.println(Arrays.toString(payload));
            int senderId = message.getSenderId();

            ByteBuffer buffer = ByteBuffer.wrap(payload);
            MessageType type = MessageType.fromCode(buffer.get());
            // System.out.println("---------Type: " + type + " from " +  senderId);
            int instanceId = buffer.getInt();
            // System.out.println("---------InstanceId: " + instanceId);
            byte activeProposal = buffer.get();
            // System.out.println("---------ActiveProposal: " + activeProposal);

            if (type.equals(MessageType.ACK)) {
                // if it's an ack I do not care about the proposal
                if (!flagStopProcessing) {
                    // System.out.println("---------Ack for instance " + instanceId + " proposal " + activeProposal);
                    ack(instanceId, activeProposal);
                }
            }
            else {
                // get the proposal set
                Set<Integer> proposal = new HashSet<>();
                int value;
                // System.out.println("---------Received proposal with " + buffer.remaining() + " bytes: ");
                while (buffer.remaining() >= 4) {
                    value = buffer.getInt();
                    // System.out.println("---------" + value);
                    proposal.add(value);
                }
                if (buffer.remaining() != 0) {
                    System.err.println("---------Problem with proposal instanceId " + instanceId +
                            " activeProposal " + activeProposal +
                            " type " + type);
                }

                if (type.equals(MessageType.NACK)) {
                    if (!flagStopProcessing) {
                        nack(instanceId, activeProposal, proposal);
                    }
                }
                else {
                    if (!flagStopProcessing) {
                        receiveProposal(instanceId, activeProposal, proposal, senderId);
                    }
                }
            }
        }
    }

    private static class LatticeAgreementInstance {
        boolean active;
        byte acks;
        byte nacks;
        byte activeProposal;

        private Set<Integer> proposedValues;

        public LatticeAgreementInstance() {
            this.active = false;
            this.acks = 0;
            this.nacks = 0;
            this.activeProposal = 0;
            this.proposedValues = new HashSet<>();
        }

    }

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
}
