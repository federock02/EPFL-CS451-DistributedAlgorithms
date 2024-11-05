package cs451;

import java.nio.ByteBuffer;

public class Message {
    private final int id;
    private final byte[] payload;
    private final byte senderId;

    public Message(int id, byte[] payload, int senderId) {
        this.id = id;
        this.payload = payload;
        // byte is -128 to 127, processes from 1 to 128
        this.senderId = (byte) (senderId - 1);
    }

    public Message(int id, int payload, int senderId) {
        this.id = id;
        this.payload = new byte[4];
        this.payload[0] = (byte) ((payload & 0xFF000000) >> 24);
        this.payload[1] = (byte) ((payload & 0x00FF0000) >> 16);
        this.payload[2] = (byte) ((payload & 0x0000FF00) >> 8);
        this.payload[3] = (byte) ((payload & 0x000000FF));
        // byte is -128 to 127, processes from 1 to 128
        this.senderId = (byte) (senderId - 1);
        // System.out.println("New message for host " + this.senderId);
    }

    public Message(int id, String payload, int senderId) {
        this.id = id;
        this.payload = payload.getBytes();
        // byte is -128 to 127, processes from 1 to 128
        this.senderId = (byte) (senderId - 1);
    }

    public Message(int id, Object payload, int senderId) {
        this.id = id;
        this.payload = payload.toString().getBytes();
        // byte is -128 to 127, processes from 1 to 128
        this.senderId = (byte) (senderId - 1);
    }

    public Message(Message message) {
        this.id = message.id;
        this.payload = message.payload;
        this.senderId = message.getByteSenderId();
    }

    public int getId() {
        return id;
    }

    public byte[] getPayload() {
        return payload;
    }

    public int getSenderId() {
        return senderId + 1;
    }

    public byte getByteSenderId() {
        return this.senderId;
    }

    // manual serialization
    public byte[] serialize() {
        // create buffer (4 bytes for id, 1 bytes for senderId, n bytes for payload (n = length))
        ByteBuffer buffer = ByteBuffer.allocate(5 + payload.length);

        buffer.putInt(this.id);
        buffer.put(this.senderId);
        buffer.put(payload);

        return buffer.array();
    }

    // manual deserialization
    public static Message deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        // extract id and senderId
        int id = buffer.getInt();
        byte senderId = buffer.get();
        byte[] payload = buffer.array();

        return new Message(id, payload, senderId + 1);
    }
}
