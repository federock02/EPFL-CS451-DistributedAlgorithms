package cs451;

import java.nio.ByteBuffer;


public class Message {
    private final int id;
    private final int payload;
    private final int senderId;

    public Message(int id, int payload, int senderId) {
        this.id = id;
        this.payload = payload;
        this.senderId = senderId;
    }

    public int getId() {
        return id;
    }

    public int getPayload() {
        return payload;
    }

    public int getSenderId() {
        return senderId;
    }

    // manual serialization
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(8); // 4 bytes for id, 8 bytes for payload
        buffer.putInt(this.id);
        buffer.putInt(this.payload);
        buffer.putInt(this.senderId);
        return buffer.array();
    }

    // manual deserialization
    public static Message deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int id = buffer.getInt();
        int payload = buffer.getInt();
        int senderId = buffer.getInt();
        return new Message(id, payload, senderId);
    }
}
