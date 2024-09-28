package cs451;

import java.nio.ByteBuffer;


public class Message {
    private final int id;
    private final int payload;

    public Message(int id, int payload) {
        this.id = id;
        this.payload = payload;
    }

    public int getId() {
        return id;
    }

    public int getPayload() {
        return payload;
    }

    // manual serialization
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(8); // 4 bytes for id, 8 bytes for payload
        buffer.putInt(this.id);
        buffer.putInt(this.payload);
        return buffer.array();
    }

    // manual deserialization
    public static Message deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int id = buffer.getInt();
        int payload = buffer.getInt();
        return new Message(id, payload);
    }
}
