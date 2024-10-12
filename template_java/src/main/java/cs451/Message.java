package cs451;

import java.io.*;
import java.nio.ByteBuffer;

public class Message {
    private final int id;
    private final Payload payload;
    private final int senderId;

    public Message(int id, Payload payload, int senderId) {
        this.id = id;
        this.payload = payload;
        this.senderId = senderId;
    }

    public int getId() {
        return id;
    }

    public Payload getPayload() {
        return payload;
    }

    public int getSenderId() {
        return senderId;
    }

    // manual serialization
    public byte[] serialize() {

        // serialize the payload object
        byte[] payloadBytes = serializePayload(this.payload);

        // create buffer (4 bytes for id, 4 bytes for senderId, n bytes for payload (n = length))
        assert payloadBytes != null;
        ByteBuffer buffer = ByteBuffer.allocate(8 + payloadBytes.length);

        buffer.putInt(this.id);
        buffer.putInt(this.senderId);
        buffer.put(payloadBytes);

        return buffer.array();
    }

    private byte[] serializePayload(Payload payload) {
        try (ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(byteOutputStream))
        {
            // serialize the payload object
            out.writeObject(payload);

            // return byte array of the serialized object
            return byteOutputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    // manual deserialization
    public static Message deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        // extract id and senderId
        int id = buffer.getInt();
        int senderId = buffer.getInt();

        // extract payload bytes
        byte[] payloadBytes = new byte[buffer.remaining()];
        buffer.get(payloadBytes);

        // deserialize payload object
        Payload payload = deserializePayload(payloadBytes);

        return new Message(id, payload, senderId);
    }

    private static Payload deserializePayload(byte[] payloadBytes) {
        try (ByteArrayInputStream byteInputStream = new ByteArrayInputStream(payloadBytes);
             ObjectInputStream in = new ObjectInputStream(byteInputStream)) {
            return (Payload) in.readObject(); // Deserialize the object
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }
}
