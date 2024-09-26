package cs451;

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
}
