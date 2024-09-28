package cs451;

public interface PerfectLink {
    void send(Host receiver, Message message);
    void deliver();
}
