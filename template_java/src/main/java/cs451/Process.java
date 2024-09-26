package cs451;

public class Process {
    private final int id;
    private final int port;

    public Process(int id, int port) {
        this.id = id;
        this.port = port;
    }

    public int getId() {
        return id;
    }

    public int getPort() {
        return port;
    }
}
