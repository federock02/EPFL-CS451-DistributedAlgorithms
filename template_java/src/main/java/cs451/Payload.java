package cs451;

import java.io.Serializable;

public class Payload implements Serializable {

    // can be implemented in different ways
    private int value;

    public Payload(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
