package cs555.hw1.wireformats;

import java.net.Socket;

public abstract class Event {
    private Socket socket;

    public Socket getSocket() {
        return socket;
    }

    public abstract byte[] getBytes();

    public abstract int getType();
}
