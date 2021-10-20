package org.dfs.wireformats;

import java.net.Socket;

public abstract class Event {
    private Socket socket;

    public Socket getSocket() {
        return socket;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    public abstract byte[] getBytes();

    public abstract int getType();
}
