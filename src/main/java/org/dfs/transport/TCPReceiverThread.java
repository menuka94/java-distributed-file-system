package org.dfs.transport;

import org.dfs.node.Node;
import org.dfs.wireformats.EventFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;

public class TCPReceiverThread extends Thread {
    private static final Logger log = LogManager.getLogger(TCPReceiverThread.class);
    private final Socket socket;
    private final DataInputStream din;
    private final Node node;

    public TCPReceiverThread(Socket socket, Node node) throws IOException {
        this.node = node;
        this.socket = socket;
        din = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
    }

    @Override
    public void run() {
        int dataLength;
        while (socket != null) {
            try {
                dataLength = din.readInt();
                byte[] data = new byte[dataLength];
                din.readFully(data, 0, dataLength);
                node.onEvent(EventFactory.getInstance().getEvent(data, socket));
            } catch (IOException e) {
                String hostName = socket.getInetAddress().getHostName();
                log.warn("Connection to {} terminated", hostName);
                log.debug(e.getLocalizedMessage());
                // e.printStackTrace();
                break;
            }
        }
    }
}
