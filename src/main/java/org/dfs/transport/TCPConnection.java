package org.dfs.transport;

import org.dfs.node.Node;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;

public class TCPConnection {
    public static final Logger log = LogManager.getLogger(TCPConnection.class);
    private final Socket socket;
    private TCPSender tcpSender;
    private final TCPReceiverThread tcpReceiverThread;
    private final Node node; // node associated with TCPConnection

    public TCPConnection(Socket socket, Node node) throws IOException {
        this.socket = socket;
        this.node = node;
        tcpReceiverThread = new TCPReceiverThread(socket, node);
        tcpReceiverThread.start();
    }

    public Socket getSocket() {
        return socket;
    }

    public void sendData(byte[] data) throws IOException {
        if (tcpSender == null) {
            tcpSender = new TCPSender(socket);
        }

        tcpSender.sendData(data);
    }

    public byte[] getDestinationAddress() {
        return socket.getInetAddress().getAddress();
    }

    public int getDestinationPort() {
        return socket.getPort();
    }

    public byte[] getLocalAddress() {
        return socket.getLocalAddress().getAddress();
    }

    public String getLocalHostname() {
        return socket.getLocalAddress().getCanonicalHostName();
    }

    public int getLocalPort() {
        return socket.getLocalPort();
    }

    @Override
    public String toString() {
        return "TCPConnection{" +
                "destinationAddress=" + socket.getInetAddress().getHostAddress() +
                ", destinationPort=" + getDestinationPort() +
                ", localAddress=" + socket.getLocalAddress().getHostAddress() +
                ", localPort=" + getLocalPort() +
                '}';

    }
}