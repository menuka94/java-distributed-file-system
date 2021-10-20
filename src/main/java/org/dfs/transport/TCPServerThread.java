package org.dfs.transport;


import org.dfs.node.Node;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class TCPServerThread extends Thread {
    private static final Logger logger = LogManager.getLogger(TCPServerThread.class);
    private boolean listeningForClients;
    private final ServerSocket serverSocket;
    private final int listenPort;
    private final Node node;
    private final TCPConnectionsCache tcpConnectionsCache;

    public TCPServerThread(int listenPort, Node node, TCPConnectionsCache tcpConnectionsCache)
            throws IOException {
        serverSocket = new ServerSocket(listenPort);
        this.listenPort = serverSocket.getLocalPort();
        this.node = node;
        this.tcpConnectionsCache = tcpConnectionsCache;
    }

    @Override
    public void run() {
        listeningForClients = true;

        while (listeningForClients) {
            try {
                logger.info("Server is accepting connections on port " + listenPort);
                Socket socket = serverSocket.accept();
                TCPConnection tcpConnection = new TCPConnection(socket, node);
                tcpConnectionsCache.addConnection(socket, tcpConnection);
                logger.info("New Connection Established with " +
                        tcpConnection.getSocket().getInetAddress().getHostAddress() + " on port " +
                        tcpConnection.getDestinationPort());
            } catch (IOException e) {
                logger.error("Error while listening for clients");
                logger.error(e.getStackTrace());
            }
        }

        // no longer listening for clients
        logger.info("Server is no longer accepting connections");
        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Error in closing serverSocket");
            logger.error(e.getStackTrace());
        }
    }

    public void stopListeningForClients() {
        listeningForClients = false;
    }

    public int getListeningPort() {
        return serverSocket.getLocalPort();
    }

}

