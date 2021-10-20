package org.dfs.transport;

import java.net.Socket;
import java.util.HashMap;
import java.util.Set;

public class TCPConnectionsCache {
    private final HashMap<Socket, TCPConnection> cachedConnections = new HashMap<>();

    private TCPConnectionsCache instance;

    public synchronized void addConnection(Socket socket, TCPConnection tcpConnection) {
        cachedConnections.put(socket, tcpConnection);
    }

    public synchronized TCPConnection getConnection(Socket socket) {
        return cachedConnections.get(socket);
    }

    public synchronized void removeConnection(Socket socket) {
        cachedConnections.remove(socket);
    }

    public synchronized boolean containsConnection(Socket socket) {
        return cachedConnections.containsKey(socket);
    }

    public void printConnections() {
        Set<Socket> sockets = cachedConnections.keySet();
        if (sockets.size() == 0) {
            System.out.println("No Chunk Servers have been registered");
        } else {
            for (Socket socket : sockets) {
                System.out.println(cachedConnections.get(socket));
            }
        }
    }
}
