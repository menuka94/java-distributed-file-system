package cs555.hw1;

import cs555.hw1.transport.TCPConnectionsCache;
import cs555.hw1.transport.TCPServerThread;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;

/**
 * A controller node for managing information about chunk servers and chunks within the
 * system. There will be only 1 instance of the controller node.
 */
public class Controller implements Node {
    private static final Logger log = LogManager.getLogger(Controller.class);
    private static String host = Constants.Controller.HOST;
    private static int port = Integer.parseInt(Constants.Controller.PORT);
    private TCPServerThread tcpServerThread;
    private TCPConnectionsCache tcpConnectionsCache;

    private final ArrayList<ChunkServer> chunkServers;

    // singleton instance
    private static volatile Controller instance;

    private Controller() throws IOException {
        chunkServers = new ArrayList<>();
        tcpConnectionsCache = new TCPConnectionsCache();
        tcpServerThread = new TCPServerThread(port, this, tcpConnectionsCache);
        tcpServerThread.start();
    }

    public static Controller getInstance() throws IOException {
        log.info("getInstance()");
        if (instance != null) {
            return instance;
        }
        synchronized (ChunkServer.class) {
            if (instance == null) {
                instance = new Controller();
            }

            return instance;
        }
    }

    public ArrayList<ChunkServer> getChunkServersForNewFile() {
        log.info("getChunkServersForNewFile");
        ArrayList<ChunkServer> availableChunkServers = new ArrayList<>();

        return availableChunkServers;
    }

    public void listChunkServers() {
        System.out.println("No. of ChunkServers: " + chunkServers.size());
    }

    // TODO: implement heartbeats
}
