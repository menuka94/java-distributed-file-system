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
    private TCPServerThread tcpServerThread;
    private TCPConnectionsCache tcpConnectionsCache;

    private final ArrayList<ChunkServer> chunkServers;
    private InteractiveCommandParser commandParser;

    // singleton instance
    private static volatile Controller instance;

    private Controller(int port) throws IOException {
        log.info("Initializing Client on {}", System.getenv("HOSTNAME"));
        chunkServers = new ArrayList<>();
        tcpConnectionsCache = new TCPConnectionsCache();
        tcpServerThread = new TCPServerThread(port, this, tcpConnectionsCache);
        commandParser = new InteractiveCommandParser(this);
    }

    public void initialize() {
        tcpServerThread.start();
        commandParser.start();
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Not enough arguments to start Controller. " +
                    "Enter <port> for the Controller");
            System.exit(1);
        } else if (args.length > 1) {
            System.out.println("Invalid number of arguments. Provide <port> only");
        }

        int port = Integer.parseInt(args[0]);
        Controller controller = Controller.getInstance(port);
        controller.initialize();
    }

    public static Controller getInstance(int port) throws IOException {
        if (instance != null) {
            return instance;
        }
        synchronized (ChunkServer.class) {
            if (instance == null) {
                instance = new Controller(port);
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

    public void printHost() {
        System.out.println("printHost()");
    }

    // TODO: implement heartbeats
}
