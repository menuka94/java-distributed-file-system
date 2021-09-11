package cs555.hw1.node;

import cs555.hw1.Constants;
import cs555.hw1.InteractiveCommandParser;
import cs555.hw1.transport.TCPConnection;
import cs555.hw1.transport.TCPConnectionsCache;
import cs555.hw1.transport.TCPServerThread;
import cs555.hw1.wireformats.ClientRequestsChunkServersFromController;
import cs555.hw1.wireformats.ControllerSendsClientChunkServers;
import cs555.hw1.wireformats.Event;
import cs555.hw1.wireformats.Protocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;

/**
 * A controller node for managing information about chunk servers and chunks within the
 * system. There will be only 1 instance of the controller node.
 * Responsible for tracking information about the chunks chunks held by various chunk servers.
 * This is achieved using heartbeats that are periodically exchanged between the controller and chunk servers.
 * Also responsible for tracking 'live' chunk servers in the system.
 * The Controller does not store anything on disk -- all information about the chunk servers
 * and the chunks they hold are maintained in memory.
 *
 * The Controller runs on a preset host and port.
 */
public class Controller implements Node {
    private static final Logger log = LogManager.getLogger(Controller.class);

    private int port;
    private TCPServerThread tcpServerThread;
    private TCPConnectionsCache tcpConnectionsCache;

    private final ArrayList<ChunkServer> chunkServers;
    private InteractiveCommandParser commandParser;
    private Socket clientSocket;
    private TCPConnection clientConnection;

    // singleton instance
    private static volatile Controller instance;

    private Controller(int port) throws IOException {
        log.info("Initializing Controller on {}:{}", System.getenv("HOSTNAME"), port);
        this.port = port;
        chunkServers = new ArrayList<>();
        tcpConnectionsCache = new TCPConnectionsCache();
        tcpServerThread = new TCPServerThread(port, this, tcpConnectionsCache);
        commandParser = new InteractiveCommandParser(this);
        clientSocket = new Socket(Constants.Client.HOST, Constants.Client.PORT);
        clientConnection = new TCPConnection(clientSocket, this);
        tcpConnectionsCache.addConnection(clientSocket, clientConnection);
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
        System.out.println("Host: " + System.getenv("HOSTNAME") + ", Port: " + port);
    }

    @Override
    public void onEvent(Event event) {
        int type = event.getType();
        log.info("Event type: {}", type);
        switch (type) {
            case Protocol.CLIENT_REQUESTS_CHUNK_SERVERS_FROM_CONTROLLER:
                sendClientChunkServers(event);
                break;
        }
    }

    private void sendClientChunkServers(Event event) {
        log.info("sendClientChunkServers(event)");
        ClientRequestsChunkServersFromController requestEvent =
                (ClientRequestsChunkServersFromController) event;
        log.info("IP Address Length: {}", requestEvent.getIpAddressLength());
        log.info("IP Address: {}", new String(requestEvent.getIpAddress()));
        log.info("Port: {}", requestEvent.getPort());

        // test data
        String[] chunkServerHosts = new String[]{"host1", "host2", "host3"};
        int[] chunkServerPorts = new int[]{1000, 2000, 3000};

        ControllerSendsClientChunkServers responseEvent =
                new ControllerSendsClientChunkServers();
        responseEvent.setChunkServers(chunkServerHosts, chunkServerPorts);
        try {
            clientConnection.sendData(responseEvent.getBytes());
        } catch (IOException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

    // TODO: implement heartbeats
}
