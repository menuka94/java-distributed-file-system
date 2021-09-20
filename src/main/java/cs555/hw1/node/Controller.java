package cs555.hw1.node;

import cs555.hw1.InteractiveCommandParser;
import cs555.hw1.node.chunkServer.ChunkServer;
import cs555.hw1.transport.TCPConnection;
import cs555.hw1.transport.TCPConnectionsCache;
import cs555.hw1.transport.TCPServerThread;
import cs555.hw1.util.Constants;
import cs555.hw1.util.controller.FileInfo;
import cs555.hw1.wireformats.ControllerSendsClientChunkServers;
import cs555.hw1.wireformats.Event;
import cs555.hw1.wireformats.Protocol;
import cs555.hw1.wireformats.ProtocolLookup;
import cs555.hw1.wireformats.ReadFileRequest;
import cs555.hw1.wireformats.ReadFileResponse;
import cs555.hw1.wireformats.RegisterChunkServer;
import cs555.hw1.wireformats.RegisterClient;
import cs555.hw1.wireformats.ReportChunkServerRegistration;
import cs555.hw1.wireformats.ReportClientRegistration;
import cs555.hw1.wireformats.SendFileInfo;
import cs555.hw1.wireformats.SendMajorHeartbeat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A controller node for managing information about chunk servers and chunks within the
 * system. There will be only 1 instance of the controller node.
 * Responsible for tracking information about the chunks held by various chunk servers.
 * This is achieved using heartbeats that are periodically exchanged between the controller and chunk servers.
 * Also responsible for tracking 'live' chunk servers in the system.
 * The Controller does not store anything on disk -- all information about the chunk servers
 * and the chunks they hold are maintained in memory.
 * <p>
 * The Controller runs on a preset host and port.
 */
public class Controller implements Node {
    private static final Logger log = LogManager.getLogger(Controller.class);

    private int port;
    private TCPServerThread tcpServerThread;
    private TCPConnectionsCache tcpConnectionsCache;

    private InteractiveCommandParser commandParser;
    private Socket clientSocket;
    private TCPConnection clientConnection;

    // ChunkServerID, Socket
    private volatile ConcurrentHashMap<Integer, Socket> chunkServerSocketMap;

    // ChunkServerID, ChunkInfo
    private volatile ConcurrentHashMap<Integer, ArrayList<String>> chunkServerChunksMap;

    private volatile ConcurrentHashMap<Integer, Long> chunkServerFreeSpaceMap;

    // ChunkServerID, Port
    private volatile ConcurrentHashMap<Integer, Integer> chunkServerListeningPortMap;

    // File names and number of chunks (for all files in the system)
    private volatile Vector<FileInfo> fileInfos;

    private Random random;

    // singleton instance
    private static volatile Controller instance;

    private Controller(int port) throws IOException {
        log.info("Initializing Controller on {}:{}", System.getenv("HOSTNAME"), port);
        this.port = port;
        tcpConnectionsCache = new TCPConnectionsCache();
        tcpServerThread = new TCPServerThread(port, this, tcpConnectionsCache);
        commandParser = new InteractiveCommandParser(this);
        tcpConnectionsCache.addConnection(clientSocket, clientConnection);
        chunkServerSocketMap = new ConcurrentHashMap<>();
        chunkServerListeningPortMap = new ConcurrentHashMap<>();
        chunkServerChunksMap = new ConcurrentHashMap<>();
        chunkServerFreeSpaceMap = new ConcurrentHashMap<>();
        fileInfos = new Vector<>();
        random = new Random();
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
        ArrayList<Integer> ids = new ArrayList<>(chunkServerSocketMap.keySet());
        System.out.println("No. of Chunk Servers: " + ids.size());
        Collections.sort(ids);
        for (Integer id : ids) {
            Socket socket = chunkServerSocketMap.get(id);
            System.out.println("Chunk Server ID: " + id +
                    ", IP Address: " + socket.getInetAddress().getHostAddress() +
                    ", Hostname: " + socket.getInetAddress().getCanonicalHostName() +
                    ", Port: " + socket.getPort());
        }
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
                sendChunkServersToClient(event);
                break;
            case Protocol.REGISTER_CLIENT:
                try {
                    registerClient(event);
                } catch (IOException e) {
                    log.error("Error registering client: {}", e.getMessage());
                    e.printStackTrace();
                }
                break;
            case Protocol.REGISTER_CHUNK_SERVER:
                registerChunkServer(event);
                break;
            case Protocol.SEND_MAJOR_HEARTBEAT:
                handleMajorHeartbeat(event);
                break;
            case Protocol.SEND_MINOR_HEARTBEAT:
                handleMinorHeartbeat(event);
                break;
            case Protocol.READ_FILE_REQUEST:
                handleReadFileRequest(event);
                break;
            case Protocol.SEND_FILE_INFO:
                handleSendFileInfo(event);
                break;
            default:
                log.warn("Unknown event type");
        }
    }

    private void handleSendFileInfo(Event event) {
        SendFileInfo sendFileInfo = (SendFileInfo) event;
        String fileName = sendFileInfo.getFileName();
        int noOfChunks = sendFileInfo.getNoOfChunks();
        int fileSize = sendFileInfo.getFileSize();
        fileInfos.add(new FileInfo(fileName, noOfChunks, fileSize));
        log.info("Added new file info: (name={}, #chunks={}, size={} KB)",
                fileName, noOfChunks, fileSize);
    }

    /**
     * Send ChunkServers (host, port) to client for each of the chunks for the file requested
     */
    private void handleReadFileRequest(Event event) {
        ReadFileRequest readFileRequest = (ReadFileRequest) event;
        String fileName = readFileRequest.getFileName();

        log.info("readFile: {}", fileName);

        int noOfChunks = 0;
        int fileSize = 0;
        boolean fileFound = false;
        for (FileInfo fileInfo : fileInfos) {
            if (fileInfo.getFileName().equals(fileName)) {
                fileFound = true;
                noOfChunks = fileInfo.getNoOfChunks();
                fileSize = fileInfo.getFileSize();
            }
        }

        if (!fileFound) {
            log.warn("File '{}' not found", fileName);
            return;
        }

        // find chunk servers that contain each chunk of the file
        String[] chunkServerHosts = new String[noOfChunks];
        int[] chunkServerPorts = new int[noOfChunks];

        for (int i = 0; i < noOfChunks; i++) {
            String chunkName = fileName + Constants.ChunkServer.EXT_DATA_CHUNK + (i + 1);

            // iterate through map <ChunkServerID, chunkNames>
            for (Map.Entry<Integer, ArrayList<String>> entry : chunkServerChunksMap.entrySet()) {
                if (entry.getValue().contains(chunkName)) {
                    // found a chunk server containing the chunk we're looking for
                    int chunkServerId = entry.getKey();
                    Socket socket = chunkServerSocketMap.get(chunkServerId);
                    chunkServerHosts[i] = socket.getInetAddress().getHostAddress();
                    chunkServerPorts[i] = socket.getPort();
                }
            }
        }

        // sanity check
        for (String chunkServerHost : chunkServerHosts) {
            if ("".equals(chunkServerHost)) {
                log.error("No ChunkServer with the needed chunk found.");
            }
        }

        // send response to client
        ReadFileResponse readFileResponse = new ReadFileResponse();
        readFileResponse.setFileName(fileName);
        readFileResponse.setNoOfChunks(noOfChunks);
        readFileResponse.setFileSize(fileSize);
        readFileResponse.setChunkServerHosts(chunkServerHosts);
        readFileResponse.setChunkServerPorts(chunkServerPorts);

        try {
            clientConnection.sendData(readFileResponse.getBytes());
        } catch (IOException e) {
            log.error(e.getLocalizedMessage());
            e.printStackTrace();
        }
    }

    private synchronized void handleMinorHeartbeat(Event event) {
    }

    private synchronized void handleMajorHeartbeat(Event event) {
        SendMajorHeartbeat heartbeat = (SendMajorHeartbeat) event;
        Socket socket = heartbeat.getSocket();
        String chunkServerHostname = socket.getInetAddress().getHostName();
        long freeSpace = heartbeat.getFreeSpace();
        int noOfChunks = heartbeat.getNoOfChunks();

        //update chunks/files map
        ArrayList<String> chunks = heartbeat.getChunks();
        for (Map.Entry<Integer, Socket> entry : chunkServerSocketMap.entrySet()) {
            if (socket == entry.getValue()) {
                chunkServerChunksMap.put(entry.getKey(), chunks);
            }
            chunkServerFreeSpaceMap.put(entry.getKey(), freeSpace);
        }

        log.info("Major Heartbeat received from ChunkServer '{}': (freeSpace={}, #chunks={})",
                chunkServerHostname, freeSpace, noOfChunks);
    }

    private synchronized void registerChunkServer(Event event) {
        // Process Request
        RegisterChunkServer registerChunkServer = (RegisterChunkServer) event;
        log.info("Chunk Server IP Address Length: {}", registerChunkServer.getIpAddressLength());

        byte[] ipAddress = registerChunkServer.getIpAddress();
        log.info("Chunk Server IP Address: {}", ipAddress);

        int port = registerChunkServer.getPort();
        log.info("Chunk Server Port: {}", port);

        int randomId = 0;

        // Start Response
        ReportChunkServerRegistration responseEvent = new ReportChunkServerRegistration();
        if (!Arrays.equals(ipAddress,
                registerChunkServer.getSocket().getInetAddress().getAddress())) {
            log.warn("IP Addresses differ");
            responseEvent.setSuccessStatus(-1);
            String infoString = "Registration IP Address and origin IP Address do not match";
            responseEvent.setInfoString(infoString);
            responseEvent.setLengthOfString((byte) infoString.getBytes().length);
        } else if (tcpConnectionsCache.containsConnection(registerChunkServer.getSocket())) {
            // everything is OK. Proceed to register the Chunk Server
            randomId = random.nextInt(Constants.ChunkServer.MAX_NODES) + 1; // add one to avoid zero

            // check if the ID has already been assigned
            // (random object could return the same ID more than once)
            ConcurrentHashMap.KeySetView<Integer, Socket> assignedIds = chunkServerSocketMap.keySet();
            while (assignedIds.contains(randomId)) {
                // if the random ID has already been assigned to a ChunkServer, generate a new ID
                randomId = random.nextInt(Constants.ChunkServer.MAX_NODES) + 1; // add one to avoid zero
            }

            log.info("Generated ID for new ChunkServer: {}", randomId);
            responseEvent.setSuccessStatus(0);
            String infoString = "Chunk Server successfully registered with the Controller";
            responseEvent.setInfoString(infoString);
            responseEvent.setLengthOfString((byte) infoString.getBytes().length);
        } else {
            log.warn("ChunkServer connection not found in TCPConnectionsCache. Please try again.");
            return;
        }

        TCPConnection tcpConnection = tcpConnectionsCache.getConnection(registerChunkServer.getSocket());
        try {
            tcpConnection.sendData(responseEvent.getBytes());
            chunkServerSocketMap.put(randomId, registerChunkServer.getSocket());
            chunkServerListeningPortMap.put(randomId, registerChunkServer.getPort());
        } catch (IOException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }


    private void registerClient(Event event) throws IOException {
        log.debug("registerClient(event)");

        // Process Request
        RegisterClient registerClient = (RegisterClient) event;
        log.info("Client IP Address Length: {}", registerClient.getIpAddressLength());

        byte[] clientIpAddress = registerClient.getIpAddress();
        log.info("Client IP Address: {}", clientIpAddress);

        int clientPort = registerClient.getPort();
        log.info("Client Port: {}", clientPort);

        // Start Response
        ReportClientRegistration responseEvent = new ReportClientRegistration();
        if (!Arrays.equals(clientIpAddress,
                registerClient.getSocket().getInetAddress().getAddress())) {
            log.warn("IP Addresses differ");
            responseEvent.setSuccessStatus(-1);
            String infoString = "Registration IP Address and origin IP Address do not match";
            responseEvent.setInfoString(infoString);
            responseEvent.setLengthOfString((byte) infoString.getBytes().length);
        } else if (tcpConnectionsCache.containsConnection(registerClient.getSocket())) {
            // initialize client connection
            log.info("Initializing Client Connection");
            responseEvent.setSuccessStatus(0);
            String infoString = "Client successfully registered with the Controller";
            responseEvent.setInfoString(infoString);
            responseEvent.setLengthOfString((byte) infoString.getBytes().length);
        } else {
            log.warn("Client connection not found in TCPConnectionsCache. Please try again.");
            return;
        }

        clientSocket = registerClient.getSocket();
        clientConnection = tcpConnectionsCache.getConnection(registerClient.getSocket());
        clientConnection.sendData(responseEvent.getBytes());
    }

    /**
     * Send client information about 3 ChunkServers to store a new file
     *
     * @param event
     */
    private synchronized void sendChunkServersToClient(Event event) {
        log.debug("sendChunkServerToClient(event): {}",
                ProtocolLookup.getEventLiteral(event.getType()));

        String[] chunkServerHosts = new String[3];
        String[] chunkServerHostNames = new String[3];
        int[] chunkServerPorts = new int[3];

        int noOfLiveChunkServers = chunkServerSocketMap.keySet().size();
        if (noOfLiveChunkServers < 3) {
            log.warn("No. of Live Chunk Servers is less than 3. Returning...");
            return;
        }
        ArrayList<Integer> assignedIDs = new ArrayList<>(chunkServerSocketMap.keySet());

        // select 3 registered chunk servers
        for (int i = 0; i < 3; i++) {
            String chunkServerHost = chunkServerSocketMap.get(assignedIDs.get(i))
                    .getInetAddress().getHostAddress();
            String chunkServerHostName = chunkServerSocketMap.get(assignedIDs.get(i))
                    .getInetAddress().getCanonicalHostName();
            int chunkServerPort = chunkServerListeningPortMap.get(assignedIDs.get(i));
            chunkServerHosts[i] = chunkServerHost;
            chunkServerHostNames[i] = chunkServerHostName;
            chunkServerPorts[i] = chunkServerPort;
        }

        ControllerSendsClientChunkServers responseEvent =
                new ControllerSendsClientChunkServers();

        responseEvent.setChunkServerHosts(chunkServerHosts);
        responseEvent.setChunkServerHostNames(chunkServerHostNames);
        responseEvent.setChunkServerPorts(chunkServerPorts);

        try {
            clientConnection.sendData(responseEvent.getBytes());
        } catch (IOException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Print information about chunks in all registered chunk servers
     */
    public synchronized void printChunks() {
        for (Map.Entry<Integer, ArrayList<String>> entry : chunkServerChunksMap.entrySet()) {
            Integer id = entry.getKey(); // ChunkServer ID
            String hostName = chunkServerSocketMap.get(id).getInetAddress().getHostName();
            System.out.println("-----------------------------------------");
            System.out.println("Chunk Server: " + hostName);
            System.out.println("[*] Free Space: " + chunkServerFreeSpaceMap.get(id) + " MB");
            for (String chunk : entry.getValue()) {
                System.out.println("[*] " + chunk);
            }
        }
    }

    /**
     * Print information about files in the DFS
     */
    public void printFiles() {
        for (FileInfo fileInfo : fileInfos) {
            System.out.println(fileInfo.getFileName() + ": (size = " + fileInfo.getFileSize() +
                    " KB, #chunks: " + fileInfo.getNoOfChunks() + ")");
        }
    }

    // TODO: implement heartbeat to detect ChunkServer failures
}
