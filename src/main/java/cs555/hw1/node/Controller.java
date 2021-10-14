package cs555.hw1.node;

import cs555.hw1.InteractiveCommandParser;
import cs555.hw1.node.chunkServer.ChunkServer;
import cs555.hw1.transport.TCPConnection;
import cs555.hw1.transport.TCPConnectionsCache;
import cs555.hw1.transport.TCPServerThread;
import cs555.hw1.util.Constants;
import cs555.hw1.util.FileUtil;
import cs555.hw1.util.controller.FileInfo;
import cs555.hw1.wireformats.ControllerSendsClientChunkServers;
import cs555.hw1.wireformats.Event;
import cs555.hw1.wireformats.FixCorruptChunk;
import cs555.hw1.wireformats.LivenessHeartbeat;
import cs555.hw1.wireformats.Protocol;
import cs555.hw1.wireformats.ProtocolLookup;
import cs555.hw1.wireformats.RegisterChunkServer;
import cs555.hw1.wireformats.RegisterClient;
import cs555.hw1.wireformats.ReportChunkCorruption;
import cs555.hw1.wireformats.ReportChunkServerRegistration;
import cs555.hw1.wireformats.ReportClientRegistration;
import cs555.hw1.wireformats.RetrieveFileRequest;
import cs555.hw1.wireformats.RetrieveFileResponse;
import cs555.hw1.wireformats.SendFileInfo;
import cs555.hw1.wireformats.SendMajorHeartbeat;
import cs555.hw1.wireformats.SendMinorHeartbeat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
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

    private final int port;
    private final TCPServerThread tcpServerThread;
    private final TCPConnectionsCache tcpConnectionsCache;

    private final InteractiveCommandParser commandParser;
    private Socket clientSocket;
    private TCPConnection clientConnection;

    // ChunkServerID, Socket
    private final ConcurrentHashMap<Integer, Socket> chunkServerSocketMap;

    // ChunkServerID, ChunkInfo
    private final ConcurrentHashMap<Integer, ArrayList<String>> chunkServerChunksMap;

    private final ConcurrentHashMap<Integer, Long> chunkServerFreeSpaceMap;

    // ChunkServerID, Port
    private final ConcurrentHashMap<Integer, Integer> chunkServerListeningPortMap;

    // File names and number of chunks (for all files in the system)
    private final Vector<FileInfo> fileInfos;

    private final Random random;

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

        Timer minorTimer = new Timer();
        minorTimer.schedule(new LivenessCheck(), 0, Constants.ChunkServer.LIVENESS_HEARTBEAT_INTERVAL);
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
        log.debug("Event type: {}", type);
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
            case Protocol.RETRIEVE_FILE_REQUEST:
                handleRetrieveFileRequest(event);
                break;
            case Protocol.SEND_FILE_INFO:
                handleSendFileInfo(event);
                break;
            case Protocol.REPORT_CHUNK_CORRUPTION:
                handleReportChunkCorruption(event);
                break;
            case Protocol.LIVENESS_HEARTBEAT:
                handleLivenessHeartbeat(event);
                break;
            // case Protocol.FIX_CORRUPT_CHUNK:
            //     handleChunkFixRequest(event);
            //     break;
            default:
                log.warn("Unknown event type: {}", type);
        }
    }

    private synchronized void handleLivenessHeartbeat(Event event) {
        cs555.hw1.wireformats.LivenessHeartbeat response = (cs555.hw1.wireformats.LivenessHeartbeat) event;
        Socket socket = response.getSocket();
        if (tcpConnectionsCache.containsConnection(socket)) {
            // valid chunk server
            String hostName = socket.getInetAddress().getHostName();
            log.info("LivenessHeartbeat received from ChunkServer {}", hostName);
        } else {
            log.warn("LivenessHeartbeat: ChunkServer connection not found in TCPConnectionsCache");
        }
    }


    /**
     * Periodically check the connections to all registered ChunkServers
     */
    public class LivenessCheck extends TimerTask {
        private final Logger log = LogManager.getLogger(LivenessCheck.class);

        @Override
        public void run() {
            // for each chunk server
            for (Map.Entry<Integer, Socket> entry : chunkServerSocketMap.entrySet()) {
                int chunkServerId = entry.getKey();
                Socket socket = entry.getValue();
                String hostName = socket.getInetAddress().getHostName();
                try {
                    TCPConnection connection = tcpConnectionsCache.getConnection(socket);
                    LivenessHeartbeat heartbeat = new LivenessHeartbeat();
                    connection.sendData(heartbeat.getBytes());
                    log.debug("ChunkServer {} is active", hostName);
                } catch (IOException e) {
                    log.warn("ChunkServer {} is not responding", hostName);
                    // remove chunk server from all maps
                    chunkServerSocketMap.remove(chunkServerId);
                    chunkServerFreeSpaceMap.remove(chunkServerId);
                    chunkServerChunksMap.remove(chunkServerId);
                    chunkServerListeningPortMap.remove(chunkServerId);

                    log.debug(e.getLocalizedMessage());
                }
            }
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
    private void handleRetrieveFileRequest(Event event) {
        RetrieveFileRequest retrieveFileRequest = (RetrieveFileRequest) event;
        String fileName = retrieveFileRequest.getFileName();

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
        String[] chunkServerHostNames = new String[noOfChunks];
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
                    chunkServerHostNames[i] = socket.getInetAddress().getHostName();
                    chunkServerPorts[i] = chunkServerListeningPortMap.get(chunkServerId);
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
        RetrieveFileResponse retrieveFileResponse = new RetrieveFileResponse();
        retrieveFileResponse.setFileName(fileName);
        retrieveFileResponse.setNoOfChunks(noOfChunks);
        retrieveFileResponse.setFileSize(fileSize);
        retrieveFileResponse.setChunkServerHosts(chunkServerHosts);
        retrieveFileResponse.setChunkServerHostNames(chunkServerHostNames);
        retrieveFileResponse.setChunkServerPorts(chunkServerPorts);

        try {
            clientConnection.sendData(retrieveFileResponse.getBytes());
        } catch (IOException e) {
            log.error(e.getLocalizedMessage());
            e.printStackTrace();
        }
    }

    private synchronized void handleReportChunkCorruption(Event event) {
        ReportChunkCorruption reportChunkCorruption = (ReportChunkCorruption) event;
        String corruptedChunkName = reportChunkCorruption.getChunkName();
        Socket socket = reportChunkCorruption.getSocket();
        String chunkServerHostname = socket.getInetAddress().getHostName();


        log.warn("Corrupted Chunk! {} has been corrupted at ChunkServer '{}'",
                corruptedChunkName, chunkServerHostname);

        //        String fileName = corruptionChunkName.split("_")[0]; //retrieveFileRequest.getFileName();
        String fileName = FileUtil.getFileNameFromChunkName(corruptedChunkName);

        log.info("Trying to Recover....: {}", fileName);

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
        noOfChunks = 1; // only the corrupted chunk
        String chunkServerHosts = ""; //new String();
        String chunkServerHostNames = "";// new String();
        int chunkServerPorts = 0;

        //for (int i = 0; i < noOfChunks; i++) {
        String chunkName = corruptedChunkName; //fileName + Constants.ChunkServer.EXT_DATA_CHUNK + (i + 1);

        // iterate through map <ChunkServerID, chunkNames>
        for (Map.Entry<Integer, ArrayList<String>> entry : chunkServerChunksMap.entrySet()) {
            if (entry.getValue().contains(chunkName) && chunkServerSocketMap.get(entry.getKey()) != reportChunkCorruption.getSocket()) {
                // found a chunk server containing the chunk we're looking for
                int chunkServerId = entry.getKey();
                Socket socket2 = chunkServerSocketMap.get(chunkServerId);
                chunkServerHosts = socket2.getInetAddress().getHostAddress();
                chunkServerHostNames = socket2.getInetAddress().getHostName();
                chunkServerPorts = chunkServerListeningPortMap.get(chunkServerId);
            }
        }

        if ("".equals(chunkServerHosts)) {
            log.error("No Replica ChunkServer with the needed chunk found.");
        }

        // send response to chunk Server
        FixCorruptChunk fixCorruptChunkInfo = new FixCorruptChunk();
        fixCorruptChunkInfo.setChunkName(corruptedChunkName);
        fixCorruptChunkInfo.setChunkServerHost(chunkServerHosts);
        fixCorruptChunkInfo.setChunkServerHostname(chunkServerHostNames);
        fixCorruptChunkInfo.setChunkServerPort(chunkServerPorts);

        TCPConnection tcpConnection = tcpConnectionsCache.getConnection(reportChunkCorruption.getSocket());
        try {
            tcpConnection.sendData(fixCorruptChunkInfo.getBytes());
        } catch (IOException e) {
            log.error(e.getLocalizedMessage());
            e.printStackTrace();
        }
    }


    private synchronized void handleMinorHeartbeat(Event event) {
        SendMinorHeartbeat heartbeat = (SendMinorHeartbeat) event;
        Socket socket = heartbeat.getSocket();
        String chunkServerHostname = socket.getInetAddress().getHostName();
        long freeSpace = heartbeat.getFreeSpace();
        int noOfChunks = heartbeat.getNoOfChunks();
        int noOfNewChunks = heartbeat.getNoOfNewChunks();


        //update chunks/files map
        ArrayList<String> chunks = heartbeat.getNewChunks();
        for (Map.Entry<Integer, Socket> entry : chunkServerSocketMap.entrySet()) {
            if (socket == entry.getValue()) {
                chunkServerChunksMap.put(entry.getKey(), chunks);
                chunkServerFreeSpaceMap.put(entry.getKey(), freeSpace);
            }
        }

        log.info("Minor Heartbeat received from ChunkServer '{}': (freeSpace={} KB, #chunks={}, #newChunks={})"
                , chunkServerHostname, freeSpace, noOfChunks, noOfNewChunks);
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
                chunkServerFreeSpaceMap.put(entry.getKey(), freeSpace);
            }
        }

        log.info("Major Heartbeat received from ChunkServer '{}': (freeSpace={} KB, #chunks={})",
                chunkServerHostname, freeSpace, noOfChunks);
    }

    private synchronized void registerChunkServer(Event event) {
        // Process Request
        RegisterChunkServer registerChunkServer = (RegisterChunkServer) event;

        byte[] ipAddress = registerChunkServer.getIpAddress();
        int port = registerChunkServer.getPort();

        int randomId = 0;

        // Start Response
        ReportChunkServerRegistration responseEvent = new ReportChunkServerRegistration();
        Socket chunkServerSocket = registerChunkServer.getSocket();
        if (!Arrays.equals(ipAddress,
                chunkServerSocket.getInetAddress().getAddress())) {
            log.warn("IP Addresses differ");
            responseEvent.setSuccessStatus(-1);
            String infoString = "Registration IP Address and origin IP Address do not match";
            responseEvent.setInfoString(infoString);
            responseEvent.setLengthOfString((byte) infoString.getBytes().length);
        } else if (tcpConnectionsCache.containsConnection(chunkServerSocket)) {
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

        log.info("Registering ChunkServer (IPAddress: {}, Host: {}, Port: {})",
                new String(ipAddress), chunkServerSocket.getInetAddress().getHostName(), port);

        TCPConnection tcpConnection = tcpConnectionsCache.getConnection(chunkServerSocket);
        try {
            tcpConnection.sendData(responseEvent.getBytes());
            chunkServerSocketMap.put(randomId, chunkServerSocket);
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

        byte[] clientIpAddress = registerClient.getIpAddress();
        int clientPort = registerClient.getPort();

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

        log.info("Registering Client (IPAddress: {}, Host: {}, Port: {})",
                new String(clientIpAddress), clientSocket.getInetAddress().getHostName(), clientPort);

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

        String[] chunkServerHosts = new String[Constants.REPLICATION_LEVEL];
        String[] chunkServerHostNames = new String[Constants.REPLICATION_LEVEL];
        int[] chunkServerPorts = new int[Constants.REPLICATION_LEVEL];

        int noOfLiveChunkServers = chunkServerSocketMap.keySet().size();
        if (noOfLiveChunkServers < Constants.REPLICATION_LEVEL) {
            log.warn("No. of Live Chunk Servers is less than {}. Returning...", Constants.REPLICATION_LEVEL);
            return;
        }

        ArrayList<Integer> selectedChunkServerIDs = getChunkServersWithHighestFreeSpace();

        // select 3 registered chunk servers
        for (int i = 0; i < Constants.REPLICATION_LEVEL; i++) {
            String chunkServerHost = chunkServerSocketMap.get(selectedChunkServerIDs.get(i))
                    .getInetAddress().getHostAddress();
            String chunkServerHostName = chunkServerSocketMap.get(selectedChunkServerIDs.get(i))
                    .getInetAddress().getCanonicalHostName();
            int chunkServerPort = chunkServerListeningPortMap.get(selectedChunkServerIDs.get(i));

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
     * Get a list of ChunkServer IDs that have the highest free space available
     *
     * @return
     */
    private ArrayList<Integer> getChunkServersWithHighestFreeSpace() {
        ArrayList<Integer> ids = new ArrayList<>();
        ArrayList<Long> sortedFreeSpaces = new ArrayList<>(chunkServerFreeSpaceMap.values());
        log.debug("freeSpaces (before sorting): {}", sortedFreeSpaces);
        sortedFreeSpaces.sort(Comparator.reverseOrder());
        log.debug("freeSpaces (after sorting): {}", sortedFreeSpaces);
        List<Long> topNFreeSpaces = sortedFreeSpaces.subList(0, Constants.REPLICATION_LEVEL);

        for (Long freeSpace : topNFreeSpaces) {
            for (Map.Entry<Integer, Long> entry : chunkServerFreeSpaceMap.entrySet()) {
                if (Objects.equals(entry.getValue(), freeSpace)) {
                    if (!ids.contains(entry.getKey())) {
                        ids.add(entry.getKey());
                        break;
                    }
                }
            }
        }

        assert ids.size() == topNFreeSpaces.size();
        assert ids.size() == Constants.REPLICATION_LEVEL;

        log.debug("topNFreeSpaces: {}, ids: {}", topNFreeSpaces, ids);
        return ids;
    }

    /**
     * Print information about chunks in all registered chunk servers
     */
    public synchronized void printChunks(boolean printChunks) {
        for (Map.Entry<Integer, ArrayList<String>> entry : chunkServerChunksMap.entrySet()) {
            Integer id = entry.getKey(); // ChunkServer ID
            String hostName = chunkServerSocketMap.get(id).getInetAddress().getHostName();
            System.out.println("-----------------------------------------");
            System.out.printf("Chunk Server (%d): %s {freeSpace = %s MB, #chunks = %s}%n",
                    id, hostName, chunkServerFreeSpaceMap.get(id), entry.getValue().size());
            if (printChunks) {
                for (String chunk : entry.getValue()) {
                    System.out.println("[*] " + chunk);
                }
            }
        }
    }

    /**
     * Print information about files in the DFS
     */
    public void printFiles() {
        System.out.println("No. of files: " + fileInfos.size());
        for (FileInfo fileInfo : fileInfos) {
            System.out.println(fileInfo.getFileName() + ": (size = " + fileInfo.getFileSize() +
                    " KB, #chunks: " + fileInfo.getNoOfChunks() + ")");
        }
    }

    // TODO: implement heartbeat to detect ChunkServer failures
}
