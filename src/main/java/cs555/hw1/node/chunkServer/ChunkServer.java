package cs555.hw1.node.chunkServer;

import cs555.hw1.InteractiveCommandParser;
import cs555.hw1.models.Chunk;
import cs555.hw1.models.StoredFile;
import cs555.hw1.node.Node;
import cs555.hw1.transport.TCPConnection;
import cs555.hw1.transport.TCPConnectionsCache;
import cs555.hw1.transport.TCPServerThread;
import cs555.hw1.util.Constants;
import cs555.hw1.util.FileUtil;
import cs555.hw1.wireformats.Event;
import cs555.hw1.wireformats.Protocol;
import cs555.hw1.wireformats.RegisterChunkServer;
import cs555.hw1.wireformats.ReportChunkServerRegistration;
import cs555.hw1.wireformats.SendMajorHeartbeat;
import cs555.hw1.wireformats.SendMinorHeartbeat;
import cs555.hw1.wireformats.StoreChunk;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Each chunk server will maintain a list of the files that it manages.
 * For each file, the chunk server will maintain information about the chunks that it holds
 * A chunk server will regularly send heartbeats to the controller node.
 * A given chunk server cannot hold more than one replica of a given chunk.
 */
public class ChunkServer implements Node {
    private static final Logger log = LogManager.getLogger(ChunkServer.class);

    private TCPConnection controllerConnection;
    private TCPServerThread tcpServerThread;
    private TCPConnectionsCache tcpConnectionsCache;
    private InteractiveCommandParser commandParser;

    private volatile HashMap<String, StoredFile> filesMap;
    private volatile ArrayList<String> chunks;
    private String hostName;

    public ChunkServer(Socket controllerSocket) throws IOException {
        log.info("Initializing ChunkServer on {}", System.getenv("HOSTNAME"));
        controllerConnection = new TCPConnection(controllerSocket, this);
        filesMap = new HashMap<>();
        chunks = new ArrayList<>();
        hostName = controllerSocket.getLocalAddress().getHostName();

        tcpConnectionsCache = new TCPConnectionsCache();
        tcpServerThread = new TCPServerThread(0, this, tcpConnectionsCache);
        commandParser = new InteractiveCommandParser(this);
        sendRegistrationRequestToController();
    }

    public void initialize() {
        tcpServerThread.start();
        commandParser.start();

        Timer majorTimer = new Timer();
        majorTimer.schedule(new MajorHeartbeat(), 0, Constants.ChunkServer.MAJOR_HEARTBEAT_INTERVAL);
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Not enough arguments to start ChunkServer. " +
                    "Enter <controller-host> and <controller-port>");
            System.exit(1);
        } else if (args.length > 2) {
            System.out.println("Invalid number of arguments. Provide <controller-host> and <controller-port>");
        }

        String controllerHost = args[0];
        int controllerPort = Integer.parseInt(args[1]);
        Socket controllerSocket = new Socket(controllerHost, controllerPort);
        ChunkServer chunkServer = new ChunkServer(controllerSocket);
        chunkServer.initialize();
        TCPConnection tcpConnection;
        if (chunkServer.tcpConnectionsCache.containsConnection(controllerSocket)) {
            log.info("Connection found in TCPConnectionsCache");
            tcpConnection = chunkServer.tcpConnectionsCache.getConnection(controllerSocket);
        } else {
            log.info("Connection not found in TCPConnectionsCache. Creating new connection");
            tcpConnection = new TCPConnection(controllerSocket, chunkServer);
        }
    }

    private void sendRegistrationRequestToController() throws IOException {
        log.info("sendRegistrationRequestToController");
        RegisterChunkServer registerChunkServer = new RegisterChunkServer();
        registerChunkServer.setIpAddressLength((byte) controllerConnection.getSocket()
                .getLocalAddress().getAddress().length);
        registerChunkServer.setIpAddress(controllerConnection.getSocket()
                .getLocalAddress().getAddress());
        registerChunkServer.setPort(tcpServerThread.getListeningPort());
        registerChunkServer.setSocket(controllerConnection.getSocket());

        controllerConnection.sendData(registerChunkServer.getBytes());
    }

    public void printHost() {
        String host = controllerConnection.getLocalHostname();
        int port = controllerConnection.getLocalPort();
        System.out.println("Host: " + host + ", Port: " + port);
    }

    @Override
    public void onEvent(Event event) {
        int type = event.getType();
        switch (type) {
            case Protocol.REPORT_CHUNK_SERVER_REGISTRATION:
                handleControllerRegistrationResponse(event);
                break;
            case Protocol.STORE_CHUNK:
                try {
                    handleStoreChunk(event);
                } catch (IOException e) {
                    log.error("Error storing chunk");
                    log.error(e.getLocalizedMessage());
                    e.printStackTrace();
                }
                break;
            default:
                log.warn("Unknown event type: {}", type);
        }
    }

    private synchronized void handleStoreChunk(Event event) throws IOException {
        log.debug("handleStoreChunk(event)");
        StoreChunk storeChunk = (StoreChunk) event;
        String fileName = storeChunk.getFileName();
        int sequenceNumber = storeChunk.getSequenceNumber();
        int version = storeChunk.getVersion();
        byte[] chunk = storeChunk.getChunk();

        try {
            writeChunkToDisk(fileName, chunk, sequenceNumber, version);
        } catch (IOException e) {
            log.error("Error writing chunk to disk: (fileName={}, sequence={})", fileName, sequenceNumber);
            log.error(e.getLocalizedMessage());
            e.printStackTrace();
        }

        // forward chunk (replication)
        StoreChunk nextStoreChunkEvent = new StoreChunk();
        nextStoreChunkEvent.setChunk(chunk);
        nextStoreChunkEvent.setVersion(version);
        nextStoreChunkEvent.setSequenceNumber(sequenceNumber);
        nextStoreChunkEvent.setFileName(fileName);

        int nextChunkServersSize = storeChunk.getNoOfNextChunkServers();
        log.info("nextChunkServersSize: {}", nextChunkServersSize);
        String[] nextChunkServerHosts = storeChunk.getNextChunkServerHosts();
        int[] nextChunkServerPorts = storeChunk.getNextChunkServerPorts();

        Socket nextSocket;
        TCPConnection nextConnection;
        if (nextChunkServersSize == 2) {
            // chunk has only been written once (on A). need to forward from A to B
            log.info("Written on A: {}.{}{}", fileName, Constants.ChunkServer.EXT_DATA_CHUNK, sequenceNumber);
            nextSocket = new Socket(nextChunkServerHosts[0], nextChunkServerPorts[0]);
            String[] newNextChunkServerHosts = Arrays.copyOfRange(nextChunkServerHosts, 1, nextChunkServersSize);
            int[] newNextChunkServerPorts = Arrays.copyOfRange(nextChunkServerPorts, 1, nextChunkServersSize);

            nextStoreChunkEvent.setNoOfNextChunkServers(nextChunkServersSize - 1);
            nextStoreChunkEvent.setNextChunkServerHosts(newNextChunkServerHosts);
            nextStoreChunkEvent.setNextChunkServerPorts(newNextChunkServerPorts);

            if (tcpConnectionsCache.containsConnection(nextSocket)) {
                nextConnection = tcpConnectionsCache.getConnection(nextSocket);
            } else {
                nextConnection = new TCPConnection(nextSocket, this);
            }

            log.info("Forwarding {}.{}{} to B", fileName, Constants.ChunkServer.EXT_DATA_CHUNK, sequenceNumber);
            nextConnection.sendData(nextStoreChunkEvent.getBytes());

        } else if (nextChunkServersSize == 1) {
            // chunk has been written twice (on A and B). forward from B to C
            log.info("Replicated on B: {}.{}{}", fileName, Constants.ChunkServer.EXT_DATA_CHUNK, sequenceNumber);
            nextSocket = new Socket(nextChunkServerHosts[0], nextChunkServerPorts[0]);

            nextStoreChunkEvent.setNoOfNextChunkServers(0);
            nextStoreChunkEvent.setNextChunkServerHosts(new String[0]);
            nextStoreChunkEvent.setNextChunkServerPorts(new int[0]);

            if (tcpConnectionsCache.containsConnection(nextSocket)) {
                nextConnection = tcpConnectionsCache.getConnection(nextSocket);
            } else {
                nextConnection = new TCPConnection(nextSocket, this);
            }

            log.info("Forwarding {}.{}{} to C", fileName, Constants.ChunkServer.EXT_DATA_CHUNK, sequenceNumber);
            nextConnection.sendData(nextStoreChunkEvent.getBytes());
        } else if (nextChunkServersSize == 0) {
            // chunk on C
            // already written to disk above
            // replication complete
            log.info("Replicated on C {}.{}{}", fileName, Constants.ChunkServer.EXT_DATA_CHUNK, sequenceNumber);
        } else {
            log.warn("Invalid number of nextChunkServers: {}", nextChunkServersSize);
        }
    }

    /**
     * Print names of all chunks available at the ChunkServer
     */
    public synchronized void printChunks() {
        System.out.println("No. of Chunks: " + chunks.size());
        for (String chunk : chunks) {
            System.out.println(chunk);
        }
    }

    public class MajorHeartbeat extends TimerTask {
        private final Logger log = LogManager.getLogger(MajorHeartbeat.class);

        @Override
        public void run() {
            SendMajorHeartbeat heartbeat = new SendMajorHeartbeat();
            heartbeat.setFreeSpace(getFreeSpaceMB());
            heartbeat.setChunks(chunks);
            heartbeat.setNoOfChunks(chunks.size());
            try {
                log.info("ChunkServer {} sending major heartbeat", hostName);
                controllerConnection.sendData(heartbeat.getBytes());
            } catch (IOException e) {
                log.error(e.getLocalizedMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     * Get free space on node's /tmp drive (in MB)
     *
     * @return
     */
    private static long getFreeSpaceMB() {
        long freeSpaceKB = new File("/tmp").getFreeSpace();
        return freeSpaceKB / 1000;
    }

    public static class MinorHeartbeat extends TimerTask {
        private static final Logger log = LogManager.getLogger(MinorHeartbeat.class);

        @Override
        public void run() {
            SendMinorHeartbeat heartbeat = new SendMinorHeartbeat();
        }
    }

    /**
     * Write Chunk to Disk
     * Calculate and store hashes for 8KB slices
     */
    private void writeChunkToDisk(String fileName, byte[] chunk, int sequenceNumber, int version) throws IOException {
        Files.createDirectories(Paths.get(Constants.CHUNK_DIR));
        String outputFileName = Constants.CHUNK_DIR + File.separator +
                fileName + Constants.ChunkServer.EXT_DATA_CHUNK + sequenceNumber;
        log.info("outputFileName: {}", outputFileName);
        Files.write(new File(outputFileName).toPath(), chunk);

        Chunk chunkObj = new Chunk(sequenceNumber, version, fileName);

        // create 8KB slices from 64KB chunk
        List<byte[]> slices = FileUtil.splitFile(chunk, Constants.SLICE_SIZE);
        ArrayList<String> hashes = new ArrayList<>();
        for (byte[] slice : slices) {
            hashes.add(FileUtil.hash(slice));
        }
        chunkObj.setSliceHashes(hashes);
        log.info("Slice Hashes computed for Chunk({}, sequence-{}, version-{})", fileName, sequenceNumber, version);

        if (!chunks.contains(chunkObj.getName())) {
            chunks.add(chunkObj.getName());
            log.info("{} added to chunks list", chunkObj.getName());
        } else {
            log.warn("{} already exists. Attempting to store the same chunk more than once.", chunkObj.getName());
        }

        if (filesMap.containsKey(fileName)) {
            StoredFile storedFile = filesMap.get(fileName);
            storedFile.addChunk(chunkObj);
        } else {
            StoredFile storedFile = new StoredFile(fileName);
            storedFile.addChunk(chunkObj);
            filesMap.put(fileName, storedFile);
        }
    }


    private void handleControllerRegistrationResponse(Event event) {
        log.info("handleControllerRegistrationResponse(event)");
        ReportChunkServerRegistration registrationEvent = (ReportChunkServerRegistration) event;
        int successStatus = registrationEvent.getSuccessStatus();
        String infoString = registrationEvent.getInfoString();

        log.info("{} ({})", infoString, successStatus);
        if (successStatus == -1) {
            log.warn("Registration failed. Exiting...");
            System.exit(-1);
        }
    }
}