package cs555.hw1.node;

import cs555.hw1.InteractiveCommandParser;
import cs555.hw1.models.Chunk;
import cs555.hw1.models.StoredFile;
import cs555.hw1.transport.TCPConnection;
import cs555.hw1.transport.TCPConnectionsCache;
import cs555.hw1.transport.TCPServerThread;
import cs555.hw1.util.Constants;
import cs555.hw1.util.FileUtil;
import cs555.hw1.wireformats.Event;
import cs555.hw1.wireformats.ForwardChunk;
import cs555.hw1.wireformats.Protocol;
import cs555.hw1.wireformats.RegisterChunkServer;
import cs555.hw1.wireformats.ReplicateChunkRequest;
import cs555.hw1.wireformats.ReportChunkServerRegistration;
import cs555.hw1.wireformats.StoreChunk;
import cs555.hw1.wireformats.WriteInitialChunk;
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

    private HashMap<String, StoredFile> filesMap;

    public ChunkServer(Socket controllerSocket) throws IOException {
        log.info("Initializing ChunkServer on {}", System.getenv("HOSTNAME"));
        controllerConnection = new TCPConnection(controllerSocket, this);
        filesMap = new HashMap<>();

        tcpConnectionsCache = new TCPConnectionsCache();
        tcpServerThread = new TCPServerThread(0, this, tcpConnectionsCache);
        commandParser = new InteractiveCommandParser(this);
        sendRegistrationRequestToController();
    }

    public void initialize() {
        tcpServerThread.start();
        commandParser.start();
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
            case Protocol.WRITE_INITIAL_CHUNK:
//                handleWriteInitialChunk(event);
                log.info("WRITE_INITIAL_CHUNK: Inactive");
                break;
            case Protocol.REPLICATE_CHUNK_REQUEST:
//                handleReplicateChunkRequest(event);
                log.info("REPLICATE_CHUNK_REQUEST: Inactive");
                break;
            case Protocol.FORWARD_CHUNK:
//                handleForwardChunk(event);
                log.info("FORWARD_CHUNK: Inactive");
                break;
            case Protocol.STORE_CHUNK:
                try {
                    handleStoreChunk(event);
                } catch (IOException e) {
                    log.error(e.getLocalizedMessage());
                    e.printStackTrace();
                }
                break;
            default:
                log.warn("Unknown event type: {}", type);
        }
    }

    private synchronized void handleStoreChunk(Event event) throws IOException {
        log.info("handleStoreChunk(event)");
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

        int nextChunkServersSize = storeChunk.getNextChunkServersSize();
        log.info("nextChunkServersSize: {}", nextChunkServersSize);
        String[] nextChunkServerHosts = storeChunk.getNextChunkServerHosts();
        int[] nextChunkServerPorts = storeChunk.getNextChunkServerPorts();

        Socket nextSocket;
        TCPConnection nextConnection;
        if (nextChunkServersSize == 2) {
            // chunk has only been written once (on A). need to forward from A to B
            log.info("Written on A: {}._part{}", fileName, sequenceNumber);
            nextSocket = new Socket(nextChunkServerHosts[0], nextChunkServerPorts[0]);
            String[] newNextChunkServerHosts = Arrays.copyOfRange(nextChunkServerHosts, 1, nextChunkServersSize);
            int[] newNextChunkServerPorts = Arrays.copyOfRange(nextChunkServerPorts, 1, nextChunkServersSize);

            nextStoreChunkEvent.setNextChunkServersSize(nextChunkServersSize - 1);
            nextStoreChunkEvent.setNextChunkServerHosts(newNextChunkServerHosts);
            nextStoreChunkEvent.setNextChunkServerPorts(newNextChunkServerPorts);

            if (tcpConnectionsCache.containsConnection(nextSocket)) {
                nextConnection = tcpConnectionsCache.getConnection(nextSocket);
            } else {
                nextConnection = new TCPConnection(nextSocket, this);
            }

            log.info("Forwarding {}._part{} to B", fileName, sequenceNumber);
            nextConnection.sendData(nextStoreChunkEvent.getBytes());

        } else if (nextChunkServersSize == 1) {
            // chunk has been written twice (on A and B). forward from B to C
            log.info("Replicated on B: {}._part{}", fileName, sequenceNumber);
            nextSocket = new Socket(nextChunkServerHosts[0], nextChunkServerPorts[0]);

            nextStoreChunkEvent.setNextChunkServersSize(0);
            nextStoreChunkEvent.setNextChunkServerHosts(new String[0]);
            nextStoreChunkEvent.setNextChunkServerPorts(new int[0]);

            if (tcpConnectionsCache.containsConnection(nextSocket)) {
                nextConnection = tcpConnectionsCache.getConnection(nextSocket);
            } else {
                nextConnection = new TCPConnection(nextSocket, this);
            }

            log.info("Forwarding {}._part{} to C", fileName, sequenceNumber);
            nextConnection.sendData(nextStoreChunkEvent.getBytes());
        } else if (nextChunkServersSize == 0) {
            // chunk on C
            // already written to disk above
            // replication complete
            log.info("Replicated on C {}._part{}", fileName, sequenceNumber);
        } else {
            log.warn("Invalid number of nextChunkServers: {}", nextChunkServersSize);
        }
    }

    private synchronized void handleForwardChunk(Event event) {
        log.info("handleForwardChunk(event)");
        ForwardChunk forwardChunk = (ForwardChunk) event;
        byte[] chunk = forwardChunk.getChunk();
        int sequenceNumber = forwardChunk.getSequenceNumber();
        int version = forwardChunk.getVersion();
        String fileName = forwardChunk.getFileName();

        try {
            writeChunkToDisk(fileName, chunk, sequenceNumber, version);
        } catch (IOException e) {
            log.error(e.getLocalizedMessage());
            e.printStackTrace();
        }
    }

    private synchronized void handleReplicateChunkRequest(Event event) {
        log.info("handleReplicateChunkRequest(event)");
        ReplicateChunkRequest replicateChunkRequest = (ReplicateChunkRequest) event;
        String fileName = replicateChunkRequest.getFileName();
        if (fileName.equals("")) {
            log.warn("fileName is blank");
        }

        int sequenceNumber = replicateChunkRequest.getSequenceNumber();
        if (sequenceNumber < 1) {
            log.warn("Invalid sequence number: {}", sequenceNumber);
        }
        log.info("fileName: {}, sequence: {}", fileName, sequenceNumber);

        Chunk matchingChunk = null;

        // read chunk from disk
        StoredFile storedFile = filesMap.get(fileName);
        if (storedFile == null) {
            log.error("storedFile is null i.e. chunk not found");
            return;
        }
        log.info("File '{}' found on ChunkServer. Searching for chunk-{}", fileName, sequenceNumber);
        ArrayList<Chunk> chunks = storedFile.getChunks();
        for (Chunk c : chunks) {
            if (sequenceNumber == c.getSequenceNumber()) {
                matchingChunk = c;
            }
        }

        if (matchingChunk == null) {
            log.warn("No matching chunk found");
            System.exit(1);
        }

        String nextChunkServerHost = replicateChunkRequest.getNextChunkServerHost();
        int nextChunkServerPort = replicateChunkRequest.getNextChunkServerPort();

        TCPConnection nextChunkServerConnection;
        try {
            Socket nextChunkServerSocket = new Socket(nextChunkServerHost, nextChunkServerPort);
            if (tcpConnectionsCache.containsConnection(nextChunkServerSocket)) {
                nextChunkServerConnection = tcpConnectionsCache.getConnection(nextChunkServerSocket);
            } else {
                nextChunkServerConnection = new TCPConnection(nextChunkServerSocket, this);
            }
        } catch (IOException e) {
            nextChunkServerConnection = null;
            log.error(e.getLocalizedMessage());
            e.printStackTrace();
        }

        // prepare event
        ForwardChunk forwardChunk = new ForwardChunk();
        try {
            forwardChunk.setChunk(matchingChunk.getChunkFromDisk());
            forwardChunk.setFileName(fileName);
            forwardChunk.setSequenceNumber(matchingChunk.getSequenceNumber());
            forwardChunk.setVersion(matchingChunk.getVersion());

            nextChunkServerConnection.sendData(forwardChunk.getBytes());
        } catch (IOException e) {
            log.error("Error reading chunk from disk");
            log.error(e.getLocalizedMessage());
            e.printStackTrace();
        } catch (NullPointerException e) {
            log.error(e.getLocalizedMessage());
            e.printStackTrace();
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

        if (filesMap.containsKey(fileName)) {
            StoredFile storedFile = filesMap.get(fileName);
            storedFile.addChunk(chunkObj);
        } else {
            StoredFile storedFile = new StoredFile(fileName);
            storedFile.addChunk(chunkObj);
            filesMap.put(fileName, storedFile);
            log.info("Chunk added to filesMap");
        }
    }

    private synchronized void handleWriteInitialChunk(Event event) {
        log.info("handleWriteInitialChunk(event)");
        WriteInitialChunk writeInitialChunk = (WriteInitialChunk) event;
        byte[] chunk = writeInitialChunk.getChunk();
        int sequenceNumber = writeInitialChunk.getSequenceNumber();
        int version = writeInitialChunk.getVersion();
        String fileName = writeInitialChunk.getFileName();

        log.info("FileName: {}, SequenceNo: {}, version: {}",
                fileName, sequenceNumber, version);
        log.info("Hash for received Chunk: {}", FileUtil.hash(chunk));
        log.info("Received Chunk Length: {}", chunk.length);

        // write chunk to disk
        try {
            writeChunkToDisk(fileName, chunk, sequenceNumber, version);
        } catch (IOException e) {
            log.error(e.getLocalizedMessage());
            e.printStackTrace();
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
