package org.dfs.node.chunkServer;

import org.dfs.InteractiveCommandParser;
import org.dfs.models.Chunk;
import org.dfs.models.StoredFile;
import org.dfs.node.Node;
import org.dfs.transport.TCPConnection;
import org.dfs.transport.TCPConnectionsCache;
import org.dfs.transport.TCPServerThread;
import org.dfs.util.Constants;
import org.dfs.util.FileUtil;
import org.dfs.wireformats.Event;
import org.dfs.wireformats.FixCorruptChunk;
import org.dfs.wireformats.LivenessHeartbeat;
import org.dfs.wireformats.Protocol;
import org.dfs.wireformats.RegisterChunkServer;
import org.dfs.wireformats.ReportChunkCorruption;
import org.dfs.wireformats.ReportChunkServerRegistration;
import org.dfs.wireformats.RetrieveChunkRequest;
import org.dfs.wireformats.RetrieveChunkResponse;
import org.dfs.wireformats.SendMajorHeartbeat;
import org.dfs.wireformats.SendMinorHeartbeat;
import org.dfs.wireformats.StoreChunk;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Each chunk server will maintain a list of the files that it manages.
 * For each file, the chunk server will maintain information about the chunks that it holds
 * A chunk server will regularly send heartbeats to the controller node.
 * A given chunk server cannot hold more than one replica of a given chunk.
 */
public class ChunkServer implements Node {
    private static final Logger log = LogManager.getLogger(ChunkServer.class);

    private final TCPConnection controllerConnection;
    private final TCPServerThread tcpServerThread;
    private final TCPConnectionsCache tcpConnectionsCache;
    private final InteractiveCommandParser commandParser;

    private final HashMap<String, StoredFile> filesMap;
    private final ConcurrentHashMap<String, ArrayList<String>> sliceHashesMap;
    private final ConcurrentHashMap<String, String> chunkHashesMap;
    private final ArrayList<String> chunks;
    private final ArrayList<String> newChunks;
    private volatile int prevChunkSize;

    private final String hostName;

    public ChunkServer(Socket controllerSocket, int port) throws IOException {
        log.info("Initializing ChunkServer on {}", System.getenv("HOSTNAME"));
        controllerConnection = new TCPConnection(controllerSocket, this);
        filesMap = new HashMap<>();
        chunks = new ArrayList<>();
        newChunks = new ArrayList<>();
        prevChunkSize = 0;
        sliceHashesMap = new ConcurrentHashMap<>();
        chunkHashesMap = new ConcurrentHashMap<>();
        hostName = controllerSocket.getLocalAddress().getHostName();

        tcpConnectionsCache = new TCPConnectionsCache();
        tcpServerThread = new TCPServerThread(port, this, tcpConnectionsCache);
        commandParser = new InteractiveCommandParser(this);
        sendRegistrationRequestToController();
    }

    public void initialize() {
        tcpServerThread.start();
        commandParser.start();

        // initFilesFromDisk();

        Timer minorTimer = new Timer();
        minorTimer.schedule(new MinorHeartbeat(), 0, Constants.ChunkServer.MINOR_HEARTBEAT_INTERVAL);

        Timer majorTimer = new Timer();
        majorTimer.schedule(new MajorHeartbeat(), 0, Constants.ChunkServer.MAJOR_HEARTBEAT_INTERVAL);
    }

    /**
     * Read stored chunks upon start up
     */
    private void initFilesFromDisk() {
        log.info("Reading files from disk");
        File dir = new File(Constants.CHUNK_DIR);
        String[] files = dir.list();
        if (files != null) {
            for (String f : files) {
                if (f.contains(Constants.ChunkServer.EXT_DATA_CHUNK)) {
                    chunks.add(f);

                    // populate filesMap
                    /*
                    String fileName = FileUtil.getFileNameFromChunkName(f);
                    if (filesMap.containsKey(fileName)) {
                        StoredFile storedFile = filesMap.get(fileName);
                        ArrayList<Chunk> chunks = storedFile.getChunks();
                    } else {

                    }
                     */
                }
            }
            prevChunkSize = chunks.size();
        } else {
            log.warn("{} is empty", dir.getPath());
        }
    }


    /**
     * Read stored chunks at any time by minorHeartBeat
     */
    private void ReadFilesFromDisk() {
        log.info("Reading files from disk");
        File dir = new File(Constants.CHUNK_DIR);
        String[] files = dir.list();
        if (files != null) {
            for (String f : files) {
                if (f.contains(Constants.ChunkServer.EXT_DATA_CHUNK)) {
                    chunks.add(f);

                    // populate filesMap
                    /*
                    String fileName = FileUtil.getFileNameFromChunkName(f);
                    if (filesMap.containsKey(fileName)) {
                        StoredFile storedFile = filesMap.get(fileName);
                        ArrayList<Chunk> chunks = storedFile.getChunks();
                    } else {

                    }
                     */
                }
            }
        } else {
            log.warn("{} is empty", dir.getPath());
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("Not enough arguments to start ChunkServer. " +
                    "Enter <controller-host> and <controller-port> <chunk-server-port>");
            System.exit(1);
        } else if (args.length > 3) {
            System.out.println("Invalid number of arguments. Provide <controller-host> <controller-port> <chunk-server-port>");
        }

        String controllerHost = args[0];
        int controllerPort = Integer.parseInt(args[1]);
        int chunkServerPort = Integer.parseInt(args[2]);
        Socket controllerSocket = new Socket(controllerHost, controllerPort);
        ChunkServer chunkServer = new ChunkServer(controllerSocket, chunkServerPort);
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
            case Protocol.RETRIEVE_CHUNK_REQUEST:
                handleRetrieveChunkRequest(event);
                break;
            case Protocol.FIX_CORRUPT_CHUNK:
                try {
                    handleFixCorruptChunk(event);
                } catch (IOException e) {
                    log.error("Error storing chunk");
                    log.error(e.getLocalizedMessage());
                    e.printStackTrace();
                }
                break;
            case Protocol.RETRIEVE_CHUNK_RESPONSE:
                try {
                    handleFixCorruptChunkResponse(event);
                } catch (IOException e) {
                    log.error("Error storing chunk");
                    log.error(e.getLocalizedMessage());
                    e.printStackTrace();
                }
                break;
            case Protocol.LIVENESS_HEARTBEAT:
                handleLivenessHeartbeat(event);
                break;
            default:
                log.warn("Unknown event type: {}", type);
        }
    }

    private void handleLivenessHeartbeat(Event event) {
        log.debug("Liveness Heartbeat");
    }

    /**
     * Check if the chunk requested by client is stored in ChunkServer's disk.
     * If it exists, read and send the chunk data
     *
     * @param event
     */
    private synchronized void handleRetrieveChunkRequest(Event event) {
        RetrieveChunkRequest request = (RetrieveChunkRequest) event;
        String chunkName = request.getChunkName();
        log.debug("Searching for Chunk: {}", chunkName);
        boolean chunkFound = chunks.contains(chunkName);
        if (chunkFound) {
            log.debug("{} found", chunkName);
        } else {
            log.warn("{} not found", chunkName);
        }

        // send requested chunk to client
        try {
            byte[] chunkOnDisk = FileUtil.readFileAsBytes(Constants.CHUNK_DIR + File.separator + chunkName);

            // verify the integrity of each slice of the chunks
            // (verify slices on disks against the stored ones)
            ArrayList<String> sliceHashes = FileUtil.getSliceHashesFromChunk(chunkOnDisk);
            ArrayList<String> storedSliceHashes = sliceHashesMap.get(chunkName);  //if chunk server is restarted sliceHashMap might be empty
            assert sliceHashes.size() == storedSliceHashes.size();
            boolean corrupted = false;
            boolean corruptedChunk = false;
            int sliceHashSize = sliceHashes.size();
            if (storedSliceHashes.size() < sliceHashSize) {  // if more information is deleted from the chunk there might not have 8 slices always
                sliceHashSize = storedSliceHashes.size();
            }

            for (int i = 0; i < sliceHashSize; i++) {
                if (!sliceHashes.get(i).equals(storedSliceHashes.get(i))) {
                    log.warn("Slice {} of {} is corrupted", (i + 1), chunkName);
                    corrupted = true;
                }
            }

            if (!corrupted) {
                log.debug("{}'s integrity confirmed through slices!", chunkName);
            } else {
                log.warn("{} is corrupted", chunkName);
            }

            // verity hash of the entire chunk
            String readHash = FileUtil.hash(chunkOnDisk);
            String expectedHash = chunkHashesMap.get(chunkName);
            if (!expectedHash.equals(readHash)) {
                log.warn("Chunk hashes do not match for {}", chunkName);
                corruptedChunk = true;
            } else {
                log.debug("{}'s integrity confirmed!", chunkName);
            }

            //Corruption handling.......
            //if(request.getSocket().getInetAddress().getHostName().contains("pollock")){
            if (corrupted | corruptedChunk) {
                //notify Controller
                ReportChunkCorruption reportChunkCorruption = new ReportChunkCorruption();
                reportChunkCorruption.setChunkName(chunkName);
                try {
                    log.info("ChunkServer {} is notifying controller about chunk corruption", hostName);
                    controllerConnection.sendData(reportChunkCorruption.getBytes());
                } catch (IOException e) {
                    log.error(e.getLocalizedMessage());
                    e.printStackTrace();
                }
            }

            // Using a sleep time the chunk information can be again read for the corrected chunk
            //Now the corrupted chunk will be passed to see the reflection from clientpull

            RetrieveChunkResponse response = new RetrieveChunkResponse();
            response.setChunkName(chunkName);
            response.setChunk(chunkOnDisk);
            //response.setChunkHash(readHash);

            response.setChunkHash(expectedHash);

            Socket clientSocket = request.getSocket();
            TCPConnection clientConnection;
            if (tcpConnectionsCache.containsConnection(clientSocket)) {
                clientConnection = tcpConnectionsCache.getConnection(clientSocket);
            } else {
                log.warn("Connection does not exist in cache");
                clientConnection = new TCPConnection(clientSocket, this);
            }

            clientConnection.sendData(response.getBytes());
            log.info("Sending {} to client", chunkName);
        } catch (IOException e) {
            log.error("Error reading {}", chunkName);
            log.error(e.getLocalizedMessage());
            e.printStackTrace();
        }
    }

    private synchronized void handleFixCorruptChunk(Event event) throws IOException {

        FixCorruptChunk fixCorruptChunkInfo = (FixCorruptChunk) event;

        // send response to Replica chunk Server
        //FixCorruptChunk fixCorruptChunkInfo = new FixCorruptChunk();
        String chunkName = fixCorruptChunkInfo.getChunkName();
        String chunkServerHost = fixCorruptChunkInfo.getChunkServerHost();
        String chunkServerHostName = fixCorruptChunkInfo.getChunkServerHostname();
        int chunkServerPort = fixCorruptChunkInfo.getChunkServerPort();

        //RetrieveFileResponse retrieveFileResponse = (RetrieveFileResponse) event;

        // get information about the file

        log.info("Replica ChunkServer information for Corrupted ChunkName '{}':  hosts={}, ports={}, hostNames={}",
                chunkName, chunkServerHost, chunkServerPort, chunkServerHostName);

        // sanity check
        //  assert (chunkServerHosts.length == chunkServerPorts.length &&
        //         chunkServerHosts.length == chunkServerHostNames.length);

        // contact chunk servers and retrieve the chunks

        Socket socket = new Socket(chunkServerHost, chunkServerPort);
        TCPConnection tcpConnection;
        if (tcpConnectionsCache.containsConnection(socket)) {
            tcpConnection = tcpConnectionsCache.getConnection(socket);
        } else {
            tcpConnection = new TCPConnection(socket, this);
        }

        RetrieveChunkRequest request = new RetrieveChunkRequest();
        request.setChunkName(chunkName);
        tcpConnection.sendData(request.getBytes());

        //  FixCorruptChunkResponse fixRequest = new FixCorruptChunkResponse();
        //  fixRequest.setChunkName(chunkName);


        // prepare readingChunks map for storing chunks sent by ChunkServers
        //  readingChunksMap = new ConcurrentHashMap<>();
        //
        //        // start FileAssembler thread
        //        Client.FileAssembler assembler = new Client.FileAssembler(fileName, noOfChunks);
        //        assembler.start();
    }


    private synchronized void handleFixCorruptChunkResponse(Event event) throws IOException {
        RetrieveChunkResponse response = (RetrieveChunkResponse) event;
        //FixCorruptChunkResponse fixResponse = (FixCorruptChunkResponse) event;

        String chunkName = response.getChunkName();
        byte[] chunk = response.getChunk();
        String chunkHash = response.getChunkHash();
        if (!FileUtil.hash(chunk).equals(chunkHash)) {
            log.warn("{}'s hashes do not match!", chunkName);
        } else {
            Files.createDirectories(Paths.get(Constants.CHUNK_DIR));
            String outputFileName = Constants.CHUNK_DIR + File.separator + chunkName;

            //Overwrite the corrupted chunk
            Files.write(new File(outputFileName).toPath(), chunk);
            log.info("{}'s integrity confirmed!", chunkName);
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
        log.debug("nextChunkServersSize: {}", nextChunkServersSize);
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
        /*
        System.out.println("No. of Chunks: " + chunks.size());
        for (String chunk : chunks) {
            System.out.println(chunk);
        }
        */

        for (Map.Entry<String, StoredFile> entry : filesMap.entrySet()) {
            String fileName = entry.getKey();
            System.out.println("[*] " + fileName);
            StoredFile storedFile = entry.getValue();
            ArrayList<Chunk> chunks = storedFile.getChunks();
            for (Chunk chunk : chunks) {
                System.out.printf("\t[+] %s {timestamp: %s, sequenceNo: %s, version: %s}%n",
                        chunk.getName(), chunk.getTimeStamp(), chunk.getSequenceNumber(), chunk.getVersion());
            }
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

    public class MinorHeartbeat extends TimerTask {
        private final Logger log = LogManager.getLogger(MinorHeartbeat.class);

        @Override
        public void run() {
            SendMinorHeartbeat heartbeat = new SendMinorHeartbeat();

            heartbeat.setNoOfChunks(chunks.size());
            heartbeat.setFreeSpace(getFreeSpaceMB());

            heartbeat.setNoOfNewChunks(chunks.size() - prevChunkSize);
            prevChunkSize = chunks.size();
            heartbeat.setNewChunks(chunks);


            //heartbeat.setNewChunks(newChunks);
            //            if(newChunks.size()>0){
            //                //heartbeat.setNewChunks(newChunks);
            //                chunks.addAll(newChunks);
            //                newChunks.clear();
            //            }
            //else heartbeat.setNewChunks(null);

            // Check for new chunk


            try {
                log.info("ChunkServer {} sending minor heartbeat", hostName);
                controllerConnection.sendData(heartbeat.getBytes());
            } catch (IOException e) {
                log.error(e.getLocalizedMessage());
                e.printStackTrace();
            }
        }
    }

    public class LivenessHeartbeatSender extends TimerTask {
        private final Logger log = LogManager.getLogger(LivenessHeartbeatSender.class);

        @Override
        public void run() {
            LivenessHeartbeat heartbeat = new LivenessHeartbeat();
            try {
                controllerConnection.sendData(heartbeat.getBytes());
            } catch (IOException e) {
                log.error(e.getLocalizedMessage());
                e.printStackTrace();
            }
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
        //Add file name to new chunk list
        newChunks.add(outputFileName);

        Chunk chunkObj = new Chunk(sequenceNumber, version, fileName);
        chunkObj.setTimeStamp(new Date().toString());

        // create 8KB slices from 64KB chunk
        List<byte[]> slices = FileUtil.splitFile(chunk, Constants.SLICE_SIZE);
        ArrayList<String> hashes = new ArrayList<>();
        for (byte[] slice : slices) {
            hashes.add(FileUtil.hash(slice));
        }
        chunkObj.setSliceHashes(hashes);
        log.info("Slice Hashes computed for Chunk({}, sequence-{}, version-{})", fileName, sequenceNumber, version);

        // add entry chunkHashesMap
        chunkHashesMap.put(chunkObj.getName(), Objects.requireNonNull(FileUtil.hash(chunk)));

        // add entry to sliceHashesMap
        ArrayList<String> sliceHashes = FileUtil.getSliceHashesFromChunk(chunk);
        sliceHashesMap.put(chunkObj.getName(), sliceHashes);

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
