package cs555.hw1.node;

import cs555.hw1.InteractiveCommandParser;
import cs555.hw1.transport.TCPConnection;
import cs555.hw1.transport.TCPConnectionsCache;
import cs555.hw1.transport.TCPServerThread;
import cs555.hw1.util.Constants;
import cs555.hw1.util.FileUtil;
import cs555.hw1.wireformats.ClientRequestsChunkServersFromController;
import cs555.hw1.wireformats.ControllerSendsClientChunkServers;
import cs555.hw1.wireformats.Event;
import cs555.hw1.wireformats.Protocol;
import cs555.hw1.wireformats.RegisterClient;
import cs555.hw1.wireformats.ReportClientRegistration;
import cs555.hw1.wireformats.RetrieveChunkRequest;
import cs555.hw1.wireformats.RetrieveChunkResponse;
import cs555.hw1.wireformats.RetrieveFileRequest;
import cs555.hw1.wireformats.RetrieveFileResponse;
import cs555.hw1.wireformats.SendFileInfo;
import cs555.hw1.wireformats.StoreChunk;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Client: responsible for storing, retrieving, updating files, splitting a file into chunks,
 * and assembling the file back using chunks during retrieval.
 */
public class Client implements Node {
    private static final Logger log = LogManager.getLogger(Client.class);

    private int port;
    private InteractiveCommandParser commandParser;
    private Socket controllerSocket;
    private TCPConnection controllerConnection;
    private TCPServerThread tcpServerThread;
    private TCPConnectionsCache tcpConnectionsCache;

    // store ChunkServers returned by controller (overwritten for each call)
    private ArrayList<Socket> chunkServerSockets;

    // map to store chunks when retrieving a file
    private volatile ConcurrentHashMap<String, byte[]> readingChunksMap;

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Not enough arguments to start Client. " +
                    "Enter <controller-host> and <controller-port>");
            System.exit(1);
        } else if (args.length > 2) {
            System.out.println("Invalid number of arguments. Provide <controller-host> and <controller-port>");
        }

        String controllerHost = args[0];
        int controllerPort = Integer.parseInt(args[1]);
        Socket controllerSocket = new Socket(controllerHost, controllerPort);
        Client client = new Client(controllerSocket);
        client.initialize();
        //        TCPConnection tcpConnection;
        //        if (client.tcpConnectionsCache.containsConnection(controllerSocket)) {
        //            log.info("Connection found in TCPConnectionsCache");
        //            tcpConnection = client.tcpConnectionsCache.getConnection(controllerSocket);
        //        } else {
        //            log.info("Connection not found in TCPConnectionsCache. Creating new connection");
        //            tcpConnection = new TCPConnection(controllerSocket, client);
        //        }
    }

    public Client(Socket controllerSocket) throws IOException {
        log.info("Initializing Client on {}", System.getenv("HOSTNAME"));
        this.controllerSocket = controllerSocket;
        controllerConnection = new TCPConnection(controllerSocket, this);
        tcpConnectionsCache = new TCPConnectionsCache();
        tcpServerThread = new TCPServerThread(0, this, tcpConnectionsCache);
        commandParser = new InteractiveCommandParser(this);
        sendRegistrationRequestToController();
    }

    public void initialize() {
        commandParser.start();
        tcpServerThread.start();
    }

    private void sendInitialFileInfo(String fileName, int noOfChunks, int fileSize) throws IOException {
        SendFileInfo sendFileInfo = new SendFileInfo();
        sendFileInfo.setFileName(fileName);
        sendFileInfo.setNoOfChunks(noOfChunks);
        sendFileInfo.setFileSize(fileSize);

        controllerConnection.sendData(sendFileInfo.getBytes());
    }


    /**
     * Add new file to the DFS
     *
     * @param filePath
     * @throws IOException
     */
    public synchronized void addFile(String filePath) throws IOException {
        log.info("addFile: (file = {})", filePath);

        String fileName = Paths.get(filePath).getFileName().toString();
        log.info("fileName: {}", fileName);

        // read file contents
        byte[] bytes = FileUtil.readFileAsBytes(filePath);

        // split file into chunks
        List<byte[]> chunks = FileUtil.splitFile(bytes, Constants.CHUNK_SIZE);
        log.info("No. of chunks: {}", chunks.size());

        sendInitialFileInfo(fileName, chunks.size(), bytes.length);

        for (int i = 0; i < chunks.size(); i++) {
            log.info("Writing chunk: {}", i + 1);
            // contact controller and get a list of 3 chunk servers
            sendChunkServerRequestToController();

            while (chunkServerSockets == null || chunkServerSockets.isEmpty()) {
                // wait for chunkServerSockets object to get populated
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    log.error("Error while waiting for chunkServers to get populated.");
                    log.error(e.getLocalizedMessage());
                    e.printStackTrace();
                }
            }

            // establish connection with ChunkServer A
            TCPConnection chunkServerConnectionA;
            Socket socketA = chunkServerSockets.get(0);
            if (tcpConnectionsCache.containsConnection(socketA)) {
                chunkServerConnectionA = tcpConnectionsCache.getConnection(socketA);
            } else {
                chunkServerConnectionA = new TCPConnection(socketA, this);
            }

            StoreChunk storeChunk = new StoreChunk();
            storeChunk.setChunk(chunks.get(i));
            storeChunk.setFileName(fileName);
            storeChunk.setSequenceNumber(i + 1);
            storeChunk.setVersion(1);

            String[] nextChunkServerHosts = new String[Constants.REPLICATION_LEVEL - 1];
            int[] nextChunkServerPorts = new int[Constants.REPLICATION_LEVEL - 1];

            Socket socketB = chunkServerSockets.get(1);
            Socket socketC = chunkServerSockets.get(2);

            nextChunkServerHosts[0] = socketB.getInetAddress().getHostAddress();
            nextChunkServerPorts[0] = socketB.getPort();

            nextChunkServerHosts[1] = socketC.getInetAddress().getHostAddress();
            nextChunkServerPorts[1] = socketC.getPort();

            storeChunk.setNextChunkServerHosts(nextChunkServerHosts);
            storeChunk.setNextChunkServerPorts(nextChunkServerPorts);
            storeChunk.setNoOfNextChunkServers(Constants.REPLICATION_LEVEL - 1);

            chunkServerConnectionA.sendData(storeChunk.getBytes());

            log.info("Chunk {} processed. Proceeding to the next chunk.", i + 1);
            chunkServerSockets.clear();

            // contact controller and get a list of 3 chunk servers
            sendChunkServerRequestToController();
        } // end for each chunk loop

        // contact the 3 chunk servers (A, B, C) to store the file
        // Client only writes to the first chunk server A, which is responsible for forwarding the chunk to B,
        // which in turn is responsible for forwarding it to C.
        // After the first 64KB chunk of a file has been written, the client contacts the Controller
        // to write the next chunk and repeat the process.
        // Chunk data will be sent to the chunk servers and not the controller. The controller is only
        // responsible for pointing the client to the chunk servers:
        // chunk data should not flow through the controller.
    }


    /**
     * Retrieve stored file from the DFS
     *
     * @param fileName
     */
    public synchronized void retrieveFile(String fileName) throws IOException {
        RetrieveFileRequest retrieveFileRequest = new RetrieveFileRequest();
        retrieveFileRequest.setFileName(fileName);

        controllerConnection.sendData(retrieveFileRequest.getBytes());
    }

    /**
     * Request information about 3 Chunk Servers from Controller to store a new file
     *
     * @throws IOException
     */
    private synchronized void sendChunkServerRequestToController() throws IOException {
        log.info("sendChunkServerRequestToController()");
        ClientRequestsChunkServersFromController requestChunkServersEvent =
                new ClientRequestsChunkServersFromController();
        requestChunkServersEvent.setSocket(controllerConnection.getSocket());
        controllerConnection.sendData(requestChunkServersEvent.getBytes());
    }

    public void printHost() {
        String host = controllerConnection.getLocalHostname();
        int localPort = controllerConnection.getLocalPort();
        System.out.println("Host: " + host + ", Port: " + localPort);
    }

    @Override
    public void onEvent(Event event) {
        int type = event.getType();
        switch (type) {
            case Protocol.REPORT_CLIENT_REGISTRATION:
                handleControllerRegistrationResponse(event);
                break;
            case Protocol.CONTROLLER_SENDS_CLIENT_CHUNK_SERVERS:
                handleControllerSendsChunkServers(event);
                break;
            case Protocol.RETRIEVE_FILE_RESPONSE:
                try {
                    handleRetrieveFileResponse(event);
                } catch (IOException e) {
                    log.error(e.getLocalizedMessage());
                    e.printStackTrace();
                }
                break;
            case Protocol.RETRIEVE_CHUNK_RESPONSE:
                handleRetrieveChunkResponse(event);
                break;
            default:
                log.warn("Unknown event type");
        }
    }

    private void handleRetrieveChunkResponse(Event event) {
        RetrieveChunkResponse response = (RetrieveChunkResponse) event;
        String chunkName = response.getChunkName();
        byte[] chunk = response.getChunk();
        String chunkHash = response.getChunkHash();
        if (!FileUtil.hash(chunk).equals(chunkHash)) {
            log.warn("{}'s hashes do not match!", chunkName);
        } else {
            log.info("{}'s integrity confirmed!", chunkName);
        }

        if (readingChunksMap == null) {
            log.warn("readingChunksMap has not been initialized");
            throw new NullPointerException();
        } else {
            readingChunksMap.put(chunkName, chunk);
        }
    }

    public class FileAssembler extends Thread {
        private final Logger log = LogManager.getLogger(FileAssembler.class);

        private String fileName;
        private int noOfChunks;
        private TreeSet<String> expectedChunkNames;

        public FileAssembler(String fileName, int noOfChunks) {
            this.fileName = fileName;
            this.noOfChunks = noOfChunks;
            expectedChunkNames = new TreeSet<>();
            for (int i = 0; i < noOfChunks; i++) {
                expectedChunkNames.add(fileName + Constants.ChunkServer.EXT_DATA_CHUNK + (i + 1));
            }
        }

        @Override
        public void run() {
            log.info("Starting for file {} (#chunks={})", fileName, noOfChunks);
            TreeSet<String> readingKeySet = null;
            while (!expectedChunkNames.equals(readingKeySet)) {
                readingKeySet = new TreeSet<>(readingChunksMap.keySet());
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    log.error("Error while waiting for chunk retrieval");
                    log.error(e.getLocalizedMessage());
                    e.printStackTrace();
                }
            }

            // all chunks have been received. Proceed to assembling the file.
            log.info("Received all chunks for {}", fileName);
            ArrayList<byte[]> bytes = new ArrayList<>();
            for (String iChunk : readingKeySet) {
                bytes.add(readingChunksMap.get(iChunk));
            }

            try {
                Files.createDirectories(Paths.get(Constants.CHUNK_DIR));
                FileOutputStream fos = new FileOutputStream(Constants.CHUNK_DIR + File.separator + fileName);
                // combine all chunks
                byte[][] allChunks = new byte[noOfChunks][];
                for (int i = 0; i < noOfChunks; i++) {
                    allChunks[i] = readingChunksMap.get(fileName + Constants.ChunkServer.EXT_DATA_CHUNK + (i + 1));
                }
                fos.write(FileUtil.concat(allChunks));
                //                for (byte[] aByte : bytes) {
                //                    log.info("length of chunk: {}", aByte.length);
                //                    fos.write(aByte);
                //                }

                log.info("Successfully assembled {}!", fileName);
            } catch (FileNotFoundException e) {
                log.error("Error assembling {}", fileName);
                log.error(e.getLocalizedMessage());
                e.printStackTrace();
            } catch (IOException e) {
                log.error(e.getLocalizedMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     * Process response sent by controller containing ChunkServer information for each chunk of the file needed.
     *
     * @param event
     */
    private void handleRetrieveFileResponse(Event event) throws IOException {
        RetrieveFileResponse retrieveFileResponse = (RetrieveFileResponse) event;

        // get information about the file
        String fileName = retrieveFileResponse.getFileName();
        int fileSize = retrieveFileResponse.getFileSize();
        int noOfChunks = retrieveFileResponse.getNoOfChunks();
        String[] chunkServerHosts = retrieveFileResponse.getChunkServerHosts();
        String[] chunkServerHostNames = retrieveFileResponse.getChunkServerHostNames();
        int[] chunkServerPorts = retrieveFileResponse.getChunkServerPorts();

        log.info("ChunkServer information for file '{}': size={}, #chunks={}, hosts={}, ports={}, hostNames={}",
                fileName, fileSize, noOfChunks, chunkServerHosts, chunkServerPorts, chunkServerHostNames);

        // sanity check
        assert (chunkServerHosts.length == chunkServerPorts.length &&
                chunkServerHosts.length == chunkServerHostNames.length);

        // contact chunk servers and retrieve the chunks
        for (int i = 0; i < noOfChunks; i++) {
            Socket socket = new Socket(chunkServerHosts[i], chunkServerPorts[i]);
            TCPConnection tcpConnection;
            if (tcpConnectionsCache.containsConnection(socket)) {
                tcpConnection = tcpConnectionsCache.getConnection(socket);
            } else {
                tcpConnection = new TCPConnection(socket, this);
            }

            RetrieveChunkRequest request = new RetrieveChunkRequest();
            request.setChunkName(fileName + Constants.ChunkServer.EXT_DATA_CHUNK + (i + 1));

            tcpConnection.sendData(request.getBytes());
        }

        // prepare readingChunks map for storing chunks set by ChunkServers
        readingChunksMap = new ConcurrentHashMap<>();

        // start FileAssembler thread
        FileAssembler assembler = new FileAssembler(fileName, noOfChunks);
        assembler.start();
    }

    /**
     * Contact Controller for registration
     *
     * @throws IOException
     */
    private void sendRegistrationRequestToController() throws IOException {
        log.info("sendRegistrationRequestToController()");
        RegisterClient registerClient = new RegisterClient();
        registerClient.setIpAddressLength((byte) controllerConnection.getSocket()
                .getLocalAddress().getAddress().length);
        registerClient.setIpAddress(controllerConnection.getSocket()
                .getLocalAddress().getAddress());
        registerClient.setPort(tcpServerThread.getListeningPort());
        registerClient.setSocket(controllerConnection.getSocket());

        controllerConnection.sendData(registerClient.getBytes());
    }

    /**
     * Process response sent by Controller regarding Registration
     *
     * @param event
     */
    private void handleControllerRegistrationResponse(Event event) {
        log.debug("handleControllerRegistration");
        ReportClientRegistration registrationEvent = (ReportClientRegistration) event;
        int successStatus = registrationEvent.getSuccessStatus();
        String infoString = registrationEvent.getInfoString();
        controllerSocket = registrationEvent.getSocket();

        log.info("{} ({})", infoString, successStatus);
        if (successStatus == -1) {
            log.warn("Registration failed. Exiting...");
            System.exit(-1);
        }
    }

    /**
     * Process response after the Controller sends information about 3 chunk servers
     *
     * @param event
     * @return
     */
    private void handleControllerSendsChunkServers(Event event) {
        log.debug("handleControllerSendsChunkServers(event)");
        ControllerSendsClientChunkServers sendsClientChunkServersEvent =
                (ControllerSendsClientChunkServers) event;
        String[] hosts = sendsClientChunkServersEvent.getChunkServerHosts();
        String[] hostNames = sendsClientChunkServersEvent.getChunkServerHostNames();
        int[] ports = sendsClientChunkServersEvent.getChunkServerPorts();

        ArrayList<Socket> chunkServers = new ArrayList<>();
        System.out.println("Chunk Servers Returned: ");
        for (int i = 0; i < 3; i++) {
            System.out.println(hostNames[i] + " (" + hosts[i] + ":" + ports[i] + ")");
            try {
                chunkServers.add(new Socket(hosts[i], ports[i]));
            } catch (IOException e) {
                log.error("Error creating a Socket from returned Chunk Server (host={}, port={})",
                        hosts[i], ports[i]);
                log.error(e.getLocalizedMessage());
                e.printStackTrace();
            }
        }

        chunkServerSockets = chunkServers;
    }
}
