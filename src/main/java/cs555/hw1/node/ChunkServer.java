package cs555.hw1.node;

import cs555.hw1.InteractiveCommandParser;
import cs555.hw1.models.Chunk;
import cs555.hw1.transport.TCPConnection;
import cs555.hw1.transport.TCPConnectionsCache;
import cs555.hw1.transport.TCPServerThread;
import cs555.hw1.util.Constants;
import cs555.hw1.util.FileUtil;
import cs555.hw1.wireformats.Event;
import cs555.hw1.wireformats.Protocol;
import cs555.hw1.wireformats.RegisterChunkServer;
import cs555.hw1.wireformats.ReportChunkServerRegistration;
import cs555.hw1.wireformats.WriteInitialChunk;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

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

    private ArrayList<Chunk> chunks;

    public ChunkServer(Socket controllerSocket) throws IOException {
        log.info("Initializing ChunkServer on {}", System.getenv("HOSTNAME"));
        controllerConnection = new TCPConnection(controllerSocket, this);
        chunks = new ArrayList<>();

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
                handleWriteInitialChunk(event);
                break;
            default:
                log.warn("Unknown event type: {}", type);
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
            Files.createDirectories(Paths.get(Constants.CHUNK_DIR));
            String outputFileName = Constants.CHUNK_DIR + File.separator  +
                    fileName + Constants.ChunkServer.EXT_DATA_CHUNK + sequenceNumber;
            log.info("outputFileName: {}", outputFileName);
            Files.write(new File(outputFileName).toPath(), chunk);

            Chunk chunkObj = new Chunk(chunk, sequenceNumber, version, fileName);
            chunks.add(chunkObj);
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
