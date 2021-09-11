package cs555.hw1.node;

import cs555.hw1.InteractiveCommandParser;
import cs555.hw1.transport.TCPConnection;
import cs555.hw1.transport.TCPConnectionsCache;
import cs555.hw1.transport.TCPServerThread;
import cs555.hw1.wireformats.ClientRequestsChunkServersFromController;
import cs555.hw1.wireformats.ControllerSendsClientChunkServers;
import cs555.hw1.wireformats.Event;
import cs555.hw1.wireformats.Protocol;
import cs555.hw1.wireformats.RegisterClient;
import cs555.hw1.wireformats.ReportClientRegistration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;

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


    public void addFile(String fileName, String filePath) throws IOException {
        log.info("addFile: (fileName = {}, filePath = {})", fileName, filePath);

        // contact controller and get a list of 3 chunk servers
        sendChunkServerRequestToController();

        // contact the 3 chunk servers (A, B, C) to store the file
        // Client only writes to the first chunk server A, which is responsible for forwarding the chunk to B,
        // which in turn is responsible for forwarding it to C.
        // After the first 64KB chunk of a file has been written, the client contacts the Controller
        // to write the next chunk and repeat the process.
        // Chunk data will be sent to the chunk servers and not the controller. The controller is only
        // responsible for pointing the client to the chunk servers:
        // chunk data should not flow through the controller.

    }

    private void sendChunkServerRequestToController() throws IOException {
        log.info("sendChunkServerRequestToController()");
        ClientRequestsChunkServersFromController requestChunkServersEvent =
                new ClientRequestsChunkServersFromController();
        requestChunkServersEvent.setIpAddressLength((byte) controllerConnection.getSocket()
                .getLocalAddress().getAddress().length);
        requestChunkServersEvent.setIpAddress(controllerConnection.getSocket()
                .getLocalAddress().getAddress());
        requestChunkServersEvent.setSocket(controllerConnection.getSocket());
        controllerConnection.sendData(requestChunkServersEvent.getBytes());
    }

    public void printHost() {
        String host = controllerConnection.getLocalHostname();
        int localPort = controllerConnection.getLocalPort();
        System.out.println("Host: " + host + ", Port: " + localPort);
    }

    public void listChunkServers() {
        log.info("listChunkServers");
//        controller.listChunkServers();
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
        }
    }

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

    private void handleControllerRegistrationResponse(Event event) {
        log.info("handleControllerRegistration");
        ReportClientRegistration registrationEvent = (ReportClientRegistration) event;
        int successStatus = registrationEvent.getSuccessStatus();
        String infoString = registrationEvent.getInfoString();
        log.info("{} ({})", infoString, successStatus);
        if (successStatus == -1) {
            log.warn("Controller Registration failed. Exiting...");
            System.exit(-1);
        }
    }

    private void handleControllerSendsChunkServers(Event event) {
        log.info("handleControllerSendsChunkServers(event)");
        ControllerSendsClientChunkServers sendsClientChunkServersEvent =
                (ControllerSendsClientChunkServers) event;
    }
}
