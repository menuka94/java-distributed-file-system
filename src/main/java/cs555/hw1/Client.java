package cs555.hw1;

import cs555.hw1.transport.TCPConnection;
import cs555.hw1.transport.TCPConnectionsCache;
import cs555.hw1.transport.TCPServerThread;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;

public class Client implements Node {
    private static final Logger log = LogManager.getLogger(Client.class);

    private int port;
    private InteractiveCommandParser commandParser;
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
        TCPConnection tcpConnection;
        if (client.tcpConnectionsCache.containsConnection(controllerSocket)) {
            log.info("Connection found in TCPConnectionsCache");
            tcpConnection = client.tcpConnectionsCache.getConnection(controllerSocket);
        } else {
            log.info("Connection not found in TCPConnectionsCache. Creating new connection");
            tcpConnection = new TCPConnection(controllerSocket, client);
        }
    }

    public Client(Socket controllerSocket) throws IOException {
        log.info("Initializing Client on {}", System.getenv("HOSTNAME"));
        controllerConnection = new TCPConnection(controllerSocket, this);
        tcpConnectionsCache = new TCPConnectionsCache();
        tcpServerThread = new TCPServerThread(0, this, tcpConnectionsCache);
        commandParser = new InteractiveCommandParser(this);
    }

    public void initialize() {
        commandParser.start();
        tcpServerThread.start();
    }


    public void addFile(String fileName, String filePath) {
        log.info("addFile: (fileName = {}, filePath = {})", fileName, filePath);

        // contact controller and get a list of 3 chunk servers
//        controller.getChunkServersForNewFile();
    }

    public void printHost() {
        System.out.println("printHost()");
    }

    public void listChunkServers() {
        log.info("listChunkServers");
//        controller.listChunkServers();
    }
}
