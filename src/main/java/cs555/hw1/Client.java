package cs555.hw1;

import cs555.hw1.transport.TCPConnectionsCache;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

public class Client implements Node {
    private static final Logger log = LogManager.getLogger(Client.class);

    private int port;
    private Controller controller;
    private InteractiveCommandParser commandParser;

    public Client(int port, Controller controller) {
        this.port = port;
        this.controller = controller;
        commandParser = new InteractiveCommandParser(this);
        commandParser.start();
    }


    public void addFile(String fileName, String filePath) {
        log.info("addFile: (fileName = {}, filePath = {})", fileName, filePath);

        // contact controller and get a list of 3 chunk servers
        controller.getChunkServersForNewFile();
    }

    public void listChunkServers() {
        log.info("listChunkServers");
        controller.listChunkServers();
    }
}
