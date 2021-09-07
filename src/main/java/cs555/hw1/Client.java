package cs555.hw1;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

public class Client implements Node {
    private static final Logger log = LogManager.getLogger(Client.class);
    private Controller controller;
    private InteractiveCommandParser commandParser;

    public Client(Controller controller) {
        this.controller = controller;
        commandParser = new InteractiveCommandParser(this);
    }


    public void addFile(String fileName, String filePath) {
        log.info("addFile: (fileName = {}, filePath = {})", fileName, filePath);
        // contact controller and get a list of 3 chunk servers
    }

    public void listChunkServers() {
        controller.listChunkServers();
    }
}
