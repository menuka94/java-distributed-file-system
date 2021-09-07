package cs555.hw1;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

public class Controller implements Node {
    private static final Logger log = LogManager.getLogger(Controller.class);
    private static String host = Constants.Controller.HOST;
    private static int port = Integer.parseInt(Constants.Controller.PORT);

    private final ArrayList<ChunkServer> chunkServers;

    // singleton instance
    private static volatile Controller instance;

    private Controller() {
        chunkServers = new ArrayList<>();
    }

    public static Controller getInstance() {
        log.info("getInstance()");
        if (instance != null) {
            return instance;
        }
        synchronized (ChunkServer.class) {
            if (instance == null) {
                instance = new Controller();
            }

            return instance;
        }
    }

    public boolean addFile(byte[] file) {
        boolean success = false;

        return success;
    }

    public void listChunkServers() {
        System.out.println("No. of ChunkServers: " + chunkServers.size());
    }
}
