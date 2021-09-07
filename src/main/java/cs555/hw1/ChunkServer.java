package cs555.hw1;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ChunkServer implements Node {
    private static final Logger log = LogManager.getLogger(ChunkServer.class);
    private final String host;
    private final int port;

    public ChunkServer(String host, int port) {
        this.host = host;
        this.port = port;
    }
}
