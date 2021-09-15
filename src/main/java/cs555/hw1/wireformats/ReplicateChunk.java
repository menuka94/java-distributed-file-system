package cs555.hw1.wireformats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.Socket;

public class ReplicateChunk extends Event {
    private static final Logger log = LogManager.getLogger(ReplicateChunk.class);

    private Socket socket;

    public ReplicateChunk() {

    }

    public ReplicateChunk(byte[] marshalledBytes) {

    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }

    @Override
    public int getType() {
        return Protocol.REPLICATE_CHUNK;
    }
}
