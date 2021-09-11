package cs555.hw1.models;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Chunk {
    private static final Logger log = LogManager.getLogger(Chunk.class);

    private byte[] data;
    private int version;
    private int sequenceNumber;
    private boolean valid = true;

    // last updated timestamp
    private String timeStamp;

    public byte[] getData() {
        return data;
    }

    public int getVersion() {
        return version;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public boolean isValid() {
        return valid;
    }

    // Each Chunk keeps track of its own integrity, by maintaining checksums for 8KB slices of the chunk
    // Use SHA-1 which returns a 160-bit digest for a set of bytes

}
