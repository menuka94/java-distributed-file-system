package cs555.hw1.models;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;

public class Chunk implements Serializable {
    private static final Logger log = LogManager.getLogger(Chunk.class);

    private byte[] data;
    private int version;
    private int sequenceNumber;
    private String fileName;
    private boolean valid;

    public Chunk() {

    }

    public Chunk(byte[] data, int version, int sequenceNumber, String fileName) {
        this.data = data;
        this.version = version;
        this.sequenceNumber = sequenceNumber;
        this.fileName = fileName;
        this.valid = true;
    }

    // last updated timestamp
    private String timeStamp;

    public void setData(byte[] data) {
        this.data = data;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public void setSequenceNumber(int sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

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
