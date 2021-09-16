package cs555.hw1.models;

import cs555.hw1.util.Constants;
import cs555.hw1.util.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;

public class Chunk {
    private static final Logger log = LogManager.getLogger(Chunk.class);

    private int version;
    private int sequenceNumber;
    private String fileName;
    private boolean valid;

    public Chunk() {

    }

    public Chunk(int version, int sequenceNumber, String fileName) {
        this.version = version;
        this.sequenceNumber = sequenceNumber;
        this.fileName = fileName;
        this.valid = true;
    }

    // last updated timestamp
    private String timeStamp;

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

    public byte[] getChunkFromDisk() throws IOException {
        String readFilePath = Constants.CHUNK_DIR + File.separator + fileName +
                Constants.ChunkServer.EXT_DATA_CHUNK + sequenceNumber;
        log.info("readFilePath: {}", readFilePath);
        return FileUtil.readFileAsBytes(readFilePath);
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
