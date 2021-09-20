package cs555.hw1.util.controller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Bean class to store initial file information sent by client
 */
public class FileInfo {
    private static final Logger log = LogManager.getLogger(FileInfo.class);

    private String fileName;
    private int noOfChunks;
    private int fileSize; // file size in KB

    public FileInfo(String fileName, int noOfChunks, int fileSize) {
        this.fileName = fileName;
        this.noOfChunks = noOfChunks;
        this.fileSize = fileSize;
    }

    public String getFileName() {
        return fileName;
    }

    public int getNoOfChunks() {
        return noOfChunks;
    }

    public int getFileSize() {
        return fileSize;
    }
}
