package cs555.hw1.wireformats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.Socket;

public class ReadFileResponse extends Event {
    private static final Logger log = LogManager.getLogger(ReadFileResponse.class);

    private Socket socket;
    private String fileName;
    private int noOfChunks;

    public ReadFileResponse() {

    }

    public ReadFileResponse(byte[] marshalledBytes) {

    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }

    @Override
    public int getType() {
        return Protocol.READ_FILE_RESPONSE;
    }

    @Override
    public Socket getSocket() {
        return socket;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getNoOfChunks() {
        return noOfChunks;
    }

    public void setNoOfChunks(int noOfChunks) {
        this.noOfChunks = noOfChunks;
    }
}
