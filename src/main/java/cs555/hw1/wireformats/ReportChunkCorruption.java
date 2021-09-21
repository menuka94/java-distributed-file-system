package cs555.hw1.wireformats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.Socket;

public class ReportChunkCorruption extends Event {
    private static final Logger log = LogManager.getLogger(ReportChunkCorruption.class);

    private Socket socket;
    private String chunkName;

    public ReportChunkCorruption() {

    }

    public ReportChunkCorruption(byte[] marshalledBytes) {

    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }

    @Override
    public int getType() {
        return Protocol.FIX_CORRUPT_CHUNK;
    }

    @Override
    public Socket getSocket() {
        return socket;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    public String getChunkName() {
        return chunkName;
    }

    public void setChunkName(String chunkName) {
        this.chunkName = chunkName;
    }
}
