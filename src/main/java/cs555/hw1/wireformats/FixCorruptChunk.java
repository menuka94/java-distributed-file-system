package cs555.hw1.wireformats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.Socket;

public class FixCorruptChunk extends Event {
    private static final Logger log = LogManager.getLogger();

    private Socket socket;
    private String chunkName;
    private String chunkServerHost;
    private String chunkServerHostname;
    private String chunkServerPort;

    public FixCorruptChunk() {

    }

    public FixCorruptChunk(byte[] marshalledBytes) {

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

    public String getChunkServerHost() {
        return chunkServerHost;
    }

    public void setChunkServerHost(String chunkServerHost) {
        this.chunkServerHost = chunkServerHost;
    }

    public String getChunkServerHostname() {
        return chunkServerHostname;
    }

    public void setChunkServerHostname(String chunkServerHostname) {
        this.chunkServerHostname = chunkServerHostname;
    }

    public String getChunkServerPort() {
        return chunkServerPort;
    }

    public void setChunkServerPort(String chunkServerPort) {
        this.chunkServerPort = chunkServerPort;
    }
}
