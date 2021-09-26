package cs555.hw1.wireformats;

import cs555.hw1.util.EventValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class StoreChunk extends Event {
    private static final Logger log = LogManager.getLogger(StoreChunk.class);

    private Socket socket;

    private byte[] chunk;
    private int sequenceNumber;
    private int version;
    private String fileName;
    // number of next chunk servers (to forward the chunk)
    private int noOfNextChunkServers;
    private String[] nextChunkServerHosts;
    private int[] nextChunkServerPorts;

    public StoreChunk() {

    }

    public StoreChunk(byte[] marshalledBytes) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(baInputStream);

        byte messageType = din.readByte();
        EventValidator.validateEventType(messageType, getType(), log);

        // read sequence number
        sequenceNumber = din.readInt();

        // read version
        version = din.readInt();

        // read fileName
        int fileNameLength = din.readInt();
        byte[] fileNameBytes = new byte[fileNameLength];
        din.readFully(fileNameBytes, 0, fileNameLength);
        fileName = new String(fileNameBytes);

        // read chunk
        int chunkLength = din.readInt();
        chunk = new byte[chunkLength];
        din.readFully(chunk, 0, chunkLength);

        // read noOfNextChunkServers
        noOfNextChunkServers = din.readInt();
        nextChunkServerHosts = new String[noOfNextChunkServers];
        nextChunkServerPorts = new int[noOfNextChunkServers];

        // read nextChunkServer hosts and ports
        for (int i = 0; i < noOfNextChunkServers; i++) {
            // read host
            int hostLength = din.readInt();
            byte[] hostBytes = new byte[hostLength];
            din.readFully(hostBytes, 0, hostLength);
            nextChunkServerHosts[i] = new String(hostBytes);

            // read port
            nextChunkServerPorts[i] = din.readInt();
        }

        baInputStream.close();
        din.close();
    }

    @Override
    public byte[] getBytes() {
        byte[] marshalledBytes = null;
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));

        try {
            dout.writeByte(getType());

            // write sequence number
            dout.writeInt(sequenceNumber);

            // write version
            dout.writeInt(version);

            // write fileName
            dout.writeInt(fileName.getBytes().length);
            dout.write(fileName.getBytes());

            // write chunk
            dout.writeInt(chunk.length);
            dout.write(chunk);

            assert (nextChunkServerHosts.length == nextChunkServerPorts.length &&
                    nextChunkServerHosts.length == noOfNextChunkServers);

            // write number of next servers
            dout.writeInt(noOfNextChunkServers);

            for (int i = 0; i < noOfNextChunkServers; i++) {
                // write host
                String host = nextChunkServerHosts[i];
                log.debug("Sending host {}: {}", (i + 1), host);
                dout.writeInt(host.getBytes().length);
                dout.write(host.getBytes());

                // write port
                dout.writeInt(nextChunkServerPorts[i]);
                log.debug("Sending port {}: {}", (i + 1), nextChunkServerPorts[i]);
            }

            dout.flush();

            marshalledBytes = baOutputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return marshalledBytes;
    }

    @Override
    public int getType() {
        return Protocol.STORE_CHUNK;
    }

    public byte[] getChunk() {
        return chunk;
    }

    public void setChunk(byte[] chunk) {
        this.chunk = chunk;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(int sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public String[] getNextChunkServerHosts() {
        return nextChunkServerHosts;
    }

    public void setNextChunkServerHosts(String[] nextChunkServerHosts) {
        this.nextChunkServerHosts = nextChunkServerHosts;
    }

    public int[] getNextChunkServerPorts() {
        return nextChunkServerPorts;
    }

    public void setNextChunkServerPorts(int[] nextChunkServerPorts) {
        this.nextChunkServerPorts = nextChunkServerPorts;
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

    public int getNoOfNextChunkServers() {
        return noOfNextChunkServers;
    }

    public void setNoOfNextChunkServers(int noOfNextChunkServers) {
        this.noOfNextChunkServers = noOfNextChunkServers;
    }
}
