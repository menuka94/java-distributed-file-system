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

public class ReplicateChunkRequest extends Event {
    private static final Logger log = LogManager.getLogger(ReplicateChunkRequest.class);

    private Socket socket;
    private String fileName;
    private int sequenceNumber;
    private String nextChunkServerHost;
    private int nextChunkServerPort;

    public ReplicateChunkRequest() {

    }

    public ReplicateChunkRequest(byte[] marshalledBytes) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(baInputStream);

        byte messageType = din.readByte();
        EventValidator.validateEventType(messageType, getType(), log);

        // read sequence number
        sequenceNumber = din.readInt();

        // read fileName
        int fileNameLength = din.readInt();
        byte[] fileNameBytes = new byte[fileNameLength];
        din.readFully(fileNameBytes, 0, fileNameLength);
        fileName = new String(fileNameBytes);
        log.info("marshalledBytes fileName: {}", fileName);

        // read nextChunkServerHost
        int nextChunkServerHostLength = din.readInt();
        byte[] nextChunkServerHostBytes = new byte[nextChunkServerHostLength];
        din.readFully(nextChunkServerHostBytes, 0, nextChunkServerHostLength);
        nextChunkServerHost = new String(nextChunkServerHostBytes);

        // read nextChunkServerPort
        nextChunkServerPort = din.readInt();

        baInputStream.close();
        din.close();
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(int sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public String getNextChunkServerHost() {
        return nextChunkServerHost;
    }

    public void setNextChunkServerHost(String nextChunkServerHost) {
        this.nextChunkServerHost = nextChunkServerHost;
    }

    public int getNextChunkServerPort() {
        return nextChunkServerPort;
    }

    public void setNextChunkServerPort(int nextChunkServerPort) {
        this.nextChunkServerPort = nextChunkServerPort;
    }

    @Override
    public Socket getSocket() {
        return socket;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
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

            // write fileName
            log.info("getBytes(): fileName: {}", fileName);
            dout.writeInt(fileName.getBytes().length);
            dout.write(fileName.getBytes());

            // write nextChunkServerHost
            dout.writeInt(nextChunkServerHost.getBytes().length);
            dout.write(nextChunkServerHost.getBytes());

            // write nextChunkServerPort
            dout.writeInt(nextChunkServerPort);

            dout.flush();

            marshalledBytes = baOutputStream.toByteArray();
        } catch (IOException e) {
            log.error(e.getLocalizedMessage());
            e.printStackTrace();
        }

        return marshalledBytes;
    }

    @Override
    public int getType() {
        return Protocol.REPLICATE_CHUNK_REQUEST;
    }
}
