package cs555.hw1.wireformats;

import cs555.hw1.models.Chunk;
import cs555.hw1.util.EventValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class WriteInitialChunk extends Event {
    private static final Logger log = LogManager.getLogger(WriteInitialChunk.class);

    private Socket socket;
    private byte[] chunk;
    private String fileName;
    private int sequenceNumber;
    private int version;

    private Chunk chunkObj;

    public WriteInitialChunk() {

    }

    public WriteInitialChunk(byte[] marshalledBytes) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

        byte messageType = din.readByte();
        EventValidator.validateEventType(messageType, getType(), log);

        // read sequence number
        sequenceNumber = din.readInt();

        // read version
        version = din.readInt();

/*
        // read chunk
        int chunkSize = din.readByte();
        chunk = new byte[chunkSize];
        din.readFully(chunk, 0, chunkSize);
*/

        // read fileName
        int fileNameLength = din.readByte();
        byte[] fileNameBytes = new byte[fileNameLength];
        din.readFully(fileNameBytes, 0, fileNameLength);
        fileName = new String(fileNameBytes);

        baInputStream.close();
        din.close();
    }

    @Override
    public Socket getSocket() {
        return socket;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    public byte[] getChunk() {
        return chunk;
    }

    public void setChunk(byte[] chunk) {
        this.chunk = chunk;
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

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public byte[] getBytes() {
        byte[] marshalledBytes = null;

        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));

        try {
            dout.writeByte(getType());

            dout.writeInt(sequenceNumber);

            dout.writeInt(version);

/*
            // write chunk
            dout.writeByte(chunk.length);
            dout.write(chunk);
*/

            // write fileName
            dout.writeByte(fileName.getBytes().length);
            dout.write(fileName.getBytes());

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
        return Protocol.WRITE_INITIAL_CHUNK;
    }
}
