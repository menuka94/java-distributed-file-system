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
    private String testStr;

    public String getTestStr() {
        return testStr;
    }

    public void setTestStr(String testStr) {
        this.testStr = testStr;
    }

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

        // read fileName
        int fileNameLength = din.readByte();
        byte[] fileNameBytes = new byte[fileNameLength];
        din.readFully(fileNameBytes, 0, fileNameLength);
        fileName = new String(fileNameBytes);

        // read testStr
        int testStrLength = din.readByte();
        byte[] testStrBytes = new byte[testStrLength];
        din.readFully(testStrBytes, 0, testStrLength);
        testStr = new String(testStrBytes);

        // read chunk
        int chunkLength = din.readInt();
        log.info("received chunkLength: {}", chunkLength);
        chunk = new byte[chunkLength];
        din.readFully(chunk, 0, chunkLength);

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

            // write sequenceNumber
            dout.writeInt(sequenceNumber);

            // write version
            dout.writeInt(version);

            // write fileName
            dout.writeByte(fileName.getBytes().length);
            dout.write(fileName.getBytes());

            // write testStr
            String test = "testing additional parameters";
            dout.writeByte(test.getBytes().length);
            dout.write(test.getBytes());

            // write chunk
            log.info("sending chunkLength: {}", chunk.length);
            dout.writeInt(chunk.length);
            dout.write(chunk);

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
