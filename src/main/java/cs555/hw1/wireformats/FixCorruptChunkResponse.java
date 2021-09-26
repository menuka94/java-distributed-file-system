package cs555.hw1.wireformats;

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

public class FixCorruptChunkResponse extends Event {
    private static final Logger log = LogManager.getLogger(FixCorruptChunkResponse.class);

    private Socket socket;
    private String chunkName;
    private byte[] chunk;
    private String chunkHash;

    public FixCorruptChunkResponse() {

    }

    public FixCorruptChunkResponse(byte[] marshalledBytes) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

        byte messageType = din.readByte();
        EventValidator.validateEventType(messageType, getType(), log);

        // read chunkName
        int chunkNameLength = din.readInt();
        byte[] chunkNameBytes = new byte[chunkNameLength];
        din.readFully(chunkNameBytes, 0, chunkNameLength);
        chunkName = new String(chunkNameBytes);

        // read chunk (data)
        int chunkLength = din.readInt();
        chunk = new byte[chunkLength];
        din.readFully(chunk, 0, chunkLength);

        // read chunkHash
        int chunkHashLength = din.readInt();
        byte[] chunkHashBytes = new byte[chunkHashLength];
        din.readFully(chunkHashBytes, 0, chunkHashLength);
        chunkHash = new String(chunkHashBytes);

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

            // write chunk name
            dout.writeInt(chunkName.getBytes().length);
            dout.write(chunkName.getBytes());

            // write chunk (data)
            dout.writeInt(chunk.length);
            dout.write(chunk);

            // write chunkHash
            dout.writeInt(chunkHash.getBytes().length);
            dout.write(chunkHash.getBytes());

            dout.flush();
            marshalledBytes = baOutputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return marshalledBytes;
    }

    @Override
    public int getType() {
        return Protocol.FIX_CORRUPT_CHUNK_RESPONSE;
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

    public String getChunkName() {
        return chunkName;
    }

    public void setChunkName(String chunkName) {
        this.chunkName = chunkName;
    }

    public String getChunkHash() {
        return chunkHash;
    }

    public void setChunkHash(String chunkHash) {
        this.chunkHash = chunkHash;
    }
}
