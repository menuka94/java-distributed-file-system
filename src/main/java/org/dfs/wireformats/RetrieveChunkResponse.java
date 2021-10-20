package org.dfs.wireformats;

import org.dfs.util.EventValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class RetrieveChunkResponse extends Event {
    private static final Logger log = LogManager.getLogger(RetrieveFileResponse.class);

    private String chunkName;
    private byte[] chunk;
    private String chunkHash;

    public RetrieveChunkResponse() {

    }

    public RetrieveChunkResponse(byte[] marshalledBytes) throws IOException {
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
            log.error(e.getLocalizedMessage());
            e.printStackTrace();
        } finally {
            try {
                baOutputStream.close();
                dout.close();
            } catch (IOException e) {
                log.error(e.getLocalizedMessage());
                e.printStackTrace();
            }
        }

        return marshalledBytes;
    }

    @Override
    public int getType() {
        return Protocol.RETRIEVE_CHUNK_RESPONSE;
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
