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

public class FixCorruptChunk extends Event {
    private static final Logger log = LogManager.getLogger(FixCorruptChunk.class);

    private String chunkName;
    private String chunkServerHost;
    private String chunkServerHostname;
    private int chunkServerPort;

    public FixCorruptChunk() {

    }

    public FixCorruptChunk(byte[] marshalledBytes) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

        byte messageType = din.readByte();
        EventValidator.validateEventType(messageType, getType(), log);

        // read chunk name
        int chunkNameLength = din.readInt();
        byte[] chunkNameBytes = new byte[chunkNameLength];
        din.readFully(chunkNameBytes, 0, chunkNameLength);
        chunkName = new String(chunkNameBytes);


        // read chunkServerHost
        int hostLength = din.readInt();
        byte[] hostBytes = new byte[hostLength];
        din.readFully(hostBytes, 0, hostLength);
        chunkServerHost = new String(hostBytes);

        // read chunkServerHostName
        int hostNameLength = din.readInt();
        byte[] hostNameBytes = new byte[hostNameLength];
        din.readFully(hostNameBytes, 0, hostNameLength);
        chunkServerHostname = new String(hostNameBytes);

        // read chunkServerPort
        chunkServerPort = din.readInt();

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

            // write chunkName
            dout.writeInt(chunkName.getBytes().length);
            dout.write(chunkName.getBytes());

            // write chunkServerHost
            dout.writeInt(chunkServerHost.getBytes().length);
            dout.write(chunkServerHost.getBytes());

            // write chunkServerHostname
            dout.writeInt(chunkServerHostname.getBytes().length);
            dout.write(chunkServerHostname.getBytes());

            // write port
            dout.writeInt(chunkServerPort);

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
        return Protocol.FIX_CORRUPT_CHUNK;
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

    public int getChunkServerPort() {
        return chunkServerPort;
    }

    public void setChunkServerPort(int chunkServerPort) {
        this.chunkServerPort = chunkServerPort;
    }
}
