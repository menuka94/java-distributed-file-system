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
import java.util.ArrayList;

/**
 * MajorHeartbeat includes information about all the chunks maintained at the chunk server.
 * Also includes: total number of chunks, free-space available.
 */
public class SendMajorHeartbeat extends Event {
    private static final Logger log = LogManager.getLogger(SendMajorHeartbeat.class);

    private int noOfChunks;
    private ArrayList<String> chunks;
    private long freeSpace;

    public SendMajorHeartbeat() {

    }

    public SendMajorHeartbeat(byte[] marshalledBytes) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

        byte messageType = din.readByte();
        EventValidator.validateEventType(messageType, getType(), log);

        // read number of chunks
        noOfChunks = din.readInt();

        chunks = new ArrayList<>();

        // read chunk information
        for (int i = 0; i < noOfChunks; i++) {
            int chunkInfoLength = din.readInt();
            byte[] chunkInfo = new byte[chunkInfoLength];
            din.readFully(chunkInfo, 0, chunkInfoLength);
            chunks.add(new String(chunkInfo));
        }

        // read free space
        freeSpace = din.readLong();

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

            // write number of chunks
            dout.writeInt(noOfChunks);

            // write chunk information
            for (String chunk : chunks) {
                dout.writeInt(chunk.getBytes().length);
                dout.write(chunk.getBytes());
            }

            // write free space
            dout.writeLong(freeSpace);

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
        return Protocol.SEND_MAJOR_HEARTBEAT;
    }

    public ArrayList<String> getChunks() {
        return chunks;
    }

    public void setChunks(ArrayList<String> chunks) {
        this.chunks = chunks;
    }

    public int getNoOfChunks() {
        return noOfChunks;
    }

    public void setNoOfChunks(int noOfChunks) {
        this.noOfChunks = noOfChunks;
    }

    public long getFreeSpace() {
        return freeSpace;
    }

    public void setFreeSpace(long freeSpace) {
        this.freeSpace = freeSpace;
    }
}
