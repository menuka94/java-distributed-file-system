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
import java.util.ArrayList;

/**
 * MinorHeartbeat includes information about any newly added chunks.
 * Also includes: total number of chunks and free-space available.
 */
public class SendMinorHeartbeat extends Event {
    private static final Logger log = LogManager.getLogger(SendMinorHeartbeat.class);

    private Socket socket;
    private int noOfChunks;
    private ArrayList<String> newChunks;
    private long freeSpace;
    private int totNewChunks;

    public SendMinorHeartbeat() {

    }


    public SendMinorHeartbeat(byte[] marshalledBytes) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

        byte messageType = din.readByte();
        EventValidator.validateEventType(messageType, getType(), log);

        // read number of chunks
        noOfChunks = din.readInt();


        // read free space
        freeSpace = din.readLong();

        //read total new chunks
        totNewChunks = din.readInt();

        newChunks = new ArrayList<>();

        // read updated chunk information
        for (int i = 0; i < noOfChunks; i++) {
            int chunkInfoLength = din.readInt();
            byte[] chunkInfo = new byte[chunkInfoLength];
            din.readFully(chunkInfo, 0, chunkInfoLength);
            newChunks.add(new String(chunkInfo));
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

            // write number of chunks
            dout.writeInt(noOfChunks);

            // write free space
            dout.writeLong(freeSpace);
            //write no of new Chunks
            dout.writeInt(totNewChunks);

            //    write updated chunks information
            //if(newChunks.size()>0)
            for (String chunk : newChunks) {
                dout.writeInt(chunk.getBytes().length);
                dout.write(chunk.getBytes());
            }


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


    public ArrayList<String> getNewChunks() {
        return newChunks;
    }

    //    public void setChunks(ArrayList<String> chunks) {
    //        this.chunks = chunks;
    //    }
    public void setNewChunks(ArrayList<String> newChunks) {

        this.newChunks = newChunks;
    }

    public int getNoOfChunks() {
        return noOfChunks;
    }

    public int getNoOfNewChunks() {
        return totNewChunks;
    }

    public void setNoOfChunks(int noOfChunks) {
        this.noOfChunks = noOfChunks;
    }

    public void setNoOfNewChunks(int noOfNewChunks) {
        this.totNewChunks = noOfNewChunks;
    }

    public long getFreeSpace() {
        return freeSpace;
    }


    public void setFreeSpace(long freeSpace) {
        this.freeSpace = freeSpace;
    }


    @Override
    public int getType() {
        return Protocol.SEND_MINOR_HEARTBEAT;
    }

    @Override
    public Socket getSocket() {
        return socket;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }
}
