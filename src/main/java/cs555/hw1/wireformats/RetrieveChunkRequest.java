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

public class RetrieveChunkRequest extends Event {
    private static final Logger log = LogManager.getLogger(RetrieveChunkRequest.class);

    private Socket socket;
    private String chunkName;

    public RetrieveChunkRequest() {

    }

    public RetrieveChunkRequest(byte[] marshalledBytes) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

        byte messageType = din.readByte();
        EventValidator.validateEventType(messageType, getType(), log);

        // read chunkName
        int chunkNameLength = din.readInt();
        byte[] chunkNameBytes = new byte[chunkNameLength];
        din.readFully(chunkNameBytes, 0, chunkNameLength);
        chunkName = new String(chunkNameBytes);

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

            dout.flush();;
            marshalledBytes = baOutputStream.toByteArray();
        } catch (IOException e) {
            log.error(e.getLocalizedMessage());
            e.printStackTrace();
        }

        return marshalledBytes;
    }

    @Override
    public int getType() {
        return Protocol.RETRIEVE_CHUNK_REQUEST;
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
}
