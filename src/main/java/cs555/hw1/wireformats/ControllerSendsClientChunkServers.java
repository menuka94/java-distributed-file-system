package cs555.hw1.wireformats;

import cs555.hw1.EventValidator;
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

public class ControllerSendsClientChunkServers extends Event {
    private static final Logger log = LogManager.getLogger(ControllerSendsClientChunkServers.class);

    private Socket socket;

    private String[] chunkServerHosts;
    private int[] chunkServerPorts;

    public ControllerSendsClientChunkServers() {

    }

    public ControllerSendsClientChunkServers(byte[] marshalledBytes) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

        byte messageType = din.readByte();

        EventValidator.validateEventType(messageType, getType(), log);

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

    @Override
    public byte[] getBytes() {
        byte[] marshalledBytes = null;
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));

        try {
            dout.writeByte(getType());
            dout.flush();

            marshalledBytes = baOutputStream.toByteArray();
        } catch (IOException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                baOutputStream.close();
                dout.close();
            } catch (IOException e) {
                log.error(e.getMessage());
                e.printStackTrace();
            }
        }

        return marshalledBytes;
    }

    public void setChunkServers(String[] chunkServerHosts, int[] chunkServerPorts) {
        this.chunkServerHosts = chunkServerHosts;
        this.chunkServerPorts = chunkServerPorts;
    }

    @Override
    public int getType() {
        return Protocol.CONTROLLER_SENDS_CLIENT_CHUNK_SERVERS;
    }
}
