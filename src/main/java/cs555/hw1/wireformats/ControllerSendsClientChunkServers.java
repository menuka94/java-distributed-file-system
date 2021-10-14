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

public class ControllerSendsClientChunkServers extends Event {
    private static final Logger log = LogManager.getLogger(ControllerSendsClientChunkServers.class);

    private String[] chunkServerHosts;
    private String[] chunkServerHostNames;
    private int[] chunkServerPorts;

    public ControllerSendsClientChunkServers() {

    }

    public ControllerSendsClientChunkServers(byte[] marshalledBytes) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

        byte messageType = din.readByte();
        EventValidator.validateEventType(messageType, getType(), log);

        chunkServerHosts = new String[3];
        chunkServerHostNames = new String[3];
        chunkServerPorts = new int[3];

        for (int i = 0; i < 3; i++) {
            // read host IP
            int hostLength = din.readByte();
            byte[] host = new byte[hostLength];
            din.readFully(host, 0, hostLength);
            chunkServerHosts[i] = new String(host);

            // read host name
            int hostNameLength = din.readByte();
            byte[] hostName = new byte[hostNameLength];
            din.readFully(hostName, 0, hostNameLength);
            chunkServerHostNames[i] = new String(hostName);

            // read port
            int port = din.readInt();
            chunkServerPorts[i] = port;
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

            // write chunk server hosts and ports
            for (int i = 0; i < 3; i++) {
                // write host IP address
                int hostLength = (byte) chunkServerHosts[i].getBytes().length;
                dout.writeByte(hostLength);
                dout.write(chunkServerHosts[i].getBytes());

                // write host name
                int hostNameLength = chunkServerHostNames[i].getBytes().length;
                dout.writeByte(hostNameLength);
                dout.write(chunkServerHostNames[i].getBytes());

                // write port
                dout.writeInt(chunkServerPorts[i]);
            }

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


    public String[] getChunkServerHosts() {
        return chunkServerHosts;
    }

    public void setChunkServerHosts(String[] chunkServerHosts) {
        this.chunkServerHosts = chunkServerHosts;
    }

    public int[] getChunkServerPorts() {
        return chunkServerPorts;
    }

    public void setChunkServerPorts(int[] chunkServerPorts) {
        this.chunkServerPorts = chunkServerPorts;
    }

    @Override
    public int getType() {
        return Protocol.CONTROLLER_SENDS_CLIENT_CHUNK_SERVERS;
    }

    public String[] getChunkServerHostNames() {
        return chunkServerHostNames;
    }

    public void setChunkServerHostNames(String[] chunkServerHostNames) {
        this.chunkServerHostNames = chunkServerHostNames;
    }
}
