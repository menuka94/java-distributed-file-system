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

public class RegisterChunkServer extends Event {
    private static final Logger log = LogManager.getLogger(RegisterChunkServer.class);

    private byte ipAddressLength;
    private byte[] ipAddress;
    private int port;

    public RegisterChunkServer() {

    }

    public RegisterChunkServer(byte[] marshalledBytes) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

        byte messageType = din.readByte();

        EventValidator.validateEventType(messageType, getType(), log);

        ipAddressLength = din.readByte();
        ipAddress = new byte[ipAddressLength];
        din.readFully(ipAddress, 0, ipAddressLength);
        port = din.readInt();

        baInputStream.close();
        din.close();
    }

    public byte getIpAddressLength() {
        return ipAddressLength;
    }

    public void setIpAddressLength(byte ipAddressLength) {
        this.ipAddressLength = ipAddressLength;
    }

    public byte[] getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(byte[] ipAddress) {
        this.ipAddress = ipAddress;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public byte[] getBytes() {
        byte[] marshalledBytes = null;
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));

        try {
            dout.writeByte(getType());
            dout.writeByte(ipAddressLength);
            dout.write(ipAddress);
            dout.writeInt(port);
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

    @Override
    public int getType() {
        return Protocol.REGISTER_CHUNK_SERVER;
    }
}
