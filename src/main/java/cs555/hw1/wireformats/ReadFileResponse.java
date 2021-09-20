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

public class ReadFileResponse extends Event {
    private static final Logger log = LogManager.getLogger(ReadFileResponse.class);

    private Socket socket;
    private String fileName;
    private int noOfChunks;
    private int fileSize;
    private String[] chunkServerHosts;
    private String[] chunkServerHostNames;
    private int[] chunkServerPorts;

    public ReadFileResponse() {

    }

    public ReadFileResponse(byte[] marshalledBytes) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

        byte messageType = din.readByte();
        EventValidator.validateEventType(messageType, getType(), log);

        // read file name
        int fileNameLength = din.readInt();
        byte[] fileNameBytes = new byte[fileNameLength];
        din.readFully(fileNameBytes, 0, fileNameLength);
        fileName = new String(fileNameBytes);

        // read no. of chunks
        noOfChunks = din.readInt();

        // read file size
        fileSize = din.readInt();

        // read chunkServerHosts
        chunkServerHosts = new String[noOfChunks];
        for (int i = 0; i < noOfChunks; i++) {
            int hostLength = din.readInt();
            byte[] hostBytes = new byte[hostLength];
            din.readFully(hostBytes, 0, hostLength);
            chunkServerHosts[i] = new String(hostBytes);
        }

        // read chunkServerHostNames
        chunkServerHostNames = new String[noOfChunks];
        for (int i = 0; i < noOfChunks; i++) {
            int hostNameLength = din.readInt();
            byte[] hostNameBytes = new byte[hostNameLength];
            din.readFully(hostNameBytes, 0, hostNameLength);
            chunkServerHostNames[i] = new String(hostNameBytes);
        }

        // read chunkServerPorts
        chunkServerPorts = new int[noOfChunks];
        for (int i = 0; i < noOfChunks; i++) {
            chunkServerPorts[i] = din.readInt();
        }
    }

    @Override
    public byte[] getBytes() {
        byte[] marshalledBytes = null;
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStream));

        try {
            dout.writeByte(getType());

            // write file name
            dout.writeInt(fileName.getBytes().length);
            dout.write(fileName.getBytes());

            // write no. of chunks
            dout.writeInt(noOfChunks);

            // write file size
            dout.writeInt(fileSize);

            // write chunkServerHosts
            for (String host : chunkServerHosts) {
                dout.writeInt(host.getBytes().length);
                dout.write(host.getBytes());
            }

            for (String hostName : chunkServerHostNames) {
                dout.writeInt(hostName.getBytes().length);
                dout.write(hostName.getBytes());
            }

            // write chunkServerPorts
            for (int port : chunkServerPorts) {
                dout.writeInt(port);
            }

            dout.flush();
            marshalledBytes = baOutputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return marshalledBytes;
    }

    @Override
    public int getType() {
        return Protocol.READ_FILE_RESPONSE;
    }

    @Override
    public Socket getSocket() {
        return socket;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getNoOfChunks() {
        return noOfChunks;
    }

    public void setNoOfChunks(int noOfChunks) {
        this.noOfChunks = noOfChunks;
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

    public int getFileSize() {
        return fileSize;
    }

    public void setFileSize(int fileSize) {
        this.fileSize = fileSize;
    }

    public String[] getChunkServerHostNames() {
        return chunkServerHostNames;
    }

    public void setChunkServerHostNames(String[] chunkServerHostNames) {
        this.chunkServerHostNames = chunkServerHostNames;
    }
}
