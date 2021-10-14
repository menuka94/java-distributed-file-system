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

public class ReportChunkServerRegistration extends Event {
    private static final Logger log = LogManager.getLogger(ReportChunkServerRegistration.class);

    private int messageType;
    private int successStatus;
    private byte lengthOfString;
    private String infoString;

    public ReportChunkServerRegistration() {

    }

    public ReportChunkServerRegistration(byte[] marshalledBytes) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(marshalledBytes);
        DataInputStream din = new DataInputStream(new BufferedInputStream(baInputStream));

        messageType = din.readByte();
        EventValidator.validateEventType((byte) messageType, getType(), log);

        successStatus = din.readInt();
        lengthOfString = din.readByte();
        byte[] byteInfoString = new byte[lengthOfString];
        din.readFully(byteInfoString, 0, lengthOfString);

        infoString = new String(byteInfoString);

        baInputStream.close();
        din.close();
    }

    public int getMessageType() {
        return messageType;
    }

    public void setMessageType(int messageType) {
        this.messageType = messageType;
    }

    public int getSuccessStatus() {
        return successStatus;
    }

    public void setSuccessStatus(int successStatus) {
        this.successStatus = successStatus;
    }

    public byte getLengthOfString() {
        return lengthOfString;
    }

    public void setLengthOfString(byte lengthOfString) {
        this.lengthOfString = lengthOfString;
    }

    public String getInfoString() {
        return infoString;
    }

    public void setInfoString(String infoString) {
        this.infoString = infoString;
    }

    @Override
    public byte[] getBytes() {
        byte[] marshalledBytes = null;
        ByteArrayOutputStream baOutputStram = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutputStram));

        try {
            dout.writeByte(getType());
            dout.writeInt(successStatus);
            dout.writeByte(lengthOfString);
            dout.write(infoString.getBytes());
            dout.flush();

            marshalledBytes = baOutputStram.toByteArray();
        } catch (IOException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                baOutputStram.close();
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
        return Protocol.REPORT_CHUNK_SERVER_REGISTRATION;
    }
}
