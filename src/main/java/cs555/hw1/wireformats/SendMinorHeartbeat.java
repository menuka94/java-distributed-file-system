package cs555.hw1.wireformats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.Socket;

/**
 * MinorHeartbeat includes information about any newly added chunks.
 * Also includes: total number of chunks and free-space available.
 */
public class SendMinorHeartbeat extends Event {
    private static final Logger log = LogManager.getLogger(SendMinorHeartbeat.class);

    private Socket socket;

    public SendMinorHeartbeat() {

    }

    public SendMinorHeartbeat(byte[] marshalledBytes) {

    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
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
