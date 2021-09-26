package cs555.hw1.wireformats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.Socket;

public class LivenessResponse extends Event {
    private static final Logger log = LogManager.getLogger(LivenessResponse.class);

    private Socket socket;

    public LivenessResponse() {

    }

    public LivenessResponse(byte[] marshalledBytes) {

    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }

    @Override
    public int getType() {
        return Protocol.LIVENESS_RESPONSE;
    }

    @Override
    public Socket getSocket() {
        return socket;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }
}
