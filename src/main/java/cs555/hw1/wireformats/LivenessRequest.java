package cs555.hw1.wireformats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.Socket;

public class LivenessRequest extends Event {
    private static final Logger log = LogManager.getLogger(LivenessRequest.class);

    private Socket socket;

    public LivenessRequest() {

    }

    public LivenessRequest(byte[] marshalledBytes) {

    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }

    @Override
    public int getType() {
        return Protocol.LIVENESS_REQUEST;
    }

    @Override
    public Socket getSocket() {
        return socket;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }
}
