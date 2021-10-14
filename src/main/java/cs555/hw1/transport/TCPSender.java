package cs555.hw1.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class TCPSender {
    private static final Logger log = LogManager.getLogger(TCPSender.class);

    private final Socket socket;
    private final DataOutputStream dout;

    public TCPSender(Socket socket) throws IOException {
        this.socket = socket;
        dout = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    }

    public void sendData(byte[] dataToSend) throws IOException {
        synchronized (socket) {
            int dataLength = dataToSend.length;
            dout.writeInt(dataLength);
            dout.write(dataToSend, 0, dataLength);
            dout.flush();
        }
    }
}
