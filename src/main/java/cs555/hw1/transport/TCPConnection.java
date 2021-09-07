package cs555.hw1.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.Socket;

public class TCPConnection {
    public static final Logger log = LogManager.getLogger(TCPConnection.class);
    private Socket socket;
    private TCPSender tcpSender;
}
