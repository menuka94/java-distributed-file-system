package cs555.hw1.wireformats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

public class EventFactory {
    private static final Logger log = LogManager.getLogger(EventFactory.class);
    private static EventFactory instance;

    private EventFactory() {

    }

    public synchronized static EventFactory getInstance() {
        if (instance == null) {
            instance = new EventFactory();
        }
        return instance;
    }

    public Event getEvent(byte[] data, Socket socket) throws IOException {
        byte b = ByteBuffer.wrap(data).get(0);
        switch ((int) b) {
            case Protocol.CLIENT_REQUESTS_CHUNK_SERVERS_FROM_CONTROLLER:
                log.info("CLIENT_REQUESTS_CHUNK_SERVERS_FROM_CONTROLLER");
                ClientRequestsChunkServersFromController requestChunkServersEvent =
                        new ClientRequestsChunkServersFromController(data);
                requestChunkServersEvent.setSocket(socket);
                return requestChunkServersEvent;
            case Protocol.CONTROLLER_SENDS_CLIENT_CHUNK_SERVERS:
                log.info("CONTROLLER_SENDS_CLIENT_CHUNK_SERVERS");
                ControllerSendsClientChunkServers sendsClientChunkServersEvent =
                        new ControllerSendsClientChunkServers();
                sendsClientChunkServersEvent.setSocket(socket);
                return sendsClientChunkServersEvent;
            case Protocol.REGISTER_CLIENT:
                log.info("REGISTER_CLIENT");
                RegisterClient registerClient = new RegisterClient(data);
                registerClient.setSocket(socket);
                return registerClient;
            case Protocol.REPORT_CLIENT_REGISTRATION:
                log.info("REPORT_CLIENT_REGISTRATION");
                ReportClientRegistration clientRegistration = new ReportClientRegistration(data);
                clientRegistration.setSocket(socket);
                return clientRegistration;
            default:
                log.error("Unknown event type: {}", (int) b);
                return null;
        }
    }
}
