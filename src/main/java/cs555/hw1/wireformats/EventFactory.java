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
        log.info("getEvent(): {}", ProtocolLookup.getEventLiteral(b));
        switch ((int) b) {
            case Protocol.CLIENT_REQUESTS_CHUNK_SERVERS_FROM_CONTROLLER:
                ClientRequestsChunkServersFromController requestChunkServersEvent =
                        new ClientRequestsChunkServersFromController(data);
                requestChunkServersEvent.setSocket(socket);
                return requestChunkServersEvent;
            case Protocol.CONTROLLER_SENDS_CLIENT_CHUNK_SERVERS:
                ControllerSendsClientChunkServers sendsClientChunkServersEvent =
                        new ControllerSendsClientChunkServers(data);
                sendsClientChunkServersEvent.setSocket(socket);
                return sendsClientChunkServersEvent;
            case Protocol.REGISTER_CLIENT:
                RegisterClient registerClient = new RegisterClient(data);
                registerClient.setSocket(socket);
                return registerClient;
            case Protocol.REPORT_CLIENT_REGISTRATION:
                ReportClientRegistration clientRegistration = new ReportClientRegistration(data);
                clientRegistration.setSocket(socket);
                return clientRegistration;
            case Protocol.REGISTER_CHUNK_SERVER:
                RegisterChunkServer registerChunkServer = new RegisterChunkServer(data);
                registerChunkServer.setSocket(socket);
                return registerChunkServer;
            case Protocol.REPORT_CHUNK_SERVER_REGISTRATION:
                ReportChunkServerRegistration chunkServerRegistration = new ReportChunkServerRegistration(data);
                chunkServerRegistration.setSocket(socket);
                return chunkServerRegistration;
            case Protocol.WRITE_INITIAL_CHUNK:
                WriteInitialChunk writeInitialChunk = new WriteInitialChunk(data);
                writeInitialChunk.setSocket(socket);
                return writeInitialChunk;
            case Protocol.REPLICATE_CHUNK_REQUEST:
                ReplicateChunkRequest replicateChunkRequest = new ReplicateChunkRequest(data);
                replicateChunkRequest.setSocket(socket);
                return replicateChunkRequest;
            case Protocol.FORWARD_CHUNK:
                ForwardChunk forwardChunk = new ForwardChunk(data);
                forwardChunk.setSocket(socket);
                return forwardChunk;
            case Protocol.STORE_CHUNK:
                StoreChunk storeChunk = new StoreChunk(data);
                storeChunk.setSocket(socket);
                return storeChunk;
            default:
                log.error("Unknown event type: {}", (int) b);
                return null;
        }
    }
}
