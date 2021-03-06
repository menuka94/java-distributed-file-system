package org.dfs.wireformats;

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
        log.debug("getEvent(): {}", ProtocolLookup.getEventLiteral(b));
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
            case Protocol.STORE_CHUNK:
                StoreChunk storeChunk = new StoreChunk(data);
                storeChunk.setSocket(socket);
                return storeChunk;
            case Protocol.SEND_MAJOR_HEARTBEAT:
                SendMajorHeartbeat majorHeartbeat = new SendMajorHeartbeat(data);
                majorHeartbeat.setSocket(socket);
                return majorHeartbeat;
            case Protocol.SEND_MINOR_HEARTBEAT:
                SendMinorHeartbeat minorHeartbeat = new SendMinorHeartbeat(data);
                minorHeartbeat.setSocket(socket);
                return minorHeartbeat;
            case Protocol.RETRIEVE_FILE_REQUEST:
                RetrieveFileRequest retrieveFileRequest = new RetrieveFileRequest(data);
                retrieveFileRequest.setSocket(socket);
                return retrieveFileRequest;
            case Protocol.RETRIEVE_FILE_RESPONSE:
                RetrieveFileResponse retrieveFileResponse = new RetrieveFileResponse(data);
                retrieveFileResponse.setSocket(socket);
                return retrieveFileResponse;
            case Protocol.SEND_FILE_INFO:
                SendFileInfo sendFileInfo = new SendFileInfo(data);
                sendFileInfo.setSocket(socket);
                return sendFileInfo;
            case Protocol.RETRIEVE_CHUNK_REQUEST:
                RetrieveChunkRequest retrieveChunkRequest = new RetrieveChunkRequest(data);
                retrieveChunkRequest.setSocket(socket);
                return retrieveChunkRequest;
            case Protocol.RETRIEVE_CHUNK_RESPONSE:
                RetrieveChunkResponse retrieveChunkResponse = new RetrieveChunkResponse(data);
                retrieveChunkResponse.setSocket(socket);
                return retrieveChunkResponse;
            case Protocol.REPORT_CHUNK_CORRUPTION:
                ReportChunkCorruption reportChunkCorruption = new ReportChunkCorruption(data);
                reportChunkCorruption.setSocket(socket);
                return reportChunkCorruption;
            case Protocol.FIX_CORRUPT_CHUNK:
                FixCorruptChunk fixCorruptChunk = new FixCorruptChunk(data);
                fixCorruptChunk.setSocket(socket);
                return fixCorruptChunk;
            case Protocol.FIX_CORRUPT_CHUNK_RESPONSE:
                FixCorruptChunkResponse fixCorruptChunkResponse = new FixCorruptChunkResponse(data);
                fixCorruptChunkResponse.setSocket(socket);
                return fixCorruptChunkResponse;
            case Protocol.LIVENESS_HEARTBEAT:
                LivenessHeartbeat livenessHeartbeat = new LivenessHeartbeat(data);
                livenessHeartbeat.setSocket(socket);
                return livenessHeartbeat;
            //FixCorruptChunk fixCorruptChunk = new FixCorruptChunk(data);
            //fixCorruptChunk.setSocket(socket);
            //return fixCorruptChunk;
            default:
                log.error("Unknown event type: {}", (int) b);
                return null;
        }
    }
}
