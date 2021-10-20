package org.dfs.wireformats;

public class ProtocolLookup {
    public static String getEventLiteral(int type) {
        switch (type) {
            case Protocol.CLIENT_REQUESTS_CHUNK_SERVERS_FROM_CONTROLLER:
                return "CLIENT_REQUESTS_CHUNK_SERVERS_FROM_CONTROLLER";
            case Protocol.CONTROLLER_SENDS_CLIENT_CHUNK_SERVERS:
                return "CONTROLLER_SENDS_CLIENT_CHUNK_SERVERS";
            case Protocol.REGISTER_CLIENT:
                return "REGISTER_CLIENT";
            case Protocol.REPORT_CLIENT_REGISTRATION:
                return "REPORT_CLIENT_REGISTRATION";
            case Protocol.REGISTER_CHUNK_SERVER:
                return "REGISTER_CHUNK_SERVER";
            case Protocol.REPORT_CHUNK_SERVER_REGISTRATION:
                return "REPORT_CHUNK_SERVER_REGISTRATION";
            case Protocol.STORE_CHUNK:
                return "STORE_CHUNK";
            case Protocol.SEND_MAJOR_HEARTBEAT:
                return "SEND_MAJOR_HEARTBEAT";
            case Protocol.SEND_MINOR_HEARTBEAT:
                return "SEND_MINOR_HEARTBEAT";
            case Protocol.RETRIEVE_FILE_REQUEST:
                return "RETRIEVE_FILE_REQUEST";
            case Protocol.RETRIEVE_FILE_RESPONSE:
                return "RETRIEVE_FILE_RESPONSE";
            case Protocol.SEND_FILE_INFO:
                return "SEND_FILE_INFO";
            case Protocol.RETRIEVE_CHUNK_REQUEST:
                return "RETRIEVE_CHUNK_REQUEST";
            case Protocol.RETRIEVE_CHUNK_RESPONSE:
                return "RETRIEVE_CHUNK_RESPONSE";
            case Protocol.REPORT_CHUNK_CORRUPTION:
                return "REPORT_CHUNK_CORRUPTION";
            case Protocol.FIX_CORRUPT_CHUNK:
                return "FIX_CORRUPT_CHUNK";
            case Protocol.LIVENESS_HEARTBEAT:
                return "LIVENESS_HEARTBEAT";
            default:
                return "ERROR: Unknown Event: " + type;
        }
    }
}
