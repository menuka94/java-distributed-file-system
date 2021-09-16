package cs555.hw1.wireformats;

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
            case Protocol.WRITE_INITIAL_CHUNK:
                return "WRITE_INITIAL_CHUNK";
            case Protocol.REPLICATE_CHUNK_REQUEST:
                return "REPLICATE_CHUNK_REQUEST";
            case Protocol.FORWARD_CHUNK:
                return "FORWARD_CHUNK";
            default:
                return "ERROR: Unknown Event";
        }
    }
}
