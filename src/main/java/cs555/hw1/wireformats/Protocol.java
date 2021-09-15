package cs555.hw1.wireformats;

public interface Protocol {
    int CLIENT_REQUESTS_CHUNK_SERVERS_FROM_CONTROLLER = 2;
    int CONTROLLER_SENDS_CLIENT_CHUNK_SERVERS = 3;

    int REGISTER_CLIENT = 4;
    int REPORT_CLIENT_REGISTRATION = 5;

    int REGISTER_CHUNK_SERVER = 6;
    int REPORT_CHUNK_SERVER_REGISTRATION = 7;

    int WRITE_INITIAL_CHUNK = 8;
    int REPLICATE_CHUNK = 9;
}
