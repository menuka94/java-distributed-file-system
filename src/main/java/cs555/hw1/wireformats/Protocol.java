package cs555.hw1.wireformats;

public interface Protocol {
    int CLIENT_REQUESTS_CHUNK_SERVERS_FROM_CONTROLLER = 2;
    int CONTROLLER_SENDS_CLIENT_CHUNK_SERVERS = 3;
}
