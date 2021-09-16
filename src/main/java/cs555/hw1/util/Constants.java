package cs555.hw1.util;

public class Constants {
    public static final String CHUNK_DIR = "/tmp/menukaw";
    public static final int CHUNK_SIZE = 64 * 1000; // chunk size in bytes
    public static final int SLICE_SIZE = 8 * 1000; // slice size in bytes
    public static final int REPLICATION_LEVEL = 3;

    public static class Controller {
        public static final String HOST = "arkansas";
        public static final String PORT = "9000";
        public static final String CMD_GET_HOST = "get-host";
        public static final String CMD_LIST_CHUNK_SERVERS = "list-chunk-servers";
    }

    public static class Client {
        public static final String CMD_ADD_FILE = "add-file";
        public static final int PORT = 9010;
        public static final String CMD_GET_HOST = "get-host";
        public static final String CMD_RETRIEVE = "retrieve";
    }

    public static class ChunkServer {
        public static final String CMD_LIST_FILES = "list-files";
        public static final String CMD_GET_HOST = "get-host";
        public static final int MAX_NODES = 20;
        public static final String EXT_DATA_CHUNK = "_chunk";
    }
}
