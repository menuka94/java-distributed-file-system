package cs555.hw1;

public class Constants {
    public static final String CHUNK_DIR = "/tmp";

    public static class Controller {
        public static final String HOST = "arkansas";
        public static final String PORT = "9000";
        public static final String CMD_GET_HOST = "get-host";
        public static final String CMD_LIST_CHUNK_SERVERS = "list-chunk-servers";
    }

    public static class ChunkInfo {
        public static final String EXT_DATA_CHUNK = "_chunk";
    }

    public static class Client {
        public static final String CMD_ADD_FILE = "add-file";
        public static final int PORT = 9010;
        public static final String CMD_GET_HOST = "get-host";
    }

    public static class ChunkServer {
        public static final String CMD_LIST_FILES = "list-files";
        public static final String CMD_GET_HOST = "get-host";
        public static final int MAX_NODES = 20;
    }
}
