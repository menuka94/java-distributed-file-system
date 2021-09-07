package cs555.hw1;

public class Constants {
    public static final String CHUNK_DIR = "/tmp";

    public static class Controller {
        public static final String HOST = "denver";
        public static final String PORT = "9000";
    }

    public static class Client {
        public static final String CMD_LIST_CHUNK_SERVERs = "list-chunk-servers";
        public static final String CMD_ADD_FILE = "add-file";
        public static final int PORT = 9000;
    }
}
