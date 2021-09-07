package cs555.hw1;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

public class File {
    private static final Logger log = LogManager.getLogger(File.class);

    private String name;
    private ArrayList<Chunk> chunks;
}
