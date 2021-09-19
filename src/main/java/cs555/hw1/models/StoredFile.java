package cs555.hw1.models;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

public class StoredFile {
    private static final Logger log = LogManager.getLogger(StoredFile.class);

    private String name;
    private ArrayList<Chunk> chunks;

    public StoredFile(String name) {
        this.name = name;
        chunks = new ArrayList<>();
    }

    public ArrayList<Chunk> getChunks() {
        return chunks;
    }

    public void addChunk(Chunk chunk) {
        chunks.add(chunk);
    }
}