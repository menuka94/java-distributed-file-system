package cs555.hw1.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class FileUtil {
    private static final Logger log = LogManager.getLogger(FileUtil.class);

    public static byte[] readFileAsBytes(String filePath) throws IOException {
        log.info("readFileAsBytes()");
        return Files.readAllBytes(Paths.get(filePath));
    }

    public static List<byte[]> splitFile(byte[] bytes) {
        List<byte[]>chunks = new ArrayList<>();
        for (int i = 0; i < bytes.length; i++) {
            byte[] chunk = new byte[Math.min(Constants.CHUNK_SIZE, bytes.length - i)];
            for (int j = 0; j < chunk.length; j++, i++) {
                chunk[j] = bytes[i];
            }
            chunks.add(chunk);
        }

        return chunks;
    }
}
