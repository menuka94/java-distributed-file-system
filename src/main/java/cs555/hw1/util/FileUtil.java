package cs555.hw1.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;

public class FileUtil {
    private static final Logger log = LogManager.getLogger(FileUtil.class);

    public static byte[] readFileAsBytes(String filePath) throws IOException {
        log.info("readFileAsBytes()");
        return Files.readAllBytes(Paths.get(filePath));
    }

    public static byte[] concat(byte[]... arrays) {
        int length = 0;
        for (byte[] array: arrays) {
            length += array.length;
        }
        byte[] result = new byte[length];
        int pos = 0;
        for (byte[] array : arrays) {
            System.arraycopy(array, 0, result, pos, array.length);
            pos += array.length;
        }

        return result;
    }

    public static String getFileNameFromChunkName(String chunkName) {
        int i = chunkName.indexOf(Constants.ChunkServer.EXT_DATA_CHUNK);
        return chunkName.substring(0, i - 1);
    }

    public static List<byte[]> splitFile(byte[] bytes, int size) {
        List<byte[]> chunks = new ArrayList<>();
        for (int i = 0; i < bytes.length; i++) {
            byte[] chunk = new byte[Math.min(size, bytes.length - i)];
            for (int j = 0; j < chunk.length; j++, i++) {
                chunk[j] = bytes[i];
            }
            chunks.add(chunk);
        }

        return chunks;
    }

    public static String hash(byte[] data) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-1");
            return byteArray2Hex(md.digest(data));
        } catch (NoSuchAlgorithmException e) {
            log.error(e.getLocalizedMessage());
            e.printStackTrace();
            return null;
        }
    }

    private static String byteArray2Hex(final byte[] hash) {
        Formatter formatter = new Formatter();
        for (byte b : hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

}
