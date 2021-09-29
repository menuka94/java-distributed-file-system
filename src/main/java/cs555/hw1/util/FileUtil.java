package cs555.hw1.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.List;

public class FileUtil {
    private static final Logger log = LogManager.getLogger(FileUtil.class);

    public static void main(String[] args) {
        System.out.println(getFileNameFromChunkName("Hall_of_Fame.pdf_chunk1"));
        System.out.println(getCleanedHostName("lattice-10.cs.colostate.edu"));
    }

    public static byte[] readFileAsBytes(String filePath) throws IOException {
        return Files.readAllBytes(Paths.get(filePath));
    }

    public static ArrayList<String> getSliceHashesFromChunk(byte[] chunk) {
        List<byte[]> slices = splitFile(chunk, Constants.SLICE_SIZE);
        ArrayList<String> hashes = new ArrayList<>();
        for (byte[] slice : slices) {
            hashes.add(hash(slice));
        }

        return hashes;
    }

    public static String getCleanedHostName(String hostName) {
        int i = hostName.indexOf(".");
        return hostName.substring(0, i);
    }

    public static byte[] concat(byte[]... arrays) {
        int length = 0;
        for (byte[] array : arrays) {
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
        return chunkName.substring(0, i);
    }

    /**
     * Alternative to splitFile()
     * splitFile() has some bugs
     * TODO: unify the two methods
     */
    public static List<byte[]> divideArray(byte[] source, int chunksize) {
        List<byte[]> result = new ArrayList<byte[]>();
        int start = 0;
        while (start < source.length) {
            int end = Math.min(source.length, start + chunksize);
            result.add(Arrays.copyOfRange(source, start, end));
            start += chunksize;
        }

        return result;
    }

    public static List<byte[]> splitFile(byte[] bytes, int size) {
        List<byte[]> chunks = new ArrayList<>();
        for (int i = 0; i < bytes.length; i++) {
            byte[] chunk = new byte[Math.min(size, bytes.length - i)];
            log.debug("[DEBUG]: chunk.length: {}", chunk.length);
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
