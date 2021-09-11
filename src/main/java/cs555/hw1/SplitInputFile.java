package cs555.hw1;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

/**
 * Split Input File
 */

public class SplitInputFile {
    private static String FILE_NAME = "test_file.csv";
    private static int PART_SIZE = 64000;

    public static void main(final String[] args)
            throws IOException {
        final Path file = Paths.get(FILE_NAME).toRealPath();
        final String filenameBase = file.getFileName().toString();
        final byte[] buf = new byte[PART_SIZE];

        int partNumber = 0;
        Path part;
        int bytesRead;
        byte[] toWrite;

        try (
                final InputStream in = Files.newInputStream(file);
        ) {
            while ((bytesRead = in.read(buf)) != -1) {
                part = file.resolveSibling(filenameBase + ".part" + partNumber);
                toWrite = bytesRead == PART_SIZE ? buf : Arrays.copyOf(buf, bytesRead);
                Files.write(part, toWrite, StandardOpenOption.CREATE_NEW);
                partNumber++;
            }
        }
    }
}
