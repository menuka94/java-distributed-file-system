package cs555.hw1.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class SerializeUtil {
    private static final Logger log = LogManager.getLogger(SerializeUtil.class);

    public static byte[] convertToBytes(Object object) throws IOException {
        log.info("convertToBytes()");
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(object);
            return bos.toByteArray();
        }
    }

    public static Object convertFromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        log.info("convertFromBytes()");
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             ObjectInputStream in = new ObjectInputStream(bis)) {
            return in.readObject();
        }
    }
}
