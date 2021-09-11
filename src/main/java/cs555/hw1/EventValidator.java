package cs555.hw1;

import org.apache.logging.log4j.Logger;

public class EventValidator {
    public static void validateEventType(byte messageType, int expectedType, Logger logger) {
        if (messageType != expectedType) {
            logger.warn("Unexpected message type");
        }
    }
}
