package cs555.hw1.wireformats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReportFileCorruption extends Event {
    private static final Logger log = LogManager.getLogger(ReportFileCorruption.class);

    public ReportFileCorruption() {

    }

    public ReportFileCorruption(byte[] marshalledBytes) {

    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }

    @Override
    public int getType() {
        return Protocol.REPORT_FILE_CORRUPTION;
    }
}
