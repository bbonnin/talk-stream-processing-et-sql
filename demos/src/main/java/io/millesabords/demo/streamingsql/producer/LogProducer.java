package io.millesabords.demo.streamingsql.producer;

import net.andreinc.mockneat.MockNeat;
import net.andreinc.mockneat.types.enums.IPv4Type;

import java.util.Random;

public abstract class LogProducer {

    protected static final int[] STATUS = {
            200, 403, 404, 500
    };

    protected static final String[] URLS = {
            "/home", "/products", "/commands", "/help"
    };

    protected static final Random random = new Random();

    protected final MockNeat mock = MockNeat.threadLocal();

    protected String newCsvLog(String sep) {
        return String.join(sep,
                Long.toString(System.currentTimeMillis()),
                mock.ipv4s().type(IPv4Type.CLASS_A).val(),
                mock.from(URLS).val(),
                mock.fromInts(STATUS).valStr(),
                mock.ints().range(100, 5000).valStr());
    }

    protected String newJsonLog() {
        return newLog().toJson();
    }

    protected Log newLog() {
        return new Log(
                System.currentTimeMillis(),
                mock.ipv4s().type(IPv4Type.CLASS_A).val(),
                mock.from(URLS).val(),
                mock.fromInts(STATUS).val(),
                mock.ints().range(100, 5000).val());
    }
}
