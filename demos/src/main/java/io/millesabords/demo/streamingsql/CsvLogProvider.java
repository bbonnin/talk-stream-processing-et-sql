package io.millesabords.demo.streamingsql;

import net.andreinc.mockneat.MockNeat;
import net.andreinc.mockneat.types.enums.IPv4Type;

import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Random;

public class CsvLogProvider {

    private static final int[] STATUS = {
            200, 403, 404, 500
    };

    private static final String[] URLS = {
            "/home", "/products", "/commands", "/help"
    };

    private static final Random random = new Random();

    public static void main(final String[] args) {

        final MockNeat mock = MockNeat.threadLocal();

        try {
            final PrintWriter out = new PrintWriter(new FileOutputStream("target/classes/logs/WEBLOGS.csv", true));

            while (true) {
                out.println(String.join(",",
                        Long.toString(System.currentTimeMillis()),
                        mock.ipv4s().type(IPv4Type.CLASS_A).val(),
                        mock.from(URLS).val(),
                        mock.fromInts(STATUS).valStr(),
                        mock.ints().range(100, 5000).valStr()));
                out.flush();
                Thread.sleep(random.nextInt(200) + 50);
            }
        }
        catch (final Exception ex) {
            ex.printStackTrace();
        }
    }
}
