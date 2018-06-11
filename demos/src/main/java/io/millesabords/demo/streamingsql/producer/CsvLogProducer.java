package io.millesabords.demo.streamingsql.producer;

import java.io.FileOutputStream;
import java.io.PrintWriter;

public class CsvLogProducer extends LogProducer {

    public static void main(final String[] args) {
        new CsvLogProducer().run();
    }

    private void run() {
        try {
            final PrintWriter out = new PrintWriter(new FileOutputStream("target/classes/logs/WEBLOGS.csv", true));

            while (true) {
                out.println(newCsvLog(","));
                out.flush();
                Thread.sleep(random.nextInt(200) + 50);
            }
        }
        catch (final Exception ex) {
            ex.printStackTrace();
        }
    }
}
