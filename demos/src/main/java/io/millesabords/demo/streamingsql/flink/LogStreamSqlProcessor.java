package io.millesabords.demo.streamingsql.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

public class LogStreamSqlProcessor extends LogStreamProcessor {

    private static boolean mustStop = false;

    public static void main(final String[] args) throws Exception {
        new LogStreamSqlProcessor().run();
    }

    public LogStreamSqlProcessor() {
        initEnv();
        mustStop = false;
    }

    public void stop() {
        mustStop = true;
    }

    public void run(final String query) throws Exception {

        if (query.toLowerCase().trim().startsWith("describe")) {
            if (query.toLowerCase().trim().contains("weblogs")) {
                // Just for the demo, to show the schema of the table
                System.out.println();
                System.out.println("\tFIELD      | TYPE");
                System.out.println("\t-----------+--------");
                System.out.println("\tts         | Time");
                System.out.println("\tip_address | String");
                System.out.println("\turl        | String");
                System.out.println("\tstatus     | String");
                System.out.println("\tnb_bytes   | Integer");
                System.out.println();
                //Time, String, String, String, Integer
            }
            else {
                System.out.println("Unknown entity.");
            }

//            Thread.currentThread().stop();
            throw new InterruptedException();
        }

        tableEnv.registerDataStream("weblogs", dataset,
                "ts, ip_address, url, status, nb_bytes, rowtime.rowtime");

        tableEnv.registerDataStream("weblogs2", dataset,
                "ts, ip_address, url, status, nb_bytes, rowtime.rowtime");

        final Table table = tableEnv.sqlQuery(query);

        if (query.toLowerCase().contains("weblogs2")) {
            tableEnv.toRetractStream(table, Row.class).print();
        }
        else {
            tableEnv.toAppendStream(table, Row.class)
                    .filter(r -> {
                        if (mustStop) {
                            throw new InterruptedException();
                        }
                        return true;
                    })
                    .print();
        }

        execEnv.execute();
    }

    public void run() throws Exception {
        // No STREAM keyword => https://issues.apache.org/jira/browse/FLINK-4546
        run("SELECT url, TUMBLE_END(rowtime, INTERVAL '10' SECOND), COUNT(*) AS nb_requests " +
                "FROM weblogs " +
                "GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND), url");
    }
}
