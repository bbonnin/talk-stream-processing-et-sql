package io.millesabords.demo.streamingsql.flink;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.types.Row;

public class LogStreamTableApiProcessor extends LogStreamProcessor {

    public static void main(final String[] args) throws Exception {
        new LogStreamTableApiProcessor().run();
    }

    public LogStreamTableApiProcessor() {
        initEnv();
    }

    public void run() throws Exception {

        final Table table = tableEnv
                .fromDataStream(dataset, "ts, ip_address, url, status, nb_bytes, rowtime.rowtime")
                .window(Tumble.over("10.second").on("rowtime").as("TenSecondsWindow"))
                .groupBy("TenSecondsWindow, url")
                .select("url, TenSecondsWindow.end as time, url.count as nb_requests");

        tableEnv.toAppendStream(table, Row.class).print();

        execEnv.execute();
    }
}
