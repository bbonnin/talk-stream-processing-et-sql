package io.millesabords.demo.streamingsql.flink;



//import org.apache.flink.table.api.TableEnvironment;

import org.apache.calcite.adapter.csv.CsvSchemaFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

public class StaticTableProcessor {

    public static void main(final String[] args) throws Exception {
        CsvSchemaFactory y;
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        final String[] fields = { "ts", "ip_address", "url", "status", "nb_bytes" };
        final TypeInformation<?>[] fieldTypes = { Types.SQL_TIMESTAMP, Types.STRING, Types.STRING, Types.INT, Types.INT };
        final TableSource csvSource = new CsvTableSource("weblogs.csv", fields, fieldTypes);

        tableEnv.registerTableSource("weblogs", csvSource);

        /*final String query =
                "SELECT url, TUMBLE_END(ts, INTERVAL '10' SECOND) AS duree, COUNT(url) AS nb_requests " +
                "FROM weblogs " +
                "GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), url";*/
        //final String query = "SELECT TUMBLE_END(ts, INTERVAL '10' SECOND) as duree, url FROM weblogs GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), url";
        //final String query = "SELECT url, count(*) FROM weblogs GROUP BY url";
        //final Table table = tableEnv.sql(query);

        //tableEnv.toDataSet(table, Row.class).print();

        /* THIS PART WORKS \O/ */
        final Table table = tableEnv.scan("weblogs")
                .select("url, ts")
                .window(Tumble.over("10.second").on("ts").as("TenSecondsWindow"))
                .groupBy("TenSecondsWindow, url")
                .select("url, TenSecondsWindow.end as time, url.count as nb_requests");
        tableEnv.toDataSet(table, Row.class).print();
    }
}
