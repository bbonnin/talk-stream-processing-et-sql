package io.millesabords.demo.streamingsql.flink


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row


object StreamSqlProcessor extends StreamProcessor {

    def main(args: Array[String]) {
        updateClasses()
        run()
    }

    def run() {

        val execEnv = StreamExecutionEnvironment.getExecutionEnvironment
        execEnv.setParallelism(1)
        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val tableEnv = TableEnvironment.getTableEnvironment(execEnv)
        val stream = execEnv.socketTextStream("localhost", 5000, '\n')
        val datastream = stream.map(logMapper).assignTimestampsAndWatermarks(tsExtractor)

        tableEnv.registerDataStream("weblogs", datastream, 'ts, 'ip_address, 'url, 'status, 'nb_bytes, 'rowtime.rowtime)
        //tableEnv.registerDataStream("weblogs", stream, "ts, ip_address, url, status, nb_bytes, rowtime.rowtime")

        val query = tableEnv.sqlQuery("""
        | SELECT url,
        |     TUMBLE_END(rowtime, INTERVAL '10' SECOND),
        |     COUNT(*) AS nb_requests
        | FROM weblogs
        | GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND), url
        """.stripMargin)

        tableEnv.toAppendStream[Row](query).print()

        execEnv.execute()
    }
}

