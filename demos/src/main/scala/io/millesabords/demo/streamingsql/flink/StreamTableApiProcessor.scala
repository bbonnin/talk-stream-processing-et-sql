package io.millesabords.demo.streamingsql.flink

import java.sql.Time

import javassist.ClassPool
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row


object StreamTableApiProcessor extends StreamProcessor {

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

        val table = tableEnv
            .fromDataStream(datastream, 'ts, 'ip_address, 'url, 'status, 'nb_bytes, 'rowtime.rowtime)
            .window(Tumble over 10.second on 'rowtime as 'tenSecondsWindow)
            .groupBy('tenSecondsWindow, 'url)
            .select('url, 'tenSecondsWindow.end as 'time, 'url.count as 'nb_requests)

        tableEnv.toAppendStream[Row](table).print()

        execEnv.execute()
    }
}

