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


/**
  * To review by a Scala export :)
  */
object LogStreamTableProcessor {

    def main(args: Array[String]) {
        updateClasses()
        run()
    }

    private val logMapper = new MapFunction[String, (Time, String, String, String, Integer)]() {
        @throws[Exception]
        override def map(log: String): (Time, String, String, String, Integer) = { // <timestamp> <IP address> <method> <url> <#bytes>
            val fields = log.split("\t")
            val logDate = new Time(fields(0).toLong)
            val nbBytes = fields(4).toInt
            (logDate, fields(1), fields(2), fields(3), nbBytes)
        }
    }

    private val tsExtractor = new AscendingTimestampExtractor[(Time, String, String, String, Integer)]() {
        override def extractAscendingTimestamp(element: (Time, String, String, String, Integer)): Long = element._1.getTime
    }


    private def run() {

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

    private def updateClasses() {
        try {
            val pool = ClassPool.getDefault
            val cc = pool.get("org.apache.flink.types.Row")
            val m = cc.getDeclaredMethod("toString")
            m.insertAfter("$_ = $_.replace(',', '\\t\\t');")
            cc.toClass //writeFile();

        } catch {
            case e: Exception =>
                e.printStackTrace()
        }
    }
}

