package io.millesabords.demo.streamingsql.flink

import java.sql.Time

import javassist.ClassPool
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor

trait StreamProcessor {

    val logMapper = new MapFunction[String, (Time, String, String, String, Integer)]() {
        @throws[Exception]
        override def map(log: String): (Time, String, String, String, Integer) = { // <timestamp> <IP address> <method> <url> <#bytes>
            val fields = log.split("\t")
            val logDate = new Time(fields(0).toLong)
            val nbBytes = fields(4).toInt
            (logDate, fields(1), fields(2), fields(3), nbBytes)
        }
    }

    val tsExtractor = new AscendingTimestampExtractor[(Time, String, String, String, Integer)]() {
        override def extractAscendingTimestamp(element: (Time, String, String, String, Integer)): Long = element._1.getTime
    }

    def updateClasses() {
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
