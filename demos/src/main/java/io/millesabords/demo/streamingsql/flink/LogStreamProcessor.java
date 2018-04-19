package io.millesabords.demo.streamingsql.flink;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.sql.Time;

public abstract class LogStreamProcessor {

    protected StreamExecutionEnvironment execEnv;

    protected StreamTableEnvironment tableEnv;

    protected DataStream<Tuple5<Time, String, String, String, Integer>> dataset;


    private static final MapFunction<String, Tuple5<Time, String, String, String, Integer>> logMapper =
            new MapFunction<String, Tuple5<Time, String, String, String, Integer>>() {

                @Override
                public Tuple5<Time, String, String, String, Integer> map(final String log) throws Exception {
                    // <timestamp> <IP address> <method> <url> <#bytes>
                    final String[] fields = log.split("\t");
                    final Time logDate = new Time(Long.parseLong(fields[0]));
                    final int nbBytes = Integer.parseInt(fields[4]);
                    return new Tuple5<>(logDate, fields[1], fields[2], fields[3], nbBytes);
                }
            };

    private static final AscendingTimestampExtractor tsExtractor =
            new AscendingTimestampExtractor<Tuple5<Time, String, String, String, Integer>>() {

                @Override
                public long extractAscendingTimestamp(final Tuple5<Time, String, String, String, Integer> element) {
                    return element.f0.getTime();
                }
            };

    protected void initEnv() {

        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(1);

        tableEnv = TableEnvironment.getTableEnvironment(execEnv);
        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final DataStream<String> stream = execEnv.socketTextStream("localhost", 5000, "\n");

        dataset = stream
                .map(logMapper)
                .assignTimestampsAndWatermarks(tsExtractor);

        try {
            final ClassPool pool = ClassPool.getDefault();
            final CtClass cc = pool.get("org.apache.flink.types.Row");
            final CtMethod m = cc.getDeclaredMethod("toString");
            m.insertAfter("$_ = $_.replace(',', '\\t\\t');");
            cc.toClass();//writeFile();
        }
        catch (final Exception e) {
            e.printStackTrace();
        }
    }
}
