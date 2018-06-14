package io.millesabords.demo.streamingsql.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaLogProducer extends LogProducer {

    private static final String TOPIC = "weblogs";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) throws Exception {
        new KafkaLogProducer().run();
    }

    private Producer<Integer, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaWeblogProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private void run() throws Exception {
        final Producer<Integer, String> producer = createProducer();

        try {
            while (true) {
                Log log = newLog();
                final ProducerRecord<Integer, String> record = new ProducerRecord<>(TOPIC, log.getId(), log.toJson());

                RecordMetadata metadata = producer.send(record).get();

                System.out.printf("sent record(key=%s value=%s) meta(partition=%d, offset=%d)\n",
                        record.key(), record.value(), metadata.partition(), metadata.offset());

                Thread.sleep(100);
            }
        }
        finally {
            producer.flush();
            producer.close();
        }
    }
}
