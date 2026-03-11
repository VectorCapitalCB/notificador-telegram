package cl.tradelink.arb.notification.main.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class KafkaAdapterBinary extends Thread implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdapterBinary.class);
    private final String topic;
    private final MessageProcessor processor;
    private final KafkaConsumer<String, ByteBuffer> consumer;
    private volatile boolean running = true;

    public KafkaAdapterBinary(String brokers, String topic, String groupPrefix, MessageProcessor processor) {
        this.topic = topic.trim();
        this.processor = processor;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupPrefix + "-" + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class.getName());

        this.consumer = new KafkaConsumer<>(props);
        setName("kafka-binary-" + this.topic);
        setDaemon(true);
    }

    @Override
    public void run() {
        consumer.subscribe(List.of(topic));
        log.info("Kafka binary consumer iniciado en topic={}", topic);
        while (running && !Thread.currentThread().isInterrupted()) {
            ConsumerRecords<String, ByteBuffer> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, ByteBuffer> record : records) {
                if (record.value() != null) {
                    processor.onBinaryMessage(topic, record.value());
                }
            }
            if (!records.isEmpty()) {
                consumer.commitAsync();
            }
        }
        log.info("Kafka binary consumer detenido topic={}", topic);
    }

    public void startConsumer() {
        start();
    }

    @Override
    public void close() {
        running = false;
        interrupt();
        consumer.wakeup();
        consumer.close(Duration.ofSeconds(2));
    }
}
