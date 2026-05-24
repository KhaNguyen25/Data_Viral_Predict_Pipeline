package edu.hust.edgededuplicate.kafka;

import edu.hust.edgededuplicate.utill.ConfigurationManager;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaRequestProducer {

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final boolean enabled;

    public KafkaRequestProducer() {
        this.enabled = Boolean.parseBoolean(ConfigurationManager.getProperty("kafka.enable"));
        this.topic = ConfigurationManager.getProperty("kafka.topic");

        if (!enabled) {
            this.producer = null;
            return;
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", ConfigurationManager.getProperty("kafka.bootstrap.servers"));
        props.put("client.id", "ripple_edge_node_producer");

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Đồng bộ với code Python của bạn bạn
        props.put("acks", "1");
        props.put("linger.ms", "5");
        props.put("batch.size", "16384");

        this.producer = new KafkaProducer<>(props);
    }

    public void send(String key, String value) {
        if (!enabled || producer == null) {
            return;
        }

        ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, key, value);


        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("[Kafka] Gửi thất bại: " + exception.getMessage());
            }
        });


    }

    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }
}