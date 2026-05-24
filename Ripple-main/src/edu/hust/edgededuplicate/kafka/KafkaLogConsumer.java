package edu.hust.edgededuplicate.kafka;

import edu.hust.edgededuplicate.utill.ConfigurationManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaLogConsumer {
    public void startListening() {
        Properties props = new Properties();
        props.put("bootstrap.servers", ConfigurationManager.getProperty("kafka.bootstrap.servers"));
        props.put("group.id", "ripple-group");
        props.put("key.deserializer", "org.apache.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("tiktok-viral-logs"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                // Tại đây bạn sẽ thực hiện:
                // 1. Phân tích JSON log.
                // 2. Tính toán đặc trưng (Z-score).
                // 3. Đẩy kết quả vào Redis.
                System.out.println("Nhận log mới: " + record.value());
            }
        }
    }
}
