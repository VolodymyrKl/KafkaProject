package com.softconstruct.kafkachallege.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class ConsumerConfig {
    public void consume() {
        Logger logger = LoggerFactory.getLogger(ConsumerConfig.class.getName());
        String topic = "users";
        Properties properties = getProperties(topic);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            ConsumerRecords<String, String> records = consumer.poll(10);
            do {
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key - " + record.key() + ". Value - " + record.value());
                    logger.info("Partition - " + record.partition() + ". Offset - " + record.offset());
                }
            } while (records.count() > 0);
        }
    }

    private Properties getProperties(String topic) {
        String bootstrapServers = "127.0.0.1:29092";
        Properties properties = new Properties();
        properties.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, topic);
        return properties;
    }
}
