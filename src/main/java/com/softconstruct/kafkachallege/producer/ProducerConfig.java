package com.softconstruct.kafkachallege.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ProducerConfig {
    public Producer<String, String> produce() {
        Properties properties = getProperties();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record;
        for (int i = 0; i < 20; i++) {
            record = new ProducerRecord<>("username", "password");
            producer.send(record, (recordMetadata, e) -> {
                Logger logger = LoggerFactory.getLogger(ProducerConfig.class);
                Properties logProperties = new Properties();
                try {
                    logProperties.load(new FileInputStream("src/main/resources/log4j.properties"));
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
                if (e == null) {
                    logger.info("Successfully \n " +
                            "Topic - " + recordMetadata.topic() + "\n" +
                            "Partition - " + recordMetadata.partition() + "\n" +
                            "Offset - " + recordMetadata.offset() + "\n" +
                            "Timestamp - " + recordMetadata.timestamp());
                } else {
                    logger.error("Can't produce, error ", e);
                }
            });
            producer.flush();
        }
        return producer;
    }

    private Properties getProperties() {
        String bootstrapServers = "127.0.0.1:29092";
        Properties properties = new Properties();
        properties.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}