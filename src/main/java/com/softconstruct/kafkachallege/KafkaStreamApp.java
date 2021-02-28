package com.softconstruct.kafkachallege;

import com.softconstruct.kafkachallege.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import com.softconstruct.kafkachallege.consumer.ConsumerConfig;

import java.util.Arrays;
import java.util.Properties;

public class KafkaStreamApp {
    public Properties configure() {
        String bootstrapServers = "127.0.0.1:29092";
        String idConfig = "username-passwords";
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, idConfig);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return properties;
    }

    public KafkaStreams startStream(Properties properties) {
        String topic = "users";
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> kStream = builder.stream(topic);
        KTable<String, String> kTable = kStream
                .flatMapValues(p -> Arrays.asList(p.toLowerCase().split("\\W+")))
                .toTable();
        kTable.toStream().to(topic, Produced.with(Serdes.String(), Serdes.String()));
        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();
        return streams;
    }

    public static void main(String[] args) {
        Producer<String, String> producer = new ProducerConfig().produce();
        KafkaStreamApp configuration = new KafkaStreamApp();
        KafkaStreams kafkaStreams = configuration.startStream(configuration.configure());
        new ConsumerConfig().consume();
        producer.close();
        kafkaStreams.close();
    }
}
