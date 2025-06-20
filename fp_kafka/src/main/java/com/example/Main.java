package com.example;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;


public class Main {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // KafkaSource using the new API
        KafkaSource<UserEvent> source = KafkaSource.<UserEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("user-events")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new UserEventDeserializationSchema()) // custom deserializer
                .build();

        // Create stream
        DataStream<UserEvent> stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // Sink to Cassandra
        stream.addSink(new CassandraSink());

        env.execute("Flink Kafka Cassandra Job");
    }
}
