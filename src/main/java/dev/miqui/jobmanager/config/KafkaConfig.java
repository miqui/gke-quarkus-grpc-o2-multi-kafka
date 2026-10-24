package dev.miqui.jobmanager.config;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * The Kafka producer the outbox relay publishes with: a plain client, so the relay decides when a
 * message counts as sent. Configured by the {@code kafka.*} properties (application.properties),
 * which quarkus-kafka-client collects as the {@code default-kafka-broker} map. Creating it doesn't
 * connect: an unreachable cluster shows up as failed sends, not as a failed startup - the API keeps
 * accepting jobs, and the outbox holds them until Kafka is back.
 */
@ApplicationScoped
public class KafkaConfig {

    @Produces
    @Singleton
    Producer<String, String> producer(@Identifier("default-kafka-broker") Map<String, Object> config) {
        return new KafkaProducer<>(new HashMap<>(config), new StringSerializer(), new StringSerializer());
    }

    /** Waits briefly for in-flight sends; anything unacknowledged stays in the outbox. */
    void close(@Disposes Producer<String, String> producer) {
        producer.close(Duration.ofSeconds(5));
    }
}
