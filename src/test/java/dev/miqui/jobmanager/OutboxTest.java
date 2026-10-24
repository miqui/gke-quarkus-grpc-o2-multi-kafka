package dev.miqui.jobmanager;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import dev.miqui.jobmanager.repo.OutboxRepository;
import dev.miqui.jobmanager.service.JobMetrics;
import dev.miqui.jobmanager.service.JobStats;
import dev.miqui.jobmanager.service.OutboxRelay;
import dev.miqui.jobmanager.v1.Job;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/** CreateJob's outbox row, and the relay that publishes it - to the Dev Services broker, or a stub. */
@QuarkusTest
class OutboxTest extends GrpcTestSupport {

    @Inject
    OutboxRelay relay;

    @Inject
    OutboxRepository outbox;

    @Inject
    JobMetrics metrics;

    @Inject
    JobStats stats;

    @Inject
    SimpleMeterRegistry registry;

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;

    @Test
    void topicsAreNamedAfterTheJobType() {
        assertThat(OutboxRepository.topic("report.generate")).isEqualTo("jobs-report-generate");
        assertThat(OutboxRepository.topic("demo.echo")).isEqualTo("jobs-demo-echo");
    }

    @Test
    void createWritesTheJobsMessageAndTheRelayPublishesIt() throws Exception {
        double before = counter("job.outbox.published", "topic", "jobs-report-generate");
        Job job = jobs.createJob(newJob("report.generate").setSpec(struct("template", "monthly")).build());
        assertThat(count("SELECT count(*) FROM job_outbox WHERE job_id = ?::uuid AND topic = 'jobs-report-generate'",
                job.getId())).isOne();

        assertThat(relay.publish()).isOne();

        assertThat(count("SELECT count(*) FROM job_outbox")).isZero();
        assertThat(counter("job.outbox.published", "topic", "jobs-report-generate")).isEqualTo(before + 1);
        ConsumerRecord<String, String> record = consume("jobs-report-generate", job.getId());
        Job.Builder published = Job.newBuilder();
        JsonFormat.parser().merge(record.value(), published);
        assertThat(published.build()).isEqualTo(job);
    }

    @Test
    void anIdempotentRetryWritesNoSecondMessage() {
        jobs.createJob(newJob("demo.echo").setIdempotencyKey("outbox-1").build());
        jobs.createJob(newJob("demo.echo").setIdempotencyKey("outbox-1").build());

        assertThat(count("SELECT count(*) FROM job_outbox")).isOne();
    }

    @Test
    void aScheduledJobIsPublishedAtItsRunAfter() {
        Instant later = Instant.now().plus(Duration.ofHours(1));
        jobs.createJob(newJob("demo.echo").setRunAfter(Timestamp.newBuilder().setSeconds(later.getEpochSecond())).build());

        assertThat(relay.publish()).isZero();
        assertThat(count("SELECT count(*) FROM job_outbox")).isOne();

        sql("UPDATE job_outbox SET available_at = now()");
        assertThat(relay.publish()).isOne();
    }

    @Test
    void theRelayWorksInBatches() {
        for (int i = 0; i < 150; i++) {
            createJob("demo.sleep");
        }
        stats.refresh();
        assertThat(registry.get("job.outbox.pending").gauge().value()).isEqualTo(150);

        assertThat(relay.publish()).isEqualTo(150);
        assertThat(relay.publish()).isZero();
        stats.refresh();
        assertThat(registry.get("job.outbox.pending").gauge().value()).isZero();
    }

    @Test
    void aDeletedJobTakesItsUnsentMessageWithIt() {
        Job job = createJob();
        sql("UPDATE jobs SET state = 'CANCELLED' WHERE id = ?::uuid", job.getId());
        sql("DELETE FROM jobs WHERE id = ?::uuid", job.getId());

        assertThat(count("SELECT count(*) FROM job_outbox")).isZero();
    }

    @Test
    void failedSendsArePushedBackAndTheRestIsPublished() {
        double failuresBefore = counter("job.outbox.failures", "topic", "jobs-email-send");
        createJob("demo.echo");
        createJob("email.send");
        createJob("email.send");
        createJob("report.generate");
        createJob("demo.sleep");
        StubProducer producer = new StubProducer();
        OutboxRelay stubbed = new OutboxRelay(db, outbox, producer, metrics);

        assertThat(stubbed.publish()).as("demo.echo only").isOne();

        // email.send fails on the spot: its second message isn't even tried.
        assertThat(producer.attempts).containsExactly("jobs-demo-echo", "jobs-email-send", "jobs-report-generate",
                "jobs-demo-sleep");
        assertThat(count("SELECT count(*) FROM job_outbox")).isEqualTo(4);
        assertThat(count("SELECT count(*) FROM job_outbox WHERE attempts = 1 AND available_at > now() + interval '20 seconds'"))
                .isEqualTo(4);
        assertThat(counter("job.outbox.failures", "topic", "jobs-email-send")).isEqualTo(failuresBefore + 2);
        assertThat(stubbed.publish()).as("nothing due until the backoff passes").isZero();
    }

    /**
     * Acknowledges demo.echo; email.send throws, report.generate fails at once (no metadata) and
     * demo.sleep fails after the send returned (the brokers refused it).
     */
    private static final class StubProducer extends MockProducer<String, String> {

        final List<String> attempts = new ArrayList<>();

        @Override
        public synchronized Future<RecordMetadata> send(ProducerRecord<String, String> record, Callback callback) {
            attempts.add(record.topic());
            return switch (record.topic()) {
                case "jobs-email-send" -> throw new KafkaException("not authorized");
                case "jobs-report-generate" -> CompletableFuture.failedFuture(new KafkaException("no metadata"));
                case "jobs-demo-sleep" -> CompletableFuture.supplyAsync(() -> {
                    throw new KafkaException("not enough replicas");
                }, CompletableFuture.delayedExecutor(50, java.util.concurrent.TimeUnit.MILLISECONDS));
                default -> CompletableFuture.completedFuture(
                        new RecordMetadata(new TopicPartition(record.topic(), 0), 0, 0, 0, 0, 0));
            };
        }
    }

    private ConsumerRecord<String, String> consume(String topic, String key) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "outbox-test-" + key,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plusSeconds(30);
            while (Instant.now().isBefore(deadline)) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (key.equals(record.key())) {
                        return record;
                    }
                }
            }
            throw new AssertionError("no record with key " + key + " on " + topic);
        }
    }

    private double counter(String name, String... tags) {
        Counter counter = registry.find(name).tags(tags).counter();
        return counter == null ? 0 : counter.count();
    }
}
