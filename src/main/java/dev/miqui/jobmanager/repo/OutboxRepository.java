package dev.miqui.jobmanager.repo;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import dev.miqui.jobmanager.v1.Job;
import jakarta.enterprise.context.ApplicationScoped;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

/** The job_outbox table: Kafka messages written with the job, published by the outbox relay. */
@ApplicationScoped
public class OutboxRepository {

    private static final JsonFormat.Printer PRINTER = JsonFormat.printer().omittingInsignificantWhitespace();

    /** One message to publish: the record key is the job id, the value the job as JSON. */
    public record Message(long id, String jobId, String topic, String payload) {
    }

    /** jobs-&lt;type&gt; with dots as dashes: report.generate goes to jobs-report-generate. */
    public static String topic(String type) {
        return "jobs-" + type.replace('.', '-');
    }

    /** In the transaction that created {@code job}; due at its run_after. */
    public void insert(Connection c, Job job) throws SQLException {
        String payload;
        try {
            payload = PRINTER.print(job);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Job is not representable as JSON", e);
        }
        Db.update(c, """
                        INSERT INTO job_outbox (job_id, topic, payload, available_at)
                        SELECT id, ?, ?::jsonb, run_after FROM jobs WHERE id = ?
                        """,
                topic(job.getType()), payload, UUID.fromString(job.getId()));
    }

    /**
     * Locks up to {@code batch} due messages until the transaction ends, oldest first. SKIP LOCKED
     * lets every replica run the relay without two of them sending the same rows.
     */
    public List<Message> lockDue(Connection c, int batch) throws SQLException {
        return Db.list(c, """
                        SELECT id, job_id, topic, payload::text AS payload FROM job_outbox
                        WHERE available_at <= now()
                        ORDER BY available_at, id
                        LIMIT ?
                        FOR UPDATE SKIP LOCKED
                        """,
                rs -> new Message(rs.getLong("id"), rs.getString("job_id"), rs.getString("topic"),
                        rs.getString("payload")),
                batch);
    }

    public void delete(Connection c, List<Long> ids) throws SQLException {
        Db.update(c, "DELETE FROM job_outbox WHERE id = ANY(?)", new Db.LongArray(ids));
    }

    /** Due again after {@code backoff}; the message keeps its place behind newer ones until then. */
    public void retryLater(Connection c, List<Long> ids, Duration backoff) throws SQLException {
        Db.update(c, """
                        UPDATE job_outbox
                        SET attempts = attempts + 1, available_at = now() + make_interval(secs => ?::int)
                        WHERE id = ANY(?)
                        """,
                (int) backoff.toSeconds(), new Db.LongArray(ids));
    }

    public long count(Connection c) throws SQLException {
        return Db.count(c, "SELECT count(*) FROM job_outbox");
    }
}
