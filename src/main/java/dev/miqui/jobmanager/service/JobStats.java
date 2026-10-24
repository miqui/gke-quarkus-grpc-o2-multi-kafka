package dev.miqui.jobmanager.service;

import dev.miqui.jobmanager.repo.Db;
import dev.miqui.jobmanager.repo.JobRepository;
import dev.miqui.jobmanager.repo.OutboxRepository;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.HashMap;
import java.util.Map;

/**
 * Refreshes the {@code jobs{state}} and {@code job.outbox.pending} gauges from the database. Every
 * replica reports the same value.
 */
@ApplicationScoped
public class JobStats {

    private final Db db;
    private final JobRepository jobs;
    private final OutboxRepository outbox;
    private final JobMetrics metrics;

    public JobStats(Db db, JobRepository jobs, OutboxRepository outbox, JobMetrics metrics) {
        this.db = db;
        this.jobs = jobs;
        this.outbox = outbox;
        this.metrics = metrics;
    }

    @Scheduled(identity = "job-stats", every = "${app.jobs.stats-interval}",
            concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    public void refresh() {
        Map<String, Long> counts = new HashMap<>();
        db.tx(jobs::countByState).forEach(row -> counts.put(row.state(), row.count()));
        metrics.jobsByState(counts);
        metrics.outboxPending(db.tx(outbox::count));
    }
}
