-- Transactional outbox: CreateJob writes the job and its Kafka message in one transaction, and the
-- outbox relay (service/OutboxRelay.java) publishes the message afterwards and deletes the row. The
-- job is never committed without its message, nor the message without its job; a crash between
-- the send and the delete publishes the message again (at-least-once - consumers dedupe on the
-- job's id and version).

CREATE TABLE job_outbox (
    id           BIGINT GENERATED ALWAYS AS IDENTITY,
    job_id       UUID         NOT NULL,
    -- jobs-<type with dots as dashes>, one topic per job type (k8s/kafka.yaml).
    topic        VARCHAR(249) NOT NULL,
    -- The Job message as protobuf JSON; the record value.
    payload      JSONB        NOT NULL,
    -- Not published before this: the job's run_after, pushed back after a failed send.
    available_at TIMESTAMPTZ  NOT NULL,
    attempts     INTEGER      NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT job_outbox_pkey PRIMARY KEY (id),
    -- A job deleted before its message went out takes the message with it.
    CONSTRAINT job_outbox_job_id_fkey FOREIGN KEY (job_id) REFERENCES jobs (id) ON DELETE CASCADE
);

-- The relay: due rows, oldest first (FOR UPDATE SKIP LOCKED).
CREATE INDEX ix_job_outbox_available_at ON job_outbox (available_at, id);
-- The ON DELETE CASCADE above.
CREATE INDEX ix_job_outbox_job_id ON job_outbox (job_id);
