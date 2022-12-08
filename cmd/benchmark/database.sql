CREATE DATABASE varlog_benchmark WITH OWNER varlog_benchmark;

CREATE TYPE execution_trigger AS ENUM ('cron', 'command');

CREATE TABLE execution
(
    id          serial            NOT NULL PRIMARY KEY,
    commit_hash varchar           NOT NULL UNIQUE,
    trigger     execution_trigger NOT NULL NOT NULL,
    start_time  timestamp         NOT NULL,
    finish_time timestamp         NOT NULL
);

CREATE TABLE macrobenchmark_workload
(
    id          serial  NOT NULL PRIMARY KEY,
    name        varchar NOT NULL UNIQUE,
    description text
);

CREATE TABLE macrobenchmark
(
    id           serial    NOT NULL PRIMARY KEY,
    execution_id serial    NOT NULL REFERENCES execution (id) ON DELETE CASCADE,
    workload_id  serial    NOT NULL REFERENCES macrobenchmark_workload (id),
    start_time   timestamp NOT NULL,
    finish_time  timestamp NOT NULL
);

CREATE TABLE macrobenchmark_target
(
    id   serial  NOT NULL PRIMARY KEY,
    name varchar NOT NULL UNIQUE
);

CREATE TABLE macrobenchmark_metric
(
    id          serial  NOT NULL PRIMARY KEY,
    name        varchar NOT NULL UNIQUE,
    description text
);

CREATE TABLE macrobenchmark_result
(
    macrobenchmark_id serial NOT NULL REFERENCES macrobenchmark (id) ON DELETE CASCADE,
    target_id         serial NOT NULL REFERENCES macrobenchmark_target (id),
    metric_id         serial NOT NULL REFERENCES macrobenchmark_metric (id),
    value             float  NOT NULL,
    PRIMARY KEY (macrobenchmark_id, target_id, metric_id)
);

CREATE TABLE microbenchmark
(
    id           serial    NOT NULL PRIMARY KEY,
    execution_id serial    NOT NULL REFERENCES execution (id),
    start_time   timestamp NOT NULL,
    finish_time  timestamp NOT NULL
);

CREATE TABLE microbenchmark_package
(
    id   serial  NOT NULL PRIMARY KEY,
    name varchar NOT NULL UNIQUE
);

CREATE TABLE microbenchmark_result
(
    id                serial  NOT NULL PRIMARY KEY,
    microbenchmark_id serial  NOT NULL REFERENCES microbenchmark (id),
    package_id        serial  NOT NULL REFERENCES microbenchmark_package (id),
    function_name     varchar NOT NULL,
    ns_per_op         float,
    allocs_per_op     float
);

INSERT INTO execution (id, commit_hash, trigger, start_time, finish_time)
VALUES (1, '613aa577', 'cron', '2022-12-01 00:05:00', '2022-12-01 00:15:00'),
       (2, '923f35f0', 'cron', '2022-12-02 00:05:00', '2022-12-02 00:15:00'),
       (3, '55703a48', 'cron', '2022-12-03 00:05:00', '2022-12-03 00:15:00');

INSERT INTO macrobenchmark_workload (id, name)
VALUES (1, 'one_logstream'),
       (2, 'all_logstream');

INSERT INTO macrobenchmark (id, execution_id, workload_id, start_time, finish_time)
VALUES (1, 1, 1, '2022-12-01 00:05:00', '2022-12-01 00:10:00'),
       (2, 1, 2, '2022-12-01 00:10:00', '2022-12-01 00:15:00'),
       (3, 2, 1, '2022-12-02 00:05:00', '2022-12-02 00:10:00'),
       (4, 2, 2, '2022-12-02 00:10:00', '2022-12-02 00:15:00'),
       (5, 3, 1, '2022-12-03 00:05:00', '2022-12-03 00:10:00'),
       (6, 3, 2, '2022-12-03 00:10:00', '2022-12-03 00:15:00');

INSERT INTO macrobenchmark_target (id, name)
VALUES (1, '1:1'),
       (2, '1:*');

INSERT INTO macrobenchmark_metric (id, name)
VALUES (1, 'append_requests_per_second'),
       (2, 'append_bytes_per_second'),
       (3, 'append_durations_ms'),
       (4, 'subscribe_logs_per_second'),
       (5, 'subscribe_bytes_per_second'),
       (6, 'end_to_end_latency_ms');

INSERT INTO macrobenchmark_result (macrobenchmark_id, target_id, metric_id, value)
VALUES (1, 1, 2, 100.0),
       (1, 1, 5, 90.0),
       (2, 2, 2, 200.0),
       (2, 2, 5, 180.0),
       (3, 1, 2, 101.0),
       (3, 1, 5, 91.0),
       (4, 2, 2, 202.0),
       (4, 2, 5, 182.0),
       (5, 1, 2, 103.0),
       (5, 1, 5, 93.0),
       (6, 2, 2, 206.0),
       (6, 2, 5, 186.0);

