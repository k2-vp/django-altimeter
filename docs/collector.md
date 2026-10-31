# Database and task context

`deploy/collector.yaml` is the only Collector configuration. Applications keep
sending their selected signals directly to PostHog. The collector independently
observes database/Redis aggregates or its own ECS task. It has no OTLP listener,
host filesystem/socket mount, cloud API polling, disk spool or application hook.
Stopping it leaves a gap in infrastructure context; it does not stop requests.

The tested image is:

```text
otel/opentelemetry-collector-contrib:0.164.0@sha256:340c9ca6a36617084c6661bdc4f0fe92b7cda5a805d61673ec8db375fddc522b
```

The **distribution** reports `0.164.0`; its **contrib components** are `0.164.1`.
Upstream's [patch release](https://github.com/open-telemetry/opentelemetry-collector-contrib/releases/tag/v0.164.1)
and [distribution manifest](https://github.com/open-telemetry/opentelemetry-collector-releases/blob/v0.164.0/distributions/otelcol-contrib/manifest.yaml)
explain this distinction. There is no official `0.164.1` image tag. The reference
Dockerfile copies the binary from this digest into both verification images.

## Select one source owner

Use the same file and the native Collector environment provider in both profiles.
Its YAML-list substitution selects active receivers. Definitions for inactive
receivers remain in the file but are never instantiated.

| Profile | `ALTIMETER_COLLECTOR_RECEIVERS` | Owner |
| --- | --- | --- |
| Kamal | `[postgresql, redis, sqlquery/lock_wait]` | One fleet observer per database/Redis source pair |
| ECS | `[awsecscontainermetrics]` | One observer per ECS task, using that task's V4 endpoint |

Do not enable fleet receivers in every app sidecar. Deploy the fleet observer
once, including when the applications run on ECS. Run one replica and replace
it without overlapping writers. ECS sidecars collect task-only context and must
not duplicate the fleet observer. Select a smaller list to disable a source:
for example `[redis]` omits both Postgres receivers. Stop the collector when no
source is needed; an empty receiver list is not a valid running pipeline.

Copy the existing file into the pinned image at its default configuration path
`/etc/otelcol-contrib/config.yaml` during your image build. A private CA certificate
can also be copied into that image; no host mounts are needed. Use your deployment's
existing secret injection for credentials. Run unprivileged, with a read-only
root filesystem, no added capabilities, a 512 MiB container memory limit and
`GOMEMLIMIT=192MiB`. No port needs publishing.

Required environment for either profile:

- `ALTIMETER_COLLECTOR_RECEIVERS`: exactly the selected YAML list above.
- `ALTIMETER_COLLECTOR_INSTANCE_ID`: a fresh UUID for this observer process,
  supplied at each deployment/start. Do not share it between running observers.
- `ALTIMETER_METRICS_ENDPOINT`: the full HTTPS metrics ingestion URL supplied by
  your PostHog project, ending in `/i/v1/metrics`; not a base OTLP URL.
- `ALTIMETER_METRICS_AUTHORIZATION`: the configured authorization header value,
  injected out of band. Never put credentials in the YAML, image or resource.
- `ALTIMETER_COLLECTION_INTERVAL`: optional; defaults to `30s`. Keep it finite
  and long enough for the database workload. Tests use `500ms` on isolated sources.

The ECS profile additionally uses the platform-provided
`ECS_CONTAINER_METADATA_URI_V4`. It reads only `/task` and `/task/stats` on that
endpoint. It does not query EC2, CloudWatch or other tasks, and needs no cloud
polling credentials. Missing or denied metadata is an optional gap. The local
HTTP-boundary tests validate parsing and units; they do not prove deployed ECS.

Validate with the same environment and pinned binary before deployment:

```sh
otelcol-contrib validate --config=deploy/collector.yaml
```

The repository's README and AGENTS.md provide the two exact-wheel verification
commands. They run `tests/integration/test_collector_profiles.py` in each image.
These are local real-process tests, not live PostHog or deployed Kamal/ECS proof.

## Postgres and Redis access

For the Kamal/fleet profile set:

| Variable | Meaning |
| --- | --- |
| `ALTIMETER_PG_HOST`, `ALTIMETER_PG_PORT` | Direct monitoring host and port; default port `5432` |
| `ALTIMETER_PG_DATABASE` | One explicitly allowed database; also the connection database |
| `ALTIMETER_PG_USERNAME`, `ALTIMETER_PG_PASSWORD` | Read-only monitoring login |
| `ALTIMETER_PG_CA_FILE` | Optional trusted CA file; otherwise system trust |
| `ALTIMETER_REDIS_ENDPOINT` | Redis TLS host and port, including the port |
| `ALTIMETER_REDIS_USERNAME`, `ALTIMETER_REDIS_PASSWORD` | INFO-only ACL login |
| `ALTIMETER_REDIS_CA_FILE` | Optional trusted CA file; otherwise system trust |

Both receivers verify certificate chains and host names explicitly. Never enable
`insecure` or `insecure_skip_verify` to work around a certificate error. The
[Postgres receiver](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.164.1/receiver/postgresqlreceiver)
has a permissive verification default that this file overrides. The
[Redis receiver](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.164.1/receiver/redisreceiver)
defaults to plaintext, also overridden here.

Create a dedicated Postgres login without superuser, create-database, create-role
or replication privileges. Grant `CONNECT` to the allowed database and
`pg_read_all_stats`; do not grant table writes or schema creation. Set
`default_transaction_read_only=on` on that role. `pg_read_all_stats` permits
cross-session statistics needed to count lock waits; this config never selects
query text from them. Use your secret manager to set the password. Monitor a
direct PostgreSQL endpoint, or an explicitly verified monitoring endpoint;
a PgBouncer transaction endpoint is not interchangeable with PostgreSQL's
statistics catalog.

The native Postgres scrape has a two-second total timeout and one connection
per pool. The static lock query also has a two-second collection timeout,
one open connection, a two-second connect timeout and a **one-second server
statement timeout**. Query samples, top-query events and query plans are all
explicitly disabled. There is no logs pipeline.

For Redis start with a reset ACL user, enable it with a secret password, and
allow only `INFO`, `PING`, `HELLO`, `CLIENT SETNAME` and `CLIENT SETINFO`.
Do not grant key/channel patterns, writes, `SCAN` or `CONFIG`. The native
client reads `INFO ALL`; disabled instruments and the final exact allow-list
admit only the five metrics below. Redis uses the native client's finite dial
and read timeouts; the scraper timeout is not a guarantee that a blocked native
INFO operation can be interrupted immediately.

## What the measurements mean

| Instrument | Native unit and interpretation |
| --- | --- |
| `postgresql.backends` | `1`, current backends for the selected DB; includes idle/non-client backends |
| `postgresql.connection.max` | `{connections}`, server-wide configured maximum |
| `postgresql.deadlocks` | `{deadlock}`, cumulative count for the selected DB |
| `altimeter.postgresql.locks` | `{lock}`, snapshot count of database-associated locks, including observer locks |
| `altimeter.postgresql.lock_waits` | `{connection}`, sessions in that DB with `wait_event_type='Lock'` |
| `redis.clients.connected`, `redis.clients.blocked` | `{client}`, current connected/blocked client snapshots |
| `redis.memory.used`, `redis.memory.rss` | `By`, allocator usage and OS-reported Redis RSS |
| `redis.keys.evicted` | `{key}`, cumulative evictions; not queue length |
| `ecs.task.cpu.usage.total` | `Nanoseconds`, accumulated CPU across the task's containers |
| `ecs.task.memory.usage` | `Bytes`, task container memory usage sum |
| `ecs.task.memory.utilized` | `MiB`, native per-container `(usage - cache) / 2^20`, summed after integer conversion |
| `ecs.task.memory.reserved` | `MiB`, task memory reservation |

The native ECS CPU instrument is a cumulative sum with its monotonic flag unset;
use changes over elapsed wall time for a rate and handle task restarts. Do not
read nanoseconds as seconds or MiB as bytes. Memory gauges are snapshots, never
sums across time. Task memory and process RSS measure different things.

Lock count is **not** blocked requests. The wait aggregate is contemporaneous
context beside a trace, never an exact request join. It selects no SQL, relation,
PID or user values. Relation/table/index metrics are disabled; deleting their
dimensions without aggregation would create colliding streams. The static SQL
aggregates locks and waiting sessions in the database before exporting one point
per instrument. No database-wide or Redis key count is called queue length.

Enable application process collection separately with `[process]` and
`ALTIMETER_INTEGRATIONS` including `process`. Its CPU seconds, RSS bytes, thread/FD
snapshots and GC counters retain each application's writer UUID and direct export
route; this collector never receives or duplicates them. Aggregate CPU rates
using an explicit wall-time/core denominator. Summed RSS can double-count shared
pages and is not physical container memory. A fresh writer is not crash or OOM
proof. Disable the process token and stop the collector independently.

## Bounds and failures

The memory limiter is first: 256 MiB hard / 192 MiB soft, checked each second.
It limits Go heap pressure, not total process RSS; the container limit covers
mapped executable/native memory too. Batches flush after one second or 64 points,
with a hard 128-point maximum. The in-memory export queue holds two requests,
with one consumer, no disk storage and no blocking on overflow. HTTP requests
have a two-second timeout; native retry backoff is bounded by five elapsed seconds.
A full queue or memory refusal loses optional observations; it cannot backpressure
an application, because application traffic never enters this process.

The regression suite decodes real OTLP from the pinned collector and real
Postgres/Redis. It checks exact metric/attribute allow-lists, SQL and credential
canaries, an actual row-lock wait beside a native driver span, task-only ECS
metadata units, source TLS/ACL denial, stopped collectors and direct application
process observations. Its stalled-export test checks bounded batches, retries,
RSS and clean bounded shutdown. Redis TLS in the tests terminates at a local TLS
forwarder connected to the real Redis server; no INFO values are fabricated.
