# Engineer 004 - Architecture review (revision 3: final audit in section 9)

Brief version: 2026-07. Fixture SHA-256: `1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235`.

Revision 1 was the first architecture proposal (Kinesis + Lambda + Valkey/PostgreSQL + S3).
This review stress-tests it against delivery semantics, data loss, privacy, and metric
accuracy, then records every correction. Diagram: `design/architecture.mmd`. Numbers:
`artifact/sizing.py` -> `output/sizing_report.md`. Migration: `design/migration.md`.

Labels: **[Benchmarked]** = AWS / Valkey documentation or AWS Price List API (URLs in
section 7); **[Estimated]** = derived by `sizing.py` or by reasoning shown here;
**[Assumed]** = needs validation; **[Observed]** = brief or fixture. Nothing has been
load-tested.

---

## 1. Corrections log

| # | Revision 1 | Problem found | Revision 2 |
|---|---|---|---|
| C1 | Dedup `SET NX` "atomic with the counter update via Lua" | In Valkey cluster mode a multi-key operation only works when all keys hash to one slot [Benchmarked]. Dedup key + tenant counters + HLL + visitor state are different keys, so the script would fail or have to be split, and a split is not atomic: crash between steps = lost or double increment. | All keys of a tenant carry the hash tag `{t:<tenant>}`, so one Lua call per event updates marker and effects atomically [Benchmarked: scripts execute atomically]. Cost: one tenant lives on one shard (risk R6). |
| C2 | Dedup TTL 24 h, Lambda retry limits unspecified | Correctness of retries depends on the marker outliving every redelivery. With defaults, retries are unbounded and record age up to 7 days [Benchmarked], so markers could expire before a retry. | Lambda `MaximumRecordAgeInSeconds` = 6 h, `MaximumRetryAttempts` = 5, bisect on; dedup TTL = 8 h > 6 h [Assumed values]. Older or exhausted batches go to the S3 on-failure destination with payload [Benchmarked] and are covered by the batch path. |
| C3 | Replays "safe because processor is idempotent" | Replays older than the dedup TTL would re-increment live counters. Re-creating the event source mapping at `TRIM_HORIZON` would replay 7 days into live counters. | Replays and backfills never touch live counters: they feed a separate replay stream into the batch path (Iceberg MERGE + nightly recompute, which overwrite). ESMs are (re)created with `AT_TIMESTAMP` [Benchmarked: supported]. |
| C4 | Reconciliation `acknowledged == archived` | Wrong in three ways: records written whose HTTP ack never arrived are archived but not acked; SDK retries create extra archived copies; Firehose is at-least-once and lags by its buffer. | Per-record `ingest_id` plus five invariants with a settle window (section 3.4). Deficits page; surpluses open a ticket. |
| C5 | Curated data via Firehose -> Iceberg | Firehose Iceberg SKUs ($0.045 / $0.075 per GB) have undocumented applicability; Firehose duplicates would land in Iceberg. | Lambda sends packed records to Firehose Direct PUT -> S3 staging; hourly Athena `MERGE` on `(tenant_id, event_id)` into Iceberg. This deduplicates curated data by key and costs ~$42 + Athena per month [Estimated]. |
| C6 | Raw S3 retention 30 days | 30 days plus asynchronous lifecycle removal [Benchmarked: "there may be a delay"] can run past GDPR's one-month deadline. | Raw, quarantine, and failed-batch retention 14 days [Assumed]; expiry alone completes erasure in those stores. |
| C7 | Deletion registry only in PostgreSQL | A point-in-time restore of RDS recreates the database as of an earlier time [Benchmarked], losing registry rows written after it and resurrecting deleted users. | Registry mirrored to an append-only S3 deletion ledger; every restore or rebuild replays the ledger before serving traffic. |
| C8 | Segment state keyed by person, identity applied on write | Identify events can arrive after the events they explain (PutRecords does not preserve order [Benchmarked]); merging state on arrival is order-dependent. | State keyed by immutable `anonymous_id`; a person's rule count = sum over its linked anons at evaluation time. Order-independent. |
| C9 | Personalization trigger on threshold crossing | Retries and rebuilds can re-emit triggers; during migration both pipelines could trigger. | First crossing = `SET NX` on membership key + unique row `(tenant, rule, person, window)` in PostgreSQL; personalization is pull-based state with a per-tenant source flag (migration.md). |
| C10 | Rollup flusher "overwrite per closed minute" | A late increment between the flusher's read and write is lost until the next change. | Lua adds the minute to a dirty set; the flusher does SREM then GET then overwrite, so any increment after SREM re-marks the minute. Live path only increments minute counters for `event_time` within 10 min [Assumed], so an expired minute key is never recreated with a partial value. |
| C11 | Valkey failover "repairable" | Not specified how; a naive rebuild double counts events in flight. | Fence protocol (section 5.6). Valkey uses `noeviction` (default is `volatile-lru` [Benchmarked]) because evicting dedup markers causes double counts. No Valkey backups. |
| C12 | SQS fallback "phase 2, maybe" | Decision depended on an unknown. | Decision rule tied to SDK retry discovery in week 1 (section 3.2). |
| C13 | Tenant from payload | Fixture `evt-0011` has `tenant_id: null`; payload tenant is attacker-controlled. | Tenant from the authenticated write key; payload tenant must match or the event is quarantined. |
| C14 | Per-tenant metrics in CloudWatch implied | 500 tenants x 10 metrics = $1,500/month [Estimated]. | Per-tenant reconciliation in PostgreSQL; CloudWatch holds ~250 global metrics. |
| C15 | Cost ~$3,866/month | Inputs revised (see section 6). | **$3,036 normal / $3,163 short spike / $4,279 sustained high** [Estimated], new pipeline only (after audit 2). |
| C16 | Identity link from any event with both ids | Prototype defect found in the previous checkpoint: privacy events created links. | Links only from non-privacy SDK events, labeled "client-asserted": accepted for analytics and personalization, never for deletion authorization or data access. |

---

## 2. Event delivery and ordering

### 2.1 Idempotency keys

| Key | Scope | Who assigns | Used for |
|---|---|---|---|
| `ingest_id` (UUIDv7) | one collector write attempt of one record | collector | archive/processing reconciliation; Firehose duplicate removal |
| `(tenant_id, event_id)` | one logical event | SDK | business dedup (live marker, Iceberg MERGE) |
| fallback fingerprint `hash(tenant, anon, type, ts, properties)` | one logical event, 10 min window | processor | only if discovery shows the SDK mints a new `event_id` per retry [Assumed risk] |
| `(tenant, rule, person, window_start)` | one segment entry | segment evaluator | personalization trigger dedup |

Fixture evidence: `evt-0002` reappears with the same `event_id` and `ts` and a `received_at`
7.4 s later [Observed, synthetic], consistent with an SDK that reuses ids on retry; this
must be confirmed against the real SDK.

### 2.2 Scenario walk-through

| Scenario | What happens | Result |
|---|---|---|
| **Partial PutRecords success** | Response is HTTP 200 with `FailedRecordCount > 0` [Benchmarked]. Collector retries only failed entries (same `ingest_id`) with jittered backoff until the deadline. All records of a request accepted -> 2xx. Deadline hit -> 503 for the request; already-written records stay in Kinesis. | No ack without durability. Written-but-unacked records become duplicates if the SDK resends; removed by `(tenant, event_id)`. |
| **Write succeeded, HTTP response lost** | SDK sees a failure; if it retries, a second record with a new `ingest_id` and the same `event_id` is written. | Live marker or Iceberg MERGE drops the copy. Reconciliation counts it as written-unacked (I2). |
| **Lambda retry / bisect** | Records are redelivered (at-least-once [Benchmarked]). The Lua call finds the marker and returns "duplicate" without effects. Firehose staging writes may repeat; MERGE removes them. PostgreSQL writes are `ON CONFLICT DO NOTHING` or absolute overwrites. | Idempotent. |
| **Out-of-order identity events** | `identify` arrives after the anonymous events it explains, or before them. Visitor state is keyed by `anonymous_id`; the graph link is inserted whenever it arrives; evaluation sums over linked anons. | Same result in any order. Segment membership becomes true at the first evaluation after both pieces are present (eventual). |
| **Concurrent Valkey updates** | Lambdas on different shards update the same tenant's counters. Each update is one atomic Lua call [Benchmarked]; no read-modify-write in application code. | Atomic per event. |
| **Multiple consumers** | Firehose raw and the processor read independently (2 shared consumers; busiest shard 1.17 of 2 MiB/s [Estimated]). | Each consumer is at-least-once on its own; correctness never depends on two consumers agreeing in real time; reconciliation compares them after settle. |
| **Replay** | Separate replay stream, batch path only (C3). The processor checks the deletion registry on every record. | No live double counting; no resurrection of deleted subjects. |

### 2.3 What is atomic, what is eventually consistent

- **Atomic:** one event's dedup marker + all its Valkey effects (single Lua call, single slot);
  a PostgreSQL rollup overwrite; a trigger insert (unique constraint); an Iceberg MERGE commit.
- **Eventually consistent, unavoidably:** Valkey vs PostgreSQL rollups (flush interval);
  live vs nightly-corrected numbers; identity-dependent segments (until the link arrives);
  S3 raw vs Kinesis (Firehose buffer, <= 300 s [Assumed setting]); deletions (job completion);
  Valkey replicas (asynchronous replication; a failover "might" lose a small amount of data
  [Benchmarked]).
- **Not claimed:** end-to-end exactly-once. The system is at-least-once delivery with
  idempotent effects, plus a nightly exact recompute for closed periods.

---

## 3. Zero data loss

### 3.1 The guarantee

> Every event whose request received a 2xx from the collector has been accepted by Kinesis
> (or, if built, SQS). Within the stream retention window (7 days) it will be archived to S3
> raw, and it will be processed, quarantined, written to the failed-batch bucket with its
> payload, or suppressed by an approved deletion. Each outcome is countable (section 3.4).

Kinesis "synchronously replicates data across three Availability Zones" [Benchmarked: FAQ];
the docs do not explicitly bind that to the PutRecords response, so treating a successful
put as the durability point is a documented assumption, not a contractual guarantee.

**Outside the guarantee (unresolved, cannot be fixed without changing the SDK):** events never
acknowledged - browser closed, network failure, DNS/ALB failure before the collector,
`sendBeacon`-style fire-and-forget, and any 503 the SDK does not retry.

### 3.2 SDK scenarios

| | **A. SDK retries failed requests** | **B. SDK does not retry** |
|---|---|---|
| Kinesis throttled / unavailable < SDK retry budget | 503 -> SDK retries -> no loss | event lost on the first 503 |
| Outage > SDK retry budget | loss after the budget (unacked) | loss |
| Collector crash mid-request | retried -> possible duplicate | lost |
| Value of an SQS fallback | small: only outages longer than the retry budget | high: turns Kinesis failures into acks |
| Decision | Defer fallback; revisit if Kinesis outages exceed the SDK budget in practice | **Build in month 4-5** |

How we find out (week 1): read the SDK source; replay recorded traffic against a collector
that returns 503 and watch for resends; check whether resends reuse `event_id`.

**Fallback design (scenario B only).** After the Kinesis deadline the collector writes the
request's records to SQS with `SendMessageBatch`, then acks. A drainer re-puts them to Kinesis
with the original `ingest_id`. Both writes may have succeeded, so duplicates are possible and
are removed downstream. Drained events arrive late, so they reach dashboards through the
batch correction. Cost: ~$5 per hour of outage at the design peak [Estimated]. Limits: does
nothing for failures before the collector; a regional event affecting both services still
returns 503. **No local disk buffering:** Fargate task storage is not durable, so acking from
it would make the ack meaningless.

### 3.3 Behaviour when Kinesis is unavailable

1. Failed entries retried with jittered backoff inside a ~1-2 s request deadline [Assumed].
2. Then: scenario A -> 503 + `Retry-After`; scenario B with fallback -> SQS, then 2xx.
3. Processing and dashboards keep serving existing state, marked stale.
4. On recovery: drain SQS, watch `IteratorAge` fall, reconcile the outage hours.

### 3.4 Reconciliation (corrected)

Counted per `(tenant, received hour)`. The collector flushes per-tenant counters to PostgreSQL
every 10 s; Athena computes archive and processing sets.

| Invariant | Definition | Exact? | On violation |
|---|---|---|---|
| I1 accounting | `events_in = acked + rejected_4xx + failed_5xx` per collector | exact | bug ticket |
| I2 ack implies write | `acked <= written` where `written` = distinct `ingest_id` accepted by Kinesis or SQS; the gap = written-unacked | exact inequality | gap trend only (expected under retries) |
| I3 archive completeness | after settle (Firehose buffer + 15 min, checked at +1 h): `distinct ingest_id in S3 raw = written` | exact | **deficit pages** (possible loss); surplus = collector counter lost on crash -> ticket |
| I4 processing completeness | `archived ingest_ids = processed + quarantined + failed_batch + suppressed` (set equality by join) | exact | page; reprocess missing ids from S3 raw |
| I5 business | `unique_events = distinct (tenant, event_id)` in curated; `duplicates = processed - unique` | exact after MERGE | none - reported |

### 3.5 Monitoring, alarms, recovery

| Signal | Threshold [Assumed] | Action |
|---|---|---|
| Collector 5xx rate | > 0.1% for 5 min | page; check Kinesis throttling / task health |
| PutRecords failed-entry rate | > 1% for 5 min | page; add shards (`UpdateShardCount`, <= 2x per call, 10 per 24 h [Benchmarked]) |
| SQS fallback writes | > 0 | page; Kinesis incident runbook |
| Kinesis `WriteProvisionedThroughputExceeded` | any sustained | add shards; check hot visitor limits |
| Lambda `IteratorAge` | > 30 s warn, > 5 min page | raise `ParallelizationFactor` (max 10 [Benchmarked]); check errors |
| Objects in failed-batch bucket | > 0 | page; inspect, fix, re-inject from S3 |
| Firehose `DataFreshness` | > 15 min | page (Firehose retries from KDS only until stream retention [Benchmarked]) |
| Reconciliation I3/I4 deficit | > 0 after settle | page; locate hour/tenant; re-archive from Kinesis within 7 days |
| Valkey evictions / memory | evictions > 0, memory > 75% | page; scale shards |
| Valkey failover event | any | start the fence rebuild (5.6) |
| Canary end-to-end latency | p95 > 5 s for 15 min | page |
| Quarantine age / privacy request age | > 48 h / > 7 days | ticket / page owner |

---

## 4. Privacy and deletion

### 4.1 Authorization

Deletion is authorized only through an authenticated tenant channel (tenant admin API or
support process). SDK `privacy_request` events are queued for verification; they never
execute anything and never create identity links (prototype finding `evt-0017`:
`unverified_identity_claim`, candidate scope `evt-0006`, `evt-0017`). A human approves the
final scope.

### 4.2 Store-by-store audit

| Store | What it holds | Erasure method | Physically gone when |
|---|---|---|---|
| Valkey | visitor rule buckets, identity cache, dedup markers, HLLs | `DEL` per-anon and per-person keys | immediately on the primary; markers expire in 8 h. HLL registers cannot be selectively removed (no removal command; HLL keeps no elements [Benchmarked]); treated as non-identifying aggregates - **legal review needed** |
| Valkey snapshots | - | none taken (derived state) | n/a |
| PostgreSQL | identity graph, segment membership, trigger rows, quarantine index | `DELETE`; registry row + audit row kept with hashed identifiers | immediately in the live DB |
| RDS backups | all of the above, historical | retention 7 days [Assumed; range 0-35 Benchmarked] | when backups age out; any restore replays the S3 deletion ledger first (C7) |
| Kinesis | in-flight records | cannot delete records; processor suppresses via registry | 7 days |
| S3 raw, quarantine, failed batches | raw payloads | lifecycle expiry 14 days; bucket versioning off, or `NoncurrentVersionExpiration` [Benchmarked] | 14 days + asynchronous lifecycle delay [Benchmarked] |
| Iceberg curated | normalized events | `DELETE` (writes position-delete files, merge-on-read [Benchmarked]) -> `OPTIMIZE REWRITE DATA` -> `VACUUM` | after VACUUM removes snapshots older than `vacuum_max_snapshot_age_seconds` (default 5 days [Benchmarked]; set to 3 days [Assumed]) |
| S3 staging | packed normalized events | lifecycle 2 days | 2 days |
| Warehouse exports | tenant's copies in Snowflake/BigQuery | tenant notified with subject identifiers; future exports exclude | under the tenant's control (they are the controller of exported copies) - **contractual** |
| CloudWatch logs | should hold no payloads | log policy forbids payload logging; 14-day retention | 14 days |
| Replay / restore | any source above | processor drops records matching the registry; batch jobs read already-deleted Iceberg | n/a |

**Why `DELETE` alone is not erasure.** Athena's Iceberg `DELETE` writes delete files that hide
rows at read time; the original Parquet files still contain the rows, and older snapshots
still reference them (time travel can read them). `OPTIMIZE REWRITE DATA` writes new files
without the rows; `VACUUM` then expires old snapshots and removes unreferenced files - and
only if the role has `s3:DeleteObject`, otherwise "the query will succeed, but the files will
not be deleted" [Benchmarked]. Deletions are batched weekly per tenant so compaction cost
stays bounded.

**Deadlines.** GDPR: "within one month", extendable by two months [Benchmarked; unofficial
copy of Art. 12(3)]. CCPA: 45 days, extendable to 90 [Benchmarked: CA Attorney General].
Internal target 7 days to approval-complete, 30 days to physical erasure everywhere we control.

**Preventing resurrection.** Registry entry `(tenant, subject ids incl. linked anons,
executed_at)` in PostgreSQL and in the S3 ledger. Processor and batch jobs drop records of a
registered subject with `received_at <= executed_at`. Whether *new* events after deletion are
collected is a tenant policy decision (default: processed normally) - **open question**.

---

## 5. Real-time accuracy

### 5.1 Live update (one Lua call per event, all keys `{t:<tenant>}`)

```
marker exists?          -> same hash: return DUPLICATE; different hash: return CONFLICT (quarantine)
received_at < fence?    -> return SKIP_LIVE (rebuild covers it)          (5.6)
SET marker hash EX 8h
if event_time within 10 min: HINCRBY minute_counters[minute] <metric or metric:suspected_bot>; SADD dirty minute
PFADD hll[hour] anonymous_id                                     (idempotent by nature)
for each matching segment rule (server-side predicate, not bot-flagged):
    HINCRBY visitor_rules[anon] "<rule>:<day>" 1
return APPLIED
```

### 5.2 Metric semantics

| Metric | Live (seconds) | Corrected (nightly, closed days) | Class |
|---|---|---|---|
| Pageviews / event counts | minute counters in Valkey, deduped by marker | `COUNT(*)` over MERGE-deduplicated Iceberg, overwrite hourly/daily rows | live **approximate** (failover tail loss, burst-onset bot misclassification, >10 min late events excluded); daily **exact** under the stated rules |
| Unique visitors (per anon) | HLL, standard error 0.81% [Benchmarked] | `COUNT(DISTINCT anonymous_id)` | live **approximate**; daily **exact** |
| Unique persons | not live in MVP | `COUNT(DISTINCT coalesce(person, anon))` via graph | **eventually correct**; deferred live |
| Rolling segments | sum of daily buckets over the window | recomputed weekly or on rebuild | **eventually consistent**; window granularity 1 day |
| "Viewed pricing 3 times in 7 days" | predicate `type = page_view AND path matches /pricing`; sum of last 7 day buckets across the person's anons >= 3 | same | client `count_today` ignored (fixture `evt-0019`: client says 3, server sees 1) |
| Personalization trigger | first crossing: `SET NX` membership + unique trigger row; served by pull | n/a | at most one trigger row per (tenant, rule, person, window); evaluation at-least-once |

### 5.3 Handling the fixture's anomaly classes

| Anomaly (fixture) | Live path | Corrected path |
|---|---|---|
| Duplicate (`evt-0002`) | marker returns DUPLICATE | MERGE on key |
| Suspected bot (`evt-0012`-`0015`) | referrer rule flags immediately; burst rule flags from the 4th event; flagged events go to `metric:suspected_bot` and never into segment buckets | burst onset events reclassified |
| Delayed events | minute counters only within 10 min; segment buckets still updated within the window | included by event day |
| Clock skew (`evt-0005`, `0006`, `0016`) | `event_time = received_at`, raw `ts` kept | same rule |
| Identity (`evt-0003`, `0008`, `0022`) | link inserted whenever it arrives; evaluation sums anons | graph join |
| Unverified identity claim (`evt-0017`) | not linked | not linked unless verified |
| Replay | never live | batch only |

### 5.4 If Valkey loses derived state

- **Failover (async replication):** the tail of recent Lua calls may be lost together (marker
  and effects are applied as one unit). Effect: small undercount in live counters and
  segment buckets for seconds before the failover; a Lambda batch retried across the
  failover could double count only if its markers were lost - corrected nightly.
- **Total loss:** dashboards fall back to PostgreSQL rollups (minute granularity up to the
  last flush); segments answer "unknown" (personalization uses its default experience)
  until rebuild.

### 5.5 Personalization guard rails

Never trigger on bot-flagged events, events with `received_at` older than 60 s [Assumed], or
unverified identity. Triggers are state served on request, so a duplicate evaluation cannot
show a visitor two different experiences.

### 5.6 Fence rebuild protocol

On a failover or loss at time `F`: (1) bump the tenant's key generation `g -> g+1` and write
`fence = F` (both in the tenant's config key; every live key name includes the generation, so
generation `g+1` starts empty - surviving pre-failover state in `g` cannot be counted twice);
(2) the live path returns SKIP_LIVE for events with `received_at < F` and applies newer
events into `g+1`; (3) once curated data is complete past `F` (MERGE watermark, ~1-2 h), the
rebuild job recomputes counters, HLLs, and 7 days of rule buckets for `received_at < F` and
adds them to `g+1` with `HINCRBY`/`PFADD`. The two halves are disjoint by `received_at`, so
nothing is counted twice; (4) clear the fence and drop generation `g`. Until step 3 finishes,
dashboards read PostgreSQL rollups for the gap and segment answers are "unknown"
(personalization serves its default experience).

---

## 6. Capacity and cost summary

From `output/sizing_report.md` (all [Estimated]; inputs labeled there):

| | Value |
|---|---|
| Average / peak hour / design peak | 579 / 1,157 / 12,000 events/s |
| Kinesis shards (1 KiB events, 50% target) | 24 (records and bytes both need 12 at 100%) |
| Busiest shard with 1.2 skew | 600 rec/s of 1,000; 1.17 of 2 MiB/s shared read |
| Per-event processing budget at design peak | 2 ms (PF 1), 20 ms (PF 10) |
| Valkey memory, normal / sustained | 4.7 / 17.3 GiB -> 3 shards x r7g.large + replicas |
| Latency L1 (collector receipt -> dashboard), typical / worst non-retry | 2.18 s / 3.35 s vs 5 s target (L2 from SDK creation adds SDK buffering + network: unknown) |
| **Monthly cost** | **$3,036 normal, $3,163 short spike, $4,279 sustained high** |
| Not included | legacy pipeline during parallel run (unknown), WAF (optional, ~$927), support plan |

**Why it differs from the earlier ~$3,866:** RDS r7g.xlarge -> r7g.large (the rollup write
rate does not need the larger instance) -$418; Valkey 4 x r7g.xlarge -> 6 x r7g.large -$254;
curated Firehose packed instead of one event per record -$176; ALB LCUs computed from the
average rate rather than the peak-hour rate -$135. Lines that were missing or under-counted
add ~$380: VPC endpoints for 9 services (+$145 over the earlier line), CloudWatch metrics and
alarms, KMS, CloudTrail, NAT, response egress, Athena, Glue, Secrets Manager. Partly offset by
Lambda modeled per invocation instead of a flat 0.2 s (-$130), lower cross-AZ (-$61), and
14-day raw retention in S3 (-$31).

Budget is not the binding constraint (~6-9% of the ceiling). Team time is.

---

## 7. Evidence classification

### 7.1 Verified capabilities [Benchmarked]

| Capability | Source |
|---|---|
| Shard limits 1 MB/s or 1,000 rec/s write, 2 MB/s and 5 reads/s shared read | https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html |
| PutRecords partial failure (`FailedRecordCount`), no ordering guarantee | https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html |
| On-demand throttles above 2x prior peak within 15 min | https://docs.aws.amazon.com/streams/latest/dev/how-do-i-size-a-stream.html |
| UpdateShardCount: <= 2x per call, 10 per rolling 24 h | https://docs.aws.amazon.com/kinesis/latest/APIReference/API_UpdateShardCount.html |
| Retention 24 h default up to 365 days, extra charge above 24 h | https://docs.aws.amazon.com/streams/latest/dev/kinesis-extended-retention.html |
| Enhanced fan-out: 2 MB/s per consumer per shard, up to 20 consumers | https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html |
| Lambda ESM: 1 poll/s/shard, at-least-once, PF max 10, bisect, retry/age limits, `AT_TIMESTAMP` | https://docs.aws.amazon.com/lambda/latest/dg/with-kinesis.html, https://docs.aws.amazon.com/lambda/latest/dg/services-kinesis-parameters.html |
| On-failure destination: records discarded; S3 destination includes payload | https://docs.aws.amazon.com/lambda/latest/dg/kinesis-on-failure-destination.html |
| Firehose at-least-once; KDS source retries until stream retention | https://docs.aws.amazon.com/firehose/latest/dev/basic-deliver.html, https://docs.aws.amazon.com/firehose/latest/dev/retry.html |
| Valkey scripts atomic; multi-key ops need one slot; hash tags | https://valkey.io/topics/eval-intro/, https://valkey.io/topics/cluster-tutorial/ |
| HLL 0.81% standard error | https://valkey.io/topics/hyperloglogs/ |
| ElastiCache async replication, failover may lose data; `noeviction` settable, default `volatile-lru`; 25% reserved memory | https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/AutoFailover.html, .../ParameterGroups.Engine.html, .../redis-memory-management.html |
| Athena Iceberg DELETE (position deletes), OPTIMIZE, VACUUM; DELETE/OPTIMIZE billed by scan | https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-delete.html, .../vacuum-statement.html, .../optimize-statement.html |
| S3 lifecycle removal is asynchronous; versioned buckets need NoncurrentVersionExpiration | https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-expire-general-considerations.html |
| RDS backup retention 0-35 days; free backup storage up to provisioned storage; PITR | https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithAutomatedBackups.BackupRetention.html, https://aws.amazon.com/rds/faqs/ |
| Timestream for LiveAnalytics closed to new customers (2025-06-20) | https://docs.aws.amazon.com/timestream/latest/developerguide/AmazonTimestreamForLiveAnalytics-availability-change.html |
| All prices | AWS Price List API, us-east-1, retrieved 2026-10-22 (appendix of sizing_report.md) |

### 7.2 Calculated estimates
Everything in section 6 and `output/sizing_report.md`: throughput, shards, memory, storage,
latency budget, cost. Derived from labeled inputs; **no measurements**.

### 7.3 Assumptions requiring validation

| Assumption | Value | How to validate |
|---|---|---|
| SDK retries; reuses `event_id`; events per request | unknown | SDK source + recorded traffic (week 1) |
| Average event size | 1 KiB | sample production traffic (week 1) |
| Collector throughput | 1,000 req/s per 1-vCPU task | load test (month 1) |
| Per-event processing cost; Valkey commands/s per shard | 0.5 ms; 96k/s total | benchmark (month 2) |
| Dedup key size | 90 B | `MEMORY USAGE` on staging data |
| Peak-hour factor; spike shape | 2x; 10x of peak hour | production metrics from legacy |
| Hash skew; largest tenant share | 1.2; unknown | key distribution from recorded traffic |
| Thresholds (lateness 10 min, dedup 8 h, max record age 6 h, alarms) | as stated | game days |
| HLL and rollups are not personal data | - | legal review |
| Tenant identified by write key | - | legacy discovery |

### 7.4 Known limitations

- No end-to-end exactly-once; live numbers are approximate and corrected nightly.
- Events lost before the collector acknowledges are outside any guarantee we can give.
- One tenant maps to one Valkey slot (C1): a very large tenant concentrates load on one shard.
- Segment windows are day-granular; person-level unique counts are not live.
- Predefined dashboard metrics only; ad hoc queries run on Athena in seconds to minutes.
- Exported warehouse copies are erased by the tenant, not by us.
- Single region.

### 7.5 Decisions intentionally deferred

| Decision | Trigger to revisit |
|---|---|
| SQS fallback | SDK discovery shows no retry (scenario B) |
| Real-time analytics database (OpenSearch / ClickHouse) for ad hoc queries | customer demand after month 6 |
| Flink for true event-time windows | lateness corrections become a product problem |
| Splitting large tenants across Valkey slots (`{t:<tenant>:<n>}` + merge on read) | one shard > 50% CPU in benchmarks |
| Live person-level uniques | product requirement |
| Multi-region / EU residency | legal or customer requirement |
| WAF | abuse observed at the collector |
| Kinesis On-demand Advantage vs provisioned | traffic grows past ~25 MiB/s |

---

## 8. Risk register

| # | Risk | Likelihood / impact [Estimated] | Mitigation |
|---|---|---|---|
| R1 | SDK does not retry, or mints new ids per retry | medium / high | week-1 discovery; SQS fallback; fingerprint dedup |
| R2 | Proxy insertion breaks an existing integration | medium / high | transparent proxy, contract tests from recorded traffic, DNS canary (migration.md) |
| R3 | Valkey throughput or memory below estimate | medium / medium | benchmark month 2; shards scale online; derived state only |
| R4 | Deletion incomplete in one store | low / high | store-by-store checklist, ledger replay on restore, audit trail, quarterly test deletion |
| R5 | Spike beyond design peak or hot visitor | medium / medium | 50% headroom, per-visitor and per-tenant limits, scheduled pre-scaling, resharding runbook |
| R6 | Large tenant hot Valkey slot | low-medium / medium | sub-slot split (section 9, C19) |
| R7 | Two engineers, six months, legacy unknowns | high / medium | phased, reversible plan; scope cuts listed in migration.md |

---

## 9. Final audit corrections (audit 2)

| # | Concern | Finding | Correction |
|---|---|---|---|
| C17 | Lambda batching | The `CreateEventSourceMapping` API reference states: for Kinesis, DynamoDB and SQS, "when you set `BatchSize` to a value greater than 10, you must set `MaximumBatchingWindowInSeconds` to at least 1" [Benchmarked: https://docs.aws.amazon.com/lambda/latest/api/API_CreateEventSourceMapping.html]. Revision 2 implied large batches with a 0 s window - invalid. (The Lambda developer guide lists Kinesis defaults of BatchSize 100 with a 0 s window, which contradicts the API reference; we follow the stricter rule.) | ESM: `BatchSize` 500, `MaximumBatchingWindowInSeconds` 1, `ParallelizationFactor` 1 (raise to 10 under backlog), bisect on, `MaximumRetryAttempts` 5, `MaximumRecordAgeInSeconds` 21600, S3 on-failure destination. L1 latency re-estimated at **2.18 s typical / 3.35 s worst** [Estimated]; invocation count re-modeled per window cycle (Lambda line $46 normal). Alternative if headroom matters more than cost: `BatchSize` 10 with a 0 s window and `ParallelizationFactor` up to 10. |
| C18 | Per-tenant rollout via DNS | Weighted DNS splits by resolver, not by tenant, and resolvers may cache beyond the TTL. | DNS weights only introduce the tenant-agnostic proxy (P0). Every later step is a per-tenant flag in the collector's tenant config (write key -> tenant -> `tee`, `ack_mode`, `read_source`, `personalization_source`), cached for <= 30 s [Assumed], plus a global kill switch that forces pure passthrough. Rollback = flag flip, not DNS (migration.md). |
| C19 | Valkey hot tenant | Acknowledged limitation of C1: a tenant is pinned to one slot, so one shard caps that tenant's live throughput (capacity unbenchmarked). | Split procedure for a tenant above a measured per-shard ceiling: hash tag `{t:<tenant>:<k>}` with `k = hash(anonymous_id) mod N`. A visitor's marker and effects stay in one sub-slot, so per-event atomicity holds. Tenant counters are summed over N keys on read; HLLs stay valid because sub-slots hold disjoint visitors, so per-sub-slot `PFCOUNT`s add up. Person-level segment evaluation reads across sub-slots (read-only, eventually consistent). The move uses the generation bump + rebuild protocol (5.6). Not built until a benchmark or production metric requires it. |
| C20 | Latency definition | Earlier text gave one number without saying where the clock starts. | L1 (`received_at` -> dashboard) is what the pipeline controls; L2 (SDK creation -> dashboard) adds SDK buffering, network, and retries, unknown until the SDK is measured. Both are estimates; neither is benchmarked. |
| C21 | Privacy coverage gaps | The store audit omitted the SQS fallback queue, Firehose error output, and the Athena query-results bucket, all of which can hold payloads. | SQS message retention set explicitly to <= 4 days [Assumed]; Firehose error prefix and Athena results bucket on 7-day lifecycle [Assumed]; all three added to the deletion checklist. Lifecycle removal is asynchronous, so erasure times are "after expiry, plus S3's removal delay", never an exact time. |
| C22 | Failed-batch recovery | Durable retention and alerting existed; the return-to-processing procedure was not written down. | Runbook below. |
| C23 | Migration overlap cost | Not separated from steady state. | `sizing_report.md` section 10: proxy CPU and legacy forwarding shown separately; legacy pipeline cost marked NOT ESTIMATED and never added to the new-pipeline totals. |

### Runbook: permanently failed Lambda batches

1. **Retained:** discarded batches are written by the ESM's S3 on-failure destination with the
   full invocation record [Benchmarked]; independently, every record is already in S3 raw
   (Firehose), so a failure to write the failed-batch object is still caught by
   reconciliation invariant I4 (archived ids not processed).
2. **Alerted:** any new object in the failed-batch bucket pages; I4 deficits page.
3. **Triage (same day):** classify - code bug, poison record, dependency outage.
4. **Fix:** deploy the fix, or add a quarantine rule for the poison shape.
5. **Return to processing:** a re-inject job reads the failed-batch objects (or the missing
   `ingest_id`s from S3 raw) and writes them to the **replay stream** (batch path: Iceberg MERGE,
   then the nightly recompute). Re-injected events never touch live counters, so a partial
   earlier attempt cannot double count. The deletion registry is applied on the way.
6. **Close:** I4 shows zero missing ids for the affected hours; delete the failed-batch objects
   (or let the 14-day lifecycle expire them).

### SDK compatibility assumptions (all unverified until week-1 discovery)

| Area | Assumption in this design | If wrong |
|---|---|---|
| Authentication | a write key in each request identifies the tenant | collector needs another tenant signal (origin domain, path); evt-0011-style events grow |
| Event identifiers | SDK sets `event_id` and reuses it on retry (fixture `evt-0002` suggests so) | fingerprint dedup in a 10-minute window; exactness drops |
| Batching | one event per request (worst case for ALB/collector) | multi-event requests reduce LCU cost; partial PutRecords handling becomes per-request |
| Request/response | SDK ignores response bodies beyond status; CORS behavior known | contract tests must reproduce exact bodies/headers |
| Retries | unknown | scenario B: SQS fallback; unacked failures remain unrecoverable |
| Transport | XHR/fetch (not `sendBeacon`) | `sendBeacon` cannot see responses, so it never retries: scenario B by construction |

---

## 10. Detailed evidence log and process findings

Moved here from `SUBMISSION.md` so the submission fits four pages. Tiers follow SCORING.md.

| Claim | Proof | Tier |
|---|---|---|
| Detector processes all 25 fixture lines and flags the anomaly classes in SUBMISSION section 9 | `../output/anomaly_report.json`, `../output/run_log.txt`; rerun `python3 artifact/detect_anomalies.py fixtures/event_sample.jsonl` | Tier 3 (execution output). Whether it matches the private answer key is unknown. |
| 49 unit tests pass | `../output/run_log.txt`; rerun `(cd artifact && python3 -m unittest -v test_detect_anomalies)` | Tier 3 |
| Prototype defects found and fixed | `../artifact/CHANGELOG.md`; regression tests in `../artifact/test_detect_anomalies.py` | Tier 2. The revert checks (each fix undone in a scratch copy, tests confirmed to fail) ran during development; their output was not kept, so that detail is Tier 0. |
| Capacity, shard count, Valkey memory | `../artifact/sizing.py`, `../output/sizing_report.md` | Tier 2 (calculation); not measured |
| Monthly cost | same; every price cites its AWS Price List API service code | Tier 3 (cost calculation with sources shown) |
| Latency L1 2.18 s typical / 3.35 s worst [Estimated] | `../output/sizing_report.md` section 7 | Tier 2 (estimate). No measurement. |
| Zero-loss guarantee, migration, privacy design | this document, `migration.md` | Tier 2 (design documents) |
| AWS limits and behaviors | links in section 7.1 | Cited documentation, not tested by us |
| Before/after data from a comparable system | none | Not claimed (no Tier 4) |

### Problems found and fixed during the work

Found by targeted probing of the prototype, revert checks of the tests, and review against
AWS documentation. No other experiments were run.

1. **Untrusted privacy events created identity links.** A forged `privacy_request` could merge
   another visitor's history into a user. It is now recorded as `unverified_identity_claim` and
   kept out of the identity graph (fixture `evt-0017`).
2. **Phone-number detection** flagged ISO dates such as `2026-06-15` as phone numbers and
   labeled them deterministic. Dates are now excluded; phone-shaped values alone are heuristic.
3. **Timestamp provenance:** corrections re-serialized the original value and did not record
   which time was used. Findings now carry `original_ts`, `event_time_used`,
   `event_time_source`, `correction_reason`.
4. **Two weak tests** still passed with tenant isolation removed; both were strengthened.
5. **Architecture review:** a multi-key Lua call across Valkey slots is not possible (fixed with
   hash tags, C1); the dedup marker could expire before Lambda retries (C2); replays would
   re-increment live counters (C3); the reconciliation equation was wrong (C4); restoring RDS
   would resurrect deleted users (C7); a 0 s batching window is invalid with `BatchSize` 500
   (C17); DNS weights cannot route per tenant (C18).
