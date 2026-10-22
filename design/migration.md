# Engineer 004 - Migration, rollback, and delivery plan

Companion to `design/architecture_review.md`. Constraints [Observed, brief]: the SDK cannot
change, existing integrations cannot break, MVP in 3 months, full system in 6, two dedicated
senior engineers. Thresholds marked [Assumed] are starting values to be tuned on real traffic.

## 1. Keeping the collector endpoint compatible

- The SDK keeps calling the **same hostname and paths**. The new ALB takes over that hostname
  through weighted DNS records **once, for the tenant-agnostic proxy only** (P0); nothing
  changes on customer sites.
- **All per-tenant control after P0 lives in the collector, not DNS.** Weighted DNS splits by
  resolver, not tenant, and resolvers may cache past the TTL. The collector resolves write key
  -> tenant and reads per-tenant flags (`tee`, `ack_mode`, `read_source`,
  `personalization_source`) from a config table cached for <= 30 s [Assumed]. A global kill
  switch forces pure passthrough to legacy.
- Week 1 builds a **contract test suite from recorded production traffic**: every path,
  method, header, CORS preflight, status code, response body, cookie, and timeout the SDK
  relies on. The collector must reproduce them byte-for-byte where the SDK reads them.
- Endpoints other than event tracking (for example personalization fetches) are proxied to
  legacy unchanged until they are migrated individually.
- The write key -> tenant mapping is read from the legacy tenant table (or a synced copy),
  so tenant identity does not depend on the payload (fixture `evt-0011`).

## 2. Running old and new in parallel

| Phase | Who acknowledges the SDK | Kinesis write | Legacy forward | Customer-facing reads |
|---|---|---|---|---|
| P0 proxy | legacy (response passed through) | none | synchronous | legacy |
| P1 tee | legacy | best-effort, short timeout, loss counted | synchronous | legacy |
| P2 shadow | legacy | best-effort | synchronous | legacy; new system computes but shows nothing |
| P3 read cutover (per tenant) | legacy | best-effort | synchronous | new dashboards for enabled tenants |
| P4 ack cutover | **new** (after Kinesis accepts) | authoritative | asynchronous, best-effort, counted | new |
| P5 decommission | new | authoritative | stopped after 30 clean days | new |

Legacy stays the system of record for acknowledgments until P4, so a new-system defect
before P4 can never lose an event the SDK believes was delivered.

## 3. No duplicate personalization triggers

- Personalization is **pull-based state**: the existing personalization API answers the
  SDK's request from one source chosen by a per-tenant flag `personalization_source =
  legacy | new`. Only one source is ever customer-facing for a tenant.
- In P2-P3 the new segment evaluator runs in **shadow**: it writes trigger rows to a shadow
  table that is compared with legacy decisions and never served.
- At a tenant's cutover, **current legacy memberships are imported** as already-fired
  `(tenant, rule, person, window)` rows, so a visitor already in a segment is not
  "re-entered" by the new system. The unique constraint enforces it.
- Rollback flips the flag back; legacy never stopped computing.

## 4. Comparing counts and accuracy

Daily per tenant, by hour of `received_at`:

1. **Pipeline integrity (exact):** reconciliation invariants I1-I4 from the architecture
   review: archived = written, processed + quarantined + failed + suppressed = archived.
2. **New vs legacy (explained):** event counts by type, unique visitors, top pages, and
   segment memberships. Every difference is assigned to a category from the prototype's
   taxonomy - duplicate, suspected bot, quarantined (malformed / no tenant / conflict),
   clock-skew re-bucketing, legacy loss - with the residual reported separately.
3. **Event-level diff:** `event_id` sets in S3 raw vs the legacy store, which also feeds the
   loss investigation (section 5).
4. **Canary events:** synthetic events every 10 s through the real endpoint; 100% must
   appear in both systems with known values.

**"Accuracy verified" for a tenant** [Assumed thresholds] = 14 consecutive days with: zero
I3/I4 deficits; unexplained residual within +/-0.5% of daily events; segment membership
agreement >= 99% after excluding bot-flagged and skew-affected visitors; 100% canary delivery;
no quarantine item older than 48 h. New counts are expected to be *higher* than legacy
during peaks (legacy loses ~3% [Observed, brief]) - that is a finding, not a failure.

## 5. Finding the cause of the existing 3% loss

The P1 tee gives, for the first time, an independent record of everything that reached the
edge. Per hour and tenant:

| Bucket | How it is measured | Fixable by the new pipeline? |
|---|---|---|
| A. Rejected or timed out by legacy | proxy sees legacy's status codes and timeouts | yes |
| B. Accepted by legacy (2xx) but missing downstream | `event_id` in S3 raw, absent in legacy store | yes |
| C. Duplicates miscounted by legacy | same `event_id` counted twice in legacy | yes |
| D. Never reached the edge | not visible to the server; estimated from SDK-side signals if they exist (sequence numbers, session start vs page view ratios) and from client error rates | **no** - outside any server-side guarantee |

Correlate A-C with traffic rate to confirm the "during peaks" pattern. If D turns out to be
a large share, the 3% cannot be fully recovered without an SDK change; we report that
honestly rather than promise it.

## 6. Progressive enablement

1. **P0 proxy:** weighted DNS 1% -> 10% -> 50% -> 100% over ~1 week, contract tests green. The proxy is pure passthrough, so tenant-level routing is not needed yet.
2. **P1 tee:** enable Kinesis writes for all traffic (best effort); measure tee loss.
3. **P2 shadow:** processor, Valkey, dashboards on internal tenants only; 2+ weeks.
4. **P3 read cutover in waves:** internal -> 10 pilot tenants (MVP) -> 10% -> 50% -> 100%,
   each wave gated on "accuracy verified" for that wave's tenants plus 7 days without a
   rollback trigger.
5. **P4 ack cutover:** same tenant waves, switched by each tenant's `ack_mode` flag in the
   collector (not DNS).
6. **P5:** legacy forward stopped after 30 clean days at 100% P4.

## 7. Rollback triggers [Assumed thresholds]

Any one triggers rollback of the current phase or wave:

| Signal | Threshold |
|---|---|
| Collector 5xx (new-ack mode) | > 0.1% for 5 min |
| Collector p99 latency | > 500 ms for 10 min (or > legacy p99 + 100 ms) |
| Contract test failure in production canary | any |
| Reconciliation deficit (I3 or I4) | any, after settle |
| Kinesis write throttling with fallback unavailable | > 0.5% of records for 5 min |
| `IteratorAge` | > 5 min for 15 min during a wave |
| Canary end-to-end latency | p95 > 5 s for 15 min |
| Unexplained count residual vs legacy | > 2% for any tenant-day in the wave |
| Confirmed tenant-reported discrepancy | any |

## 8. Rollback without losing acknowledged events

- **P0-P2:** flip the global kill switch to pure passthrough (seconds, via the cached flag);
  DNS weight back to legacy is the last resort for a proxy-level fault and takes effect only as
  resolver caches expire. Until P4 every ack came from legacy, so nothing the SDK believes
  delivered depends on the new system.
- **P3:** flip the tenant's `read_source` / `personalization_source` flags back. Legacy never
  stopped receiving data.
- **P4:** flip the tenant's `ack_mode` back to legacy. Events acked in new-ack mode are durable
  in Kinesis/S3. Legacy received them asynchronously; any that legacy missed are found by the
  event-level diff and **backfilled into legacy** through its ingestion path, if it has one
  that accepts historical events (**open question** from legacy discovery). If it does not,
  those events remain available in S3 and the new system, and the gap is reported to the
  affected tenants.
- DNS changes take effect over the record TTL; both endpoints keep serving during the change,
  and each ack corresponds to a durable write in whichever system issued it.

## 9. Resuming after a rollback

1. Root cause written up; fix includes a regression test (a fixture-style case in the
   processor tests, as the prototype does).
2. Replay the affected window from S3 raw into the replay stream (batch path) and re-run the
   comparison for that window.
3. Re-enter at the **smallest cohort of the rolled-back wave**, not where it stopped.
4. Require 3 clean days before advancing again, then the normal gates.

## 10. Roadmap (two engineers: E1 ingestion/infra, E2 processing/data)

| Weeks | E1 | E2 | Exit criteria |
|---|---|---|---|
| 1 | SDK + legacy discovery (retry, `event_id` reuse, batching, endpoints, write keys); record traffic | IaC skeleton; port prototype detector into processor package | discovery doc; SQS fallback decision |
| 2-3 | collector proxy, contract tests, P0 rollout | Kinesis + Firehose raw; reconciliation counters | P0 at 100% |
| 4 | P1 tee; load test collector to 2x design peak (synthetic, staging) | loss investigation dashboards (section 5) | measured collector throughput replaces the 1,000 req/s assumption |
| 5-6 | ESM config, alarms, runbooks | processor: validate, normalize, dedupe, flag; Valkey Lua; benchmark Valkey | Valkey benchmark replaces assumptions |
| 7-8 | dashboard API (core metrics) | rollup flusher; quarantine tooling; P2 shadow | shadow comparison running |
| 9-10 | game days: Kinesis throttle, Valkey failover, poison record, collector crash | privacy request queue + manual runbook; canary SLO | runbooks exercised |
| 11-12 | **MVP: P3 for 10 pilot tenants** | accuracy reports for pilots | pilots "accuracy verified" |
| 13-16 | SQS fallback (if scenario B); P3 waves to 50% | curated staging + Iceberg MERGE; nightly exact recompute; identity graph | nightly correction live |
| 17-20 | P3 to 100%; P4 waves begin | deletion automation (human-approved); segments + personalization in shadow, then per tenant | first automated deletion audited end to end |
| 21-24 | P4 to 100% | warehouse export from Iceberg; fence rebuild tooling | **full system**; legacy decommission starts 30 clean days later (may land after month 6) |

### MVP (month 3) includes
Compatible collector, durable Kinesis ingest with reconciliation, raw archive, processor with
dedup / normalization / quarantine / flags, real-time dashboards for core predefined metrics
for pilot tenants, alarms, runbooks, manual privacy runbook.

### Explicitly excluded from the MVP
- Behavioral segments and personalization from the new pipeline (legacy keeps serving them)
- Warehouse export from the new pipeline (legacy keeps serving it)
- Curated Iceberg tables and nightly exact correction (month 4)
- Automated deletion execution (manual runbook until month 5)
- SQS fallback (month 4-5, only if the SDK does not retry)
- Ad hoc real-time queries, segment builder UI, per-tenant retention settings
- Person-level live unique counts, ML bot detection, multi-region, exactly-once

### What still needs a human throughout
Phase and wave go/no-go; rollback calls; privacy request verification and deletion approval;
tenant notification for exported copies; quarantine triage and new legacy-mapping approval;
identity conflict resolution; bot rule changes; reviewing unexplained reconciliation
residuals; cost anomaly review.
