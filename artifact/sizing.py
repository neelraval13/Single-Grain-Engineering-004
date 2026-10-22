#!/usr/bin/env python3
"""Capacity, latency, and monthly cost model for the Engineer 004 pipeline design.

Standard library only; deterministic; nothing here is a measurement.

    python3 artifact/sizing.py                                # print the Markdown report
    python3 artifact/sizing.py --out output/sizing_report.md  # from the repository root

Every input is an Input(value, label, source). Labels follow SCORING.md:
  Observed    - stated in the brief or measured in the fixture
  Benchmarked - taken from a named external source (AWS docs / AWS Price List API)
  Assumed     - a placeholder chosen to make the design concrete; must be validated
  Estimated   - derived by this script from the inputs above (all outputs are Estimated)

Region: us-east-1 throughout. Prices are on-demand list prices from the AWS Price List
API, retrieved 2026-10-22, file per service at
  https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/<ServiceCode>/current/us-east-1/index.json
"""
from __future__ import annotations

import argparse
import math
from collections import namedtuple
from pathlib import Path

Input = namedtuple("Input", "value label source")

PRICE_API = ("https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/"
             "{}/current/us-east-1/index.json")
RETRIEVED = "2026-10-22"
HOURS_PER_MONTH = 730                    # AWS billing convention
KIB, GIB = 1024, 1024 ** 3


def price(value, code, item):
    return Input(value, "Benchmarked", f"AWS Price List API `{code}`: {item}")


def doc(value, url, note):
    return Input(value, "Benchmarked", f"{note} ({url})")


# --- Traffic ------------------------------------------------------------------------
T = {
    "events_per_day": Input(50e6, "Observed", "brief: ~50M events/day"),
    "spike_multiplier": Input(10, "Observed", "brief: 10x traffic spikes"),
    "peak_hour_factor": Input(2.0, "Assumed", "diurnal peak hour vs daily average"),
    "event_kib": Input(1.0, "Assumed", "average serialized event incl. UA/URL/context"),
    "fixture_event_bytes": Input(230, "Observed",
                                 "fixture: 5,749 bytes / 25 lines (synthetic, minimal fields)"),
    "events_per_request": Input(1.0, "Assumed", "worst case: SDK sends one event per request"),
    "new_conn_fraction": Input(1.0, "Assumed",
                               "worst case: every request opens a new connection"),
    "http_overhead_kib": Input(0.5, "Assumed", "request headers/TLS framing per request"),
    "response_kib": Input(0.3, "Assumed", "collector response size"),
    "event_sizes_kib": Input([0.5, 1.0, 2.0, 4.0], "Assumed", "sensitivity range"),
}

# --- Design parameters --------------------------------------------------------------
D = {
    "shard_write_mib_s": doc(1.0, "https://docs.aws.amazon.com/streams/latest/dev/"
                             "service-sizes-and-limits.html", "per-shard write 1 MB/s"),
    "shard_write_rec_s": doc(1000, "https://docs.aws.amazon.com/streams/latest/dev/"
                             "service-sizes-and-limits.html", "per-shard write 1,000 rec/s"),
    "shard_read_mib_s": doc(2.0, "https://docs.aws.amazon.com/streams/latest/dev/"
                            "service-sizes-and-limits.html",
                            "per-shard shared read 2 MB/s, 5 GetRecords/s"),
    "shard_target_util": Input(0.5, "Assumed", "headroom for skew and unplanned spikes"),
    "hash_skew": Input(1.2, "Assumed", "busiest shard vs mean with tenant:anon keys"),
    "per_visitor_limit": Input(50, "Assumed", "collector rate limit per anonymous_id, ev/s"),
    "shared_consumers": Input(2, "Assumed", "Firehose raw + processor Lambda"),
    "collector_rps_per_task": Input(1000, "Assumed",
                                    "1 vCPU Node.js task; MUST be load-tested"),
    "collector_target_util": Input(0.6, "Assumed", "autoscaling target"),
    "collector_min_tasks": Input(6, "Assumed", "2 per AZ x 3 AZ"),
    "collector_vcpu": Input(1, "Assumed", "task size"),
    "collector_gb": Input(2, "Assumed", "task size"),
    "lambda_mem_gb": Input(1.0, "Assumed", "processor memory"),
    "lambda_overhead_s": Input(0.030, "Assumed", "per-invocation fixed cost"),
    "lambda_per_event_s": Input(0.0005, "Assumed", "per-event CPU + amortized Valkey RTT"),
    "lambda_poll_per_s": doc(1, "https://docs.aws.amazon.com/lambda/latest/dg/"
                             "with-kinesis.html", "ESM polls each shard once per second"),
    "lambda_batch_size": Input(500, "Assumed", "ESM BatchSize (max 10,000)"),
    "lambda_batch_window_s": doc(1, "https://docs.aws.amazon.com/lambda/latest/api/API_CreateEventSourceMapping.html",
                                 "BatchSize > 10 requires MaximumBatchingWindowInSeconds >= 1"),
    "valkey_ops_per_event": Input(8, "Assumed", "commands executed inside one Lua call"),
    "dedup_ttl_h": Input(8, "Assumed", "> Lambda MaximumRecordAge (6 h) so retries hit"),
    "dedup_key_bytes": Input(90, "Assumed", "key + hash value + overhead; MEASURE"),
    "active_visitors_30d": Input(20e6, "Assumed", "distinct anonymous_ids in 30 days"),
    "visitors_with_rule_state": Input(0.3, "Assumed", "share matching any segment rule"),
    "rule_state_bytes": Input(400, "Assumed", "per-visitor hash of rule:day buckets"),
    "persons": Input(5e6, "Assumed", "identified users with identity cache entries"),
    "person_cache_bytes": Input(150, "Assumed", "person -> anon set"),
    "tenants": Input(500, "Observed", "brief: 500+ tenants"),
    "hll_bytes": doc(12 * KIB, "https://valkey.io/topics/hyperloglogs/",
                     "dense HyperLogLog size 12 KB"),
    "valkey_reserved": Input(0.25, "Assumed", "memory reserved for overhead/replication"),
    "valkey_headroom": Input(1.5, "Assumed", "growth/fragmentation headroom"),
    "valkey_min_shards": Input(2, "Assumed", "throughput headroom; unbenchmarked"),
    "raw_compression": Input(8, "Assumed", "gzip JSON ratio"),
    "curated_compression": Input(10, "Assumed", "Parquet ratio"),
    "raw_retention_days": Input(14, "Assumed", "short so expiry bounds erasure time"),
    "curated_retention_days": Input(395, "Assumed", "13 months"),
    "iceberg_overhead": Input(1.2, "Assumed", "snapshots/rewrites awaiting VACUUM"),
    "quarantine_share": Input(0.01, "Assumed", "share of events quarantined/failed"),
    "firehose_round_kib": doc(5, "https://aws.amazon.com/firehose/pricing/",
                              "records billed in 5 KB increments"),
    "rds_storage_gb": Input(200, "Assumed", "rollups + graph + registry + audit"),
    "athena_adhoc_tb": Input(2.0, "Assumed", "ad hoc + export queries per month"),
    "athena_deletion_tb": Input(1.0, "Assumed", "weekly batched DELETE/OPTIMIZE scans"),
    "s3_put_millions": Input(3.0, "Assumed", "Firehose, Iceberg, compaction writes"),
    "s3_get_millions": Input(30.0, "Assumed", "Athena reads"),
    "interface_endpoints": Input(9, "Assumed",
                                 "kinesis, firehose, logs, monitoring, ecr x2, sts, "
                                 "secrets, kms"),
    "azs": Input(3, "Assumed", "availability zones"),
    "nat_gateways": Input(1, "Assumed", "small egress (webhooks); bulk traffic via endpoints"),
    "cross_az_kib_per_event": Input(1.5, "Assumed", "Lambda<->Valkey traffic per event"),
    "cross_az_share": Input(2 / 3, "Assumed", "share of that traffic crossing AZs"),
    "export_share": Input(0.5, "Assumed", "share of curated data exported off-AWS"),
    "log_gb_month": Input(80, "Assumed", "aggregated logs only, no per-event logging"),
    "custom_metrics": Input(250, "Assumed", "global metrics; per-tenant data in Postgres"),
    "alarms": Input(60, "Assumed", "standard-resolution alarms"),
    "kms_keys": Input(5, "Assumed", "customer managed keys"),
    "kms_requests_m": Input(10, "Assumed", "with S3 bucket keys enabled"),
    "cloudtrail_data_events_m": Input(10, "Assumed", "S3 data events on privacy buckets"),
    "secrets": Input(10, "Assumed", "Secrets Manager secrets"),
    "glue_requests_m": Input(2, "Assumed", "catalog requests from Athena"),
}

# --- Prices (us-east-1, on-demand) ---------------------------------------------------
P = {
    "fargate_vcpu_h": price(0.03238, "AmazonECS", "Fargate Linux/ARM vCPU-hour"),
    "fargate_gb_h": price(0.00356, "AmazonECS", "Fargate Linux/ARM GB-hour"),
    "alb_h": price(0.0225, "AWSELB", "ALB-hour"),
    "alb_lcu_h": price(0.008, "AWSELB", "LCU-hour (25 new conn/s; 1 GB/h processed)"),
    "kds_shard_h": price(0.015, "AmazonKinesis", "provisioned shard-hour"),
    "kds_put_m": price(0.014, "AmazonKinesis", "per 1M PUT payload units (25 KB)"),
    "kds_ext_ret_shard_h": price(0.020, "AmazonKinesis", "extended retention shard-hour"),
    "lambda_gb_s": price(0.0000133334, "AWSLambda", "Arm GB-second"),
    "lambda_req_m": price(0.20, "AWSLambda", "per 1M requests"),
    "valkey_r7g_large_h": price(0.1752, "AmazonElastiCache", "cache.r7g.large Valkey"),
    "valkey_r7g_xlarge_h": price(0.3496, "AmazonElastiCache", "cache.r7g.xlarge Valkey"),
    "rds_r7g_large_maz_h": price(0.478, "AmazonRDS", "db.r7g.large PostgreSQL Multi-AZ"),
    "rds_gp3_maz_gb": price(0.23, "AmazonRDS", "gp3 Multi-AZ GB-month"),
    "rds_backup_gb": price(0.095, "AmazonRDS",
                           "backup storage beyond free allocation, GB-month"),
    "firehose_kds_gb": price(0.029, "AmazonKinesisFirehose", "KDS source, GB (tier 1)"),
    "firehose_put_gb": price(0.029, "AmazonKinesisFirehose", "Direct PUT, GB (tier 1)"),
    "s3_gb": price(0.023, "AmazonS3", "Standard storage GB-month (first 50 TB)"),
    "s3_put_k": price(0.005, "AmazonS3", "PUT/COPY/POST/LIST per 1,000"),
    "s3_get_k": price(0.0004, "AmazonS3", "GET per 1,000"),
    "athena_tb": price(5.00, "AmazonAthena", "per TB scanned"),
    "vpce_h": price(0.01, "AmazonVPC", "interface endpoint per AZ-hour"),
    "vpce_gb": price(0.01, "AmazonVPC", "interface endpoint GB processed"),
    "nat_h": price(0.045, "AmazonEC2", "NAT gateway hour"),
    "nat_gb": price(0.045, "AmazonEC2", "NAT gateway GB processed"),
    "xaz_gb": price(0.01, "AWSDataTransfer", "cross-AZ per GB, each direction"),
    "dto_gb": price(0.09, "AWSDataTransfer", "internet egress first 10 TB"),
    "cw_logs_gb": price(0.50, "AmazonCloudWatch", "logs ingestion GB"),
    "cw_logs_store_gb": price(0.03, "AmazonCloudWatch", "logs storage GB-month"),
    "cw_metric": price(0.30, "AmazonCloudWatch", "metric-month (first 10k)"),
    "cw_alarm": price(0.10, "AmazonCloudWatch", "standard alarm-month"),
    "kms_key": price(1.00, "awskms", "customer managed key-month"),
    "kms_req_10k": price(0.03, "awskms", "per 10,000 requests"),
    "cloudtrail_data_event": price(0.000001, "AWSCloudTrail", "per data event"),
    "secret": price(0.40, "AWSSecretsManager", "per secret-month"),
    "glue_req_m": price(1.00, "AWSGlue", "Data Catalog per 1M requests"),
    "sqs_req_m": price(0.40, "AWSQueueService", "standard requests per 1M (tier 1)"),
    "waf_acl": price(5.00, "awswaf", "web ACL-month"),
    "waf_rule": price(1.00, "awswaf", "rule-month"),
    "waf_req_m": price(0.60, "awswaf", "per 1M requests (tier applicability unverified)"),
}

VALKEY_NODES = [("cache.r7g.large", 13.07, "valkey_r7g_large_h"),
                ("cache.r7g.xlarge", 26.32, "valkey_r7g_xlarge_h")]


def v(table, key):
    return table[key].value


# --- Scenarios ----------------------------------------------------------------------

def rates():
    avg = v(T, "events_per_day") / 86400
    peak_hour = avg * v(T, "peak_hour_factor")
    return {
        "avg": avg,
        "peak_hour": peak_hour,
        "spike_avg": avg * v(T, "spike_multiplier"),
        "spike_peak_hour": peak_hour * v(T, "spike_multiplier"),
    }


def scenarios():
    """Each scenario is a list of (hours, events/s) segments summing to 730 h."""
    r = rates()
    spike_h = 2 * 4         # [Assumed] two 4-hour spikes per month at 10x peak-hour rate
    sustained_h = 7 * 24    # [Assumed] one week at 10x the daily average (stress case)
    return {
        "normal": [(HOURS_PER_MONTH, r["avg"])],
        "short_spike": [(HOURS_PER_MONTH - spike_h, r["avg"]),
                        (spike_h, r["spike_peak_hour"])],
        "sustained_high": [(HOURS_PER_MONTH - sustained_h, r["avg"]),
                           (sustained_h, r["spike_avg"])],
    }


SCENARIO_NOTES = {
    "normal": "730 h at the daily average rate [Estimated from Observed 50M/day]",
    "short_spike": "two 4 h spikes at 10x the peak-hour rate [Assumed]",
    "sustained_high": "7 days at 10x the daily average, e.g. Black Friday week [Assumed]",
}


def design_peak():
    return math.ceil(rates()["spike_peak_hour"] / 1000) * 1000


def shards_for(rate, event_kib):
    by_bytes = rate * event_kib / KIB / v(D, "shard_write_mib_s")
    by_records = rate / v(D, "shard_write_rec_s")
    need = max(by_bytes, by_records)
    return by_bytes, by_records, math.ceil(need / v(D, "shard_target_util"))


def valkey_memory_gb(peak_sustained_rate):
    parts = {
        "dedup keys": peak_sustained_rate * v(D, "dedup_ttl_h") * 3600
                      * v(D, "dedup_key_bytes"),
        "segment rule state": v(D, "active_visitors_30d") * v(D, "visitors_with_rule_state")
                              * v(D, "rule_state_bytes"),
        "identity cache": v(D, "persons") * v(D, "person_cache_bytes"),
        "hourly HLLs (48 h)": v(D, "tenants") * 48 * v(D, "hll_bytes"),
        "minute counters (2 h)": v(D, "tenants") * 120 * 30 * 80,
        "bot burst windows": peak_sustained_rate * 60 * 60,
    }
    return {k: b / GIB for k, b in parts.items()}


def choose_valkey(required_gb):
    best = None
    for name, mem, key in VALKEY_NODES:
        usable = mem * (1 - v(D, "valkey_reserved"))
        shards = max(v(D, "valkey_min_shards"), math.ceil(required_gb / usable))
        nodes = shards * 2                      # primary + one replica per shard
        cost = nodes * v(P, key) * HOURS_PER_MONTH
        if best is None or cost < best[-1]:
            best = (name, mem, shards, nodes, key, cost)
    return best


# --- Cost model ---------------------------------------------------------------------

def monthly_cost(segments, event_kib=None, shards=None, valkey=None):
    kib = event_kib or v(T, "event_kib")
    hours = sum(h for h, _ in segments)
    events = sum(h * 3600 * r for h, r in segments)
    requests = events / v(T, "events_per_request")
    raw_gib = events * kib * KIB / GIB
    c = {}

    # Collectors (Fargate Arm), autoscaled per segment with a floor.
    task_hours = 0
    for h, r in segments:
        tasks = max(v(D, "collector_min_tasks"),
                    math.ceil(r / v(D, "collector_rps_per_task")
                              / v(D, "collector_target_util")))
        task_hours += h * tasks
    task_rate = (v(D, "collector_vcpu") * v(P, "fargate_vcpu_h")
                 + v(D, "collector_gb") * v(P, "fargate_gb_h"))
    c["Collectors: ECS Fargate (Arm)"] = task_hours * task_rate

    # ALB: LCU = max(new conn/25, GB per hour); new connections dominate in worst case.
    lcu_hours = 0
    for h, r in segments:
        req_s = r / v(T, "events_per_request")
        gb_h = req_s * 3600 * (kib * v(T, "events_per_request") + v(T, "http_overhead_kib")) \
            * KIB / GIB
        lcu_hours += h * max(req_s * v(T, "new_conn_fraction") / 25, gb_h)
    c["Load balancing: ALB"] = hours * v(P, "alb_h") + lcu_hours * v(P, "alb_lcu_h")

    # Kinesis provisioned, fixed shard count sized for the design peak.
    put_units = events * math.ceil(kib / 25)
    c["Streaming: Kinesis shards"] = shards * hours * v(P, "kds_shard_h")
    c["Streaming: Kinesis 7-day retention"] = shards * hours * v(P, "kds_ext_ret_shard_h")
    c["Streaming: Kinesis PUT units"] = put_units / 1e6 * v(P, "kds_put_m")

    # Lambda processor: one poll per shard per second while data flows.
    invocations = 0
    for h, r in segments:
        per_shard = r / shards
        batch_events = min(v(D, "lambda_batch_size"), per_shard * v(D, "lambda_batch_window_s"))
        duration = v(D, "lambda_overhead_s") + batch_events * v(D, "lambda_per_event_s")
        cycles_full = per_shard / v(D, "lambda_batch_size")
        cycles_window = 1 / (v(D, "lambda_batch_window_s") + duration)
        invocations += shards * h * 3600 * max(cycles_full, cycles_window)
    gb_s = (invocations * v(D, "lambda_overhead_s")
            + events * v(D, "lambda_per_event_s")) * v(D, "lambda_mem_gb")
    c["Processing: Lambda"] = invocations / 1e6 * v(P, "lambda_req_m") + gb_s * v(P, "lambda_gb_s")

    # Valkey (fixed, sized for sustained-high), no backups by design.
    c["Hot state: ElastiCache Valkey"] = valkey[-1] * hours / HOURS_PER_MONTH

    # PostgreSQL.
    c["Database: RDS PostgreSQL Multi-AZ"] = (hours * v(P, "rds_r7g_large_maz_h")
                                             + v(D, "rds_storage_gb") * v(P, "rds_gp3_maz_gb"))
    c["Backups: RDS beyond free allocation"] = 0.0   # see uncertainty notes

    # Firehose: raw (one event per record -> 5 KB rounding) and curated staging (packed).
    round_kib = max(kib, v(D, "firehose_round_kib"))
    round_kib = math.ceil(round_kib / v(D, "firehose_round_kib")) * v(D, "firehose_round_kib")
    c["Firehose: raw archive (KDS source)"] = events * round_kib * KIB / GIB \
        * v(P, "firehose_kds_gb")
    c["Firehose: curated staging (packed Direct PUT)"] = raw_gib * v(P, "firehose_put_gb")

    # S3 steady state after 13 months (curated grows to retention).
    day_gib = raw_gib / (hours / 24)
    raw_store = day_gib / v(D, "raw_compression") * v(D, "raw_retention_days")
    curated_store = (day_gib / v(D, "curated_compression") * v(D, "curated_retention_days")
                     * v(D, "iceberg_overhead"))
    quarantine_store = day_gib * v(D, "quarantine_share") * v(D, "raw_retention_days")
    staging_store = day_gib / v(D, "raw_compression") * 2
    c["Storage: S3 (steady state at 13 months)"] = (
        (raw_store + curated_store + quarantine_store + staging_store) * v(P, "s3_gb")
        + v(D, "s3_put_millions") * 1000 * v(P, "s3_put_k")
        + v(D, "s3_get_millions") * 1000 * v(P, "s3_get_k"))

    # Athena: hourly MERGE (staging hour + today's curated partitions), nightly jobs,
    # ad hoc and deletion scans.
    day_curated = day_gib / v(D, "curated_compression")
    merge_tb = (hours / 24) * (12 * day_curated + day_gib / v(D, "raw_compression")) / 1024
    nightly_tb = (hours / 24) * 2 * day_curated / 1024
    athena_tb = merge_tb + nightly_tb + v(D, "athena_adhoc_tb") + v(D, "athena_deletion_tb")
    c["Analytics: Athena"] = athena_tb * v(P, "athena_tb")
    c["Analytics: Glue Data Catalog requests"] = v(D, "glue_requests_m") * v(P, "glue_req_m")

    # Networking.
    vpce_gb = raw_gib * 2          # collector->Kinesis, Lambda->Firehose
    c["Networking: VPC interface endpoints"] = (
        v(D, "interface_endpoints") * v(D, "azs") * hours * v(P, "vpce_h")
        + vpce_gb * v(P, "vpce_gb"))
    c["Networking: NAT gateway (small egress)"] = v(D, "nat_gateways") * hours * v(P, "nat_h") \
        + 10 * v(P, "nat_gb")
    xaz_gib = events * v(D, "cross_az_kib_per_event") * KIB / GIB * v(D, "cross_az_share")
    c["Networking: cross-AZ (Lambda<->Valkey)"] = xaz_gib * 2 * v(P, "xaz_gb")
    dto_gib = (requests * v(T, "response_kib") * KIB / GIB
               + day_curated * (hours / 24) * v(D, "export_share"))
    c["Networking: internet egress (responses + exports)"] = dto_gib * v(P, "dto_gb")

    # Monitoring and compliance.
    c["Monitoring: CloudWatch logs"] = v(D, "log_gb_month") * (v(P, "cw_logs_gb")
                                                               + v(P, "cw_logs_store_gb"))
    c["Monitoring: CloudWatch metrics + alarms"] = (v(D, "custom_metrics") * v(P, "cw_metric")
                                                    + v(D, "alarms") * v(P, "cw_alarm"))
    c["Security: KMS"] = (v(D, "kms_keys") * v(P, "kms_key")
                          + v(D, "kms_requests_m") * 100 * v(P, "kms_req_10k"))
    c["Security: CloudTrail data events"] = (v(D, "cloudtrail_data_events_m") * 1e6
                                             * v(P, "cloudtrail_data_event"))
    c["Security: Secrets Manager"] = v(D, "secrets") * v(P, "secret")
    return c, {"events": events, "raw_gib": raw_gib, "athena_tb": athena_tb,
               "lcu_hours": lcu_hours, "task_hours": task_hours, "invocations": invocations}


def optional_costs(events):
    return {
        "Optional: AWS WAF on the ALB (10 rules)": (v(P, "waf_acl") + 10 * v(P, "waf_rule")
                                                     + events / 1e6 * v(P, "waf_req_m")),
        "Contingency: SQS fallback, 1 h outage at design peak": (
            design_peak() * 3600 / 10 * 3 / 1e6 * v(P, "sqs_req_m")),
    }


# --- Report -------------------------------------------------------------------------

def fmt_money(x):
    return f"${x:,.0f}"


def table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return "\n".join(out)


def inputs_table(title, tbl):
    rows = []
    for key, inp in tbl.items():
        val = inp.value
        if isinstance(val, float) and val < 0.01:
            val = f"{val:.10f}".rstrip("0")
        rows.append((f"`{key}`", val, f"[{inp.label}]", inp.source))
    return f"### {title}\n\n" + table(["input", "value", "label", "source"], rows)


def report() -> str:
    r = rates()
    peak = design_peak()
    kib = v(T, "event_kib")
    out = ["# Engineer 004 - sizing and cost model",
           "",
           "Generated by `artifact/sizing.py` (Python standard library, deterministic). "
           "**Every output is [Estimated]: arithmetic over the labeled inputs below. "
           "Nothing in this report is a load test or a measurement.** Region: us-east-1. "
           f"Prices: AWS Price List API, on-demand, retrieved {RETRIEVED} "
           f"(`{PRICE_API.format('<ServiceCode>')}`).",
           ""]

    out += ["## 1. Throughput", "",
            table(["quantity", "events/s", "basis"], [
                ("daily average", f"{r['avg']:,.0f}", "50M / 86,400"),
                ("peak hour", f"{r['peak_hour']:,.0f}", "x peak_hour_factor"),
                ("10x daily average", f"{r['spike_avg']:,.0f}", "x spike_multiplier"),
                ("10x peak hour", f"{r['spike_peak_hour']:,.0f}", "conservative spike"),
                ("**design peak**", f"**{peak:,}**", "10x peak hour, rounded up"),
            ]), ""]

    rows = []
    for size in v(T, "event_sizes_kib"):
        by_b, by_r, shards = shards_for(peak, size)
        rows.append((f"{size} KiB", f"{peak * size / KIB:,.1f}", f"{by_r:.1f}", f"{by_b:.1f}",
                     shards, f"{v(T, 'events_per_day') * size * KIB / GIB:,.0f}",
                     f"{v(T, 'events_per_day') * size * KIB / GIB * 30.4 / 1024:,.2f}"))
    out += ["## 2. Capacity by event size (at design peak)", "",
            "Shards = ceil(max(records/1,000, MiB/1) / target utilization 0.5). "
            "Records bind below 1 KiB, bytes above it.", "",
            table(["event size", "MiB/s", "shards by records", "shards by bytes",
                   "provisioned shards", "raw GiB/day", "raw TiB/month"], rows), ""]

    _, _, shards = shards_for(peak, kib)
    per_shard_rec = peak / shards * v(D, "hash_skew")
    per_shard_mib = per_shard_rec * kib / KIB
    read_mib = per_shard_mib * v(D, "shared_consumers")
    out += ["## 3. Hot-partition and shared-consumer risk (1 KiB events)", "",
            table(["check", "value", "limit", "status"], [
                ("busiest shard records/s (with skew)", f"{per_shard_rec:,.0f}",
                 "1,000", "ok" if per_shard_rec < 1000 else "OVER"),
                ("headroom for a single hot visitor, rec/s",
                 f"{1000 - per_shard_rec:,.0f}",
                 f"per-visitor limit {v(D, 'per_visitor_limit')}",
                 "ok" if 1000 - per_shard_rec > v(D, "per_visitor_limit") else "RISK"),
                ("shared read MiB/s on busiest shard", f"{read_mib:.2f}",
                 "2.0 shared across consumers", "ok" if read_mib < 2 else "OVER"),
                ("shared read with a 3rd consumer (replay)",
                 f"{per_shard_mib * (v(D, 'shared_consumers') + 1):.2f}", "2.0",
                 "ok" if per_shard_mib * 3 < 2 else "use enhanced fan-out or replay stream"),
            ]), "",
            "Partition key `tenant_id:anonymous_id` spreads a large tenant across shards; "
            "the risk is one visitor (e.g. a bot) flooding one shard, which the "
            "per-visitor collector limit caps. Replays read a separate replay stream, not "
            "the live stream, so live consumers keep their shared read budget.", ""]

    per_shard_ev = peak / shards
    out += ["## 4. Processor throughput", "",
            table(["quantity", "value", "note"], [
                ("events/s per shard at design peak", f"{per_shard_ev:,.0f}", ""),
                ("per-event time budget, ParallelizationFactor 1",
                 f"{1000 / per_shard_ev:.1f} ms", "sequential per shard"),
                ("same with ParallelizationFactor 10", f"{10000 / per_shard_ev:.1f} ms",
                 "max 10 per shard [Benchmarked: AWS Lambda docs]"),
                ("assumed per-event cost", f"{v(D, 'lambda_per_event_s') * 1000:.1f} ms",
                 "[Assumed]; must benchmark"),
                ("theoretical per-shard ceiling, PF 1",
                 f"{v(D, 'lambda_batch_size') / (v(D, 'lambda_overhead_s') + v(D, 'lambda_batch_size') * v(D, 'lambda_per_event_s')):,.0f} ev/s",
                 "BatchSize / batch duration; [Estimated], not measured"),
                ("Valkey Lua calls/s at design peak", f"{peak:,}", "one per event"),
                ("Valkey commands/s at design peak",
                 f"{peak * v(D, 'valkey_ops_per_event'):,}", "inside Lua; must benchmark"),
            ]), ""]

    mem_normal = valkey_memory_gb(r["avg"])
    mem_sust = valkey_memory_gb(r["spike_avg"])
    required = sum(mem_sust.values()) * v(D, "valkey_headroom")
    valkey = choose_valkey(required)
    out += ["## 5. Valkey memory", "",
            table(["component", "normal GiB", "sustained-high GiB"],
                  [(k, f"{mem_normal[k]:.2f}", f"{mem_sust[k]:.2f}") for k in mem_normal]
                  + [("**total**", f"**{sum(mem_normal.values()):.2f}**",
                      f"**{sum(mem_sust.values()):.2f}**")]), "",
            f"Required with x{v(D, 'valkey_headroom')} headroom: {required:.1f} GiB. "
            f"Cheapest fit: **{valkey[2]} shards x {valkey[0]} + 1 replica each = "
            f"{valkey[3]} nodes** ({fmt_money(valkey[-1])}/month), "
            f"{v(D, 'valkey_reserved'):.0%} of node memory reserved. Dedup keys dominate "
            "and scale with traffic x TTL. No Valkey backups: all state is derived and "
            "rebuilt from S3.", ""]

    day_gib = v(T, "events_per_day") * kib * KIB / GIB
    out += ["## 6. Storage (1 KiB events, steady state)", "",
            table(["store", "GiB", "basis"], [
                ("Kinesis (7-day retention)", f"{day_gib * 7:,.0f}", "uncompressed"),
                ("S3 raw (14 days)", f"{day_gib / v(D, 'raw_compression') * 14:,.0f}",
                 "gzip ratio 8 [Assumed]"),
                ("S3 curated Iceberg (13 months)",
                 f"{day_gib / v(D, 'curated_compression') * 395 * v(D, 'iceberg_overhead'):,.0f}",
                 "Parquet ratio 10, x1.2 snapshot overhead [Assumed]"),
                ("S3 quarantine + failed batches (14 days)",
                 f"{day_gib * v(D, 'quarantine_share') * 14:,.0f}", "1% of events [Assumed]"),
                ("RDS PostgreSQL", f"{v(D, 'rds_storage_gb')}", "[Assumed]"),
            ]), ""]

    # (stage, typical ms, worst non-retry ms, basis). Typical uses the average wait of a
    # periodic poll (half the period); worst uses the full period.
    stages = [
        ("collector validate + PutRecords + response", 30, 50, "[Estimated]"),
        ("Kinesis availability + Lambda poll", 500, 1000,
         "[Benchmarked] ESM polls each shard once per second; polling consumers see "
         "'typically below 1 second' propagation"),
        ("batching window (BatchSize 500 needs >= 1 s)", 1000, 1000,
         "[Benchmarked] API rule; at normal traffic (~24 ev/s/shard) a 500-record batch "
         "never fills, so the full window is spent [Estimated]"),
        ("Lambda batch + Valkey Lua pipeline", 150, 300, "[Estimated]; must benchmark"),
        ("dashboard read (Valkey) + client refresh", 500, 1000, "1 s poll or push [Assumed]"),
    ]
    typ = sum(s[1] for s in stages) / 1000
    worst = sum(s[2] for s in stages) / 1000
    out += ["## 7. Latency budget", "",
            "Two different measurements. **L1** (what the pipeline controls) starts at "
            "collector receipt (`received_at`). **L2** (what a customer sees) starts at SDK "
            "event creation and adds SDK buffering, network, and retries, which we cannot "
            "change or observe server-side. Summing poll wait and batching window is "
            "conservative: they can overlap. All values are estimates, none measured.", "",
            "### L1: collector receipt -> visible on dashboard", "",
            table(["stage", "typical", "worst (no retry)", "label / basis"],
                  [(n, f"{t} ms", f"{w} ms", b) for n, t, w, b in stages]
                  + [("**total**", f"**{typ:.2f} s**", f"**{worst:.2f} s**",
                      f"[Estimated]; {5 - worst:.2f} s headroom vs 5 s in the worst "
                      "non-retry case"),
                     ("tail: Lambda retry / bisect", "", "+1-3 s per retry", "[Estimated]"),
                     ("tail: backlog (IteratorAge)", "", "unbounded",
                      "alarm at 30 s, page at 5 min")]), "",
            "### L2: SDK event creation -> collector receipt (added on top of L1)", "",
            table(["stage", "estimate", "label"], [
                ("SDK buffering / flush interval", "unknown", "SDK discovery item"),
                ("browser -> ALB network + TLS", "50-300 ms", "[Assumed]"),
                ("SDK retry after a failed request", "SDK-defined backoff", "unknown"),
            ]), "",
            f"L1 worst case leaves {5 - worst:.2f} s of the 5 s target for L2. Whether "
            "the 5 s target holds end-to-end depends on the SDK's flush interval, which "
            "must be measured. Plan: synthetic canary events every 10 s through the real "
            "endpoint (L1, server clock), plus a headless browser running the real SDK with "
            "a synchronized clock (L2).", ""]

    out += ["## 8. Monthly cost by scenario", ""]
    results = {}
    for name, segs in scenarios().items():
        results[name] = monthly_cost(segs, shards=shards, valkey=valkey)
    names = list(results)
    lines = list(results[names[0]][0])
    rows = [(item, *[fmt_money(results[n][0][item]) for n in names]) for item in lines]
    totals = [sum(results[n][0].values()) for n in names]
    rows.append(("**Total (new pipeline only)**", *[f"**{fmt_money(t)}**" for t in totals]))
    out += [table(["line item", *names], rows), "",
            "Scenarios: " + "; ".join(f"**{n}**: {SCENARIO_NOTES[n]}" for n in names) + ".", "",
            table(["driver", *names],
                  [("events/month", *[f"{results[n][1]['events'] / 1e9:.2f}B" for n in names]),
                   ("raw GiB/month", *[f"{results[n][1]['raw_gib']:,.0f}" for n in names]),
                   ("collector task-hours", *[f"{results[n][1]['task_hours']:,.0f}"
                                              for n in names]),
                   ("ALB LCU-hours", *[f"{results[n][1]['lcu_hours']:,.0f}" for n in names]),
                   ("Athena TB scanned", *[f"{results[n][1]['athena_tb']:.1f}"
                                           for n in names])]), ""]

    opt = optional_costs(results["normal"][1]["events"])
    out += ["Not in the totals:", ""]
    out += [f"- {k}: {fmt_money(val)}/month" for k, val in opt.items()]
    out += ["- **Legacy pipeline during parallel run: NOT ESTIMATED** (current cost unknown).",
            "- AWS Support plan, engineering time, and Snowflake/BigQuery charges on the "
            "customer side: excluded.", ""]

    rows = []
    for size in v(T, "event_sizes_kib"):
        _, _, s = shards_for(peak, size)
        c, _ = monthly_cost(scenarios()["normal"], event_kib=size, shards=s, valkey=valkey)
        rows.append((f"{size} KiB", s, fmt_money(sum(c.values()))))
    worst_alb = (HOURS_PER_MONTH * v(P, "alb_h")
                 + HOURS_PER_MONTH * peak / 25 * v(P, "alb_lcu_h"))
    out += ["## 9. Sensitivity", "",
            table(["event size (normal month)", "shards", "total"], rows), "",
            f"- ALB if the design peak were sustained all month: {fmt_money(worst_alb)}.",
            f"- Per-tenant CloudWatch custom metrics (500 tenants x 10): "
            f"{fmt_money(5000 * v(P, 'cw_metric'))}/month - avoided by keeping per-tenant "
            "reconciliation in PostgreSQL.",
            f"- Logging every event to CloudWatch (normal month): "
            f"{fmt_money(results['normal'][1]['raw_gib'] * v(P, 'cw_logs_gb'))}/month - "
            "avoided.",
            f"- Routing ingest through NAT instead of endpoints: +"
            f"{fmt_money(results['normal'][1]['raw_gib'] * v(P, 'nat_gb'))}/month.",
            ""]

    rows = []
    for rps in (250, 500, 1000):
        saved = D["collector_rps_per_task"]
        D["collector_rps_per_task"] = Input(rps, "Assumed", "sensitivity")
        c, info = monthly_cost(scenarios()["sustained_high"], shards=shards, valkey=valkey)
        D["collector_rps_per_task"] = saved
        rows.append((f"{rps} req/s per task", f"{info['task_hours']:,.0f}",
                     fmt_money(c["Collectors: ECS Fargate (Arm)"]),
                     fmt_money(sum(c.values()))))
    out += ["Collector capacity is the least certain input (no load test yet). "
            "Sustained-high month:", "",
            table(["collector capacity [Assumed]", "task-hours", "collector cost", "total"],
                  rows), ""]
    rows = []
    for raw_ratio, cur_ratio in ((4, 5), (8, 10), (12, 15)):
        saved = (D["raw_compression"], D["curated_compression"])
        D["raw_compression"] = Input(raw_ratio, "Assumed", "sensitivity")
        D["curated_compression"] = Input(cur_ratio, "Assumed", "sensitivity")
        c, _ = monthly_cost(scenarios()["normal"], shards=shards, valkey=valkey)
        D["raw_compression"], D["curated_compression"] = saved
        rows.append((f"gzip {raw_ratio}:1, Parquet {cur_ratio}:1",
                     fmt_money(c["Storage: S3 (steady state at 13 months)"]),
                     fmt_money(c["Analytics: Athena"]), fmt_money(sum(c.values()))))
    out += ["Compression ratios are assumed (the 25-row fixture is too small to measure "
            "them). Normal month:", "",
            table(["compression [Assumed]", "S3", "Athena", "total"], rows), "",
            "S3 is billed only for the retained window and stops at lifecycle expiration "
            "[Benchmarked: S3 lifecycle docs]; the S3 line is the 13-month steady state, "
            "so early months cost less.", ""]

    normal_c, normal_i = results["normal"]
    proxy_extra = normal_c["Collectors: ECS Fargate (Arm)"] * 0.5
    fwd_gib = normal_i["raw_gib"] * (1 + v(T, "http_overhead_kib") / v(T, "event_kib"))
    private_fwd = fwd_gib * 2 * v(P, "xaz_gb") * v(D, "cross_az_share")
    nat_fwd = fwd_gib * v(P, "nat_gb")
    out += ["## 10. Migration overlap (months 1-6, in addition to the totals above)", "",
            table(["item", "$/month", "basis"], [
                ("collector proxy forwarding to legacy", fmt_money(proxy_extra),
                 "+50% collector CPU [Assumed]"),
                ("legacy forward over a private path (same VPC / peering)",
                 fmt_money(private_fwd), "cross-AZ share of forwarded GiB [Estimated]"),
                ("legacy forward through a NAT gateway (avoid)", fmt_money(nat_fwd),
                 "$0.045/GB processed [Benchmarked]"),
                ("**legacy pipeline itself**", "**NOT ESTIMATED**", "current cost unknown"),
            ]), "",
            "The new-pipeline totals never include legacy costs. During the overlap the "
            "company pays both; that sum is an open question for the existing team.", ""]

    out += ["## 11. Cost uncertainties", "",
            "- **RDS backup storage**: free up to the account's total provisioned database "
            "storage in the region [Benchmarked: https://aws.amazon.com/rds/faqs/]; beyond "
            "that $0.095/GB-month. With 7-day retention and modest churn the line is $0; if "
            "backups grew to an extra 1x of DB size: "
            f"{fmt_money(v(D, 'rds_storage_gb') * v(P, 'rds_backup_gb'))}/month.",
            "- **Athena MERGE INTO and VACUUM billing** is not stated in AWS docs (DELETE "
            "and OPTIMIZE are documented as billed by data scanned); the model assumes "
            "MERGE is billed by data scanned too.",
            "- **Cross-AZ charges for ElastiCache replication, RDS Multi-AZ replication, and "
            "ALB-to-target traffic** were not verified and are not included.",
            "- **Firehose 5 KB rounding** on the raw stream is the conservative reading of "
            "the pricing page; a `KDS-no-rounding` SKU at $0.074/GB exists in the price "
            "list but its applicability is undocumented.",
            "- **Firehose Iceberg destination** SKUs ($0.045 and $0.075/GB) have unclear "
            "applicability; the design avoids them (packed staging + hourly Athena MERGE).",
            "- **WAF request tier** applicability is unclear; WAF is shown as optional.",
            "- All usage quantities marked [Assumed] (log volume, metric count, Athena scans, "
            "S3 requests) dominate small lines; none dominates the total.",
            ""]

    out += ["## Appendix: inputs", "",
            inputs_table("Traffic", T), "", inputs_table("Design parameters", D), "",
            inputs_table("Prices (us-east-1, on-demand)", P), ""]
    return "\n".join(out)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, help="write the Markdown report here")
    args = parser.parse_args(argv)
    text = report()
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text + "\n", encoding="utf-8")
        print(f"report written to {args.out}")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
