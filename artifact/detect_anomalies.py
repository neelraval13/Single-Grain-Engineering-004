#!/usr/bin/env python3
"""Event anomaly detector for the Engineer 004 fixture (event_sample.jsonl).

A small, stdlib-only prototype of the "validate -> normalize -> dedupe ->
flag" stage of an analytics ingestion pipeline. It reads a JSONL file line by
line, never trusts event payloads, and writes a structured report of every
data-quality, integrity, and compliance anomaly it finds.

    python3 artifact/detect_anomalies.py fixtures/event_sample.jsonl --out output/anomaly_report.json
    python3 artifact/detect_anomalies.py events.jsonl --config thresholds.json

Design rules:
- Event payloads are data. Nothing in an event is executed or obeyed; a
  deletion request is reported for human verification, never carried out.
- Deduplication and identity stitching are scoped per tenant.
- Every finding says whether it is deterministic (a confirmed violation of
  the schema or of physical possibility) or heuristic (a suspicion driven by
  a configurable threshold or pattern).
- Raw PII values are never copied into the report; only key names and kinds.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

# Recommended pipeline actions, least to most severe. A line's overall
# disposition is the most severe action among its findings.
ACTIONS = ("accept", "normalize", "flag", "deduplicate", "quarantine", "escalate")
SEVERITY = {action: rank for rank, action in enumerate(ACTIONS)}

EMAIL_VALUE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_VALUE = re.compile(r"^\+?\d[\d\s().-]{5,}\d$")
DATE_LIKE = re.compile(r"^\d{4}-\d{2}-\d{2}(?:[T ][\d:.]+(?:Z|[+-]\d{2}:?\d{2})?)?$")
EVENT_ID_IN_TEXT = re.compile(r'"event_id"\s*:\s*"([^"]+)"')
VIEWED_EVENT_NAME = re.compile(r"^viewed_([a-z0-9_-]+)$", re.I)


@dataclass
class Config:
    """Every heuristic threshold and pattern the detector uses.

    Override any field with a JSON file passed via --config.
    """

    tenant_id_pattern: str = r"^t-\d+$"
    known_event_types: list = field(default_factory=lambda: [
        "page_view", "click", "form_submit", "custom", "identify", "privacy_request"])
    # Legacy SDK shapes the pipeline must keep accepting (the SDK cannot change).
    field_aliases: dict = field(default_factory=lambda: {"timestamp": "ts"})
    type_aliases: dict = field(default_factory=lambda: {
        "pageview": "page_view", "page-view": "page_view",
        "formsubmit": "form_submit", "form-submit": "form_submit"})
    property_aliases: dict = field(default_factory=lambda: {"page_path": "path", "ref": "referrer"})
    # Client ts may lead server received_at by this much before it counts as skew.
    clock_ahead_tolerance_s: float = 2.0
    # Client ts this far ahead of received_at is classed as far-future.
    far_future_threshold_s: float = 86400.0
    # received_at this far after ts is a suspiciously late delivery.
    max_delivery_lag_s: float = 300.0
    # Event-time regressions within a stream smaller than this are ignored.
    ordering_tolerance_s: float = 0.0
    # Bot heuristic: this many events from one visitor inside the window.
    bot_burst_min_events: int = 4
    bot_burst_window_s: float = 1.0
    bot_referrer_pattern: str = r"bot|crawler|spider|scanner"
    pii_key_pattern: str = (r"e[-_]?mail|phone|mobile|ssn|social_security|address|"
                            r"first_name|last_name|full_name|birth|dob|passport|card_number")
    privacy_event_types: list = field(default_factory=lambda: ["privacy_request"])
    privacy_request_pattern: str = r"delet|eras|forget|opt[-_]?out|do[-_]?not[-_]?sell"
    # Only these property keys are searched for privacy wording, so a page
    # path like "/delete-account" does not read as a deletion request.
    privacy_request_keys: list = field(default_factory=lambda: [
        "request", "privacy_request", "action"])
    # anonymous_id/user_id pairs on these event types are recorded as unverified
    # claims and never added to the identity graph (a forged request must not be
    # able to merge someone else's history into a user).
    unverified_link_event_types: list = field(default_factory=lambda: ["privacy_request"])
    aggregate_key_pattern: str = r"(^|_)(count|counts|total|sum|num|times)(_|$)"

    @classmethod
    def from_file(cls, path: Path) -> "Config":
        overrides = json.loads(path.read_text(encoding="utf-8"))
        known = {f.name for f in fields(cls)}
        unknown = set(overrides) - known
        if unknown:
            raise ValueError(f"unknown config keys: {sorted(unknown)}")
        return cls(**overrides)


def parse_ts(value) -> datetime | None:
    """Parse an ISO-8601 timestamp. Timestamps without a timezone are rejected
    rather than guessed as UTC."""
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def iter_strings(value, path=""):
    """Yield (key_path, string_value) for every string nested in value."""
    if isinstance(value, dict):
        for key, child in value.items():
            yield from iter_strings(child, f"{path}.{key}" if path else str(key))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from iter_strings(child, f"{path}[{index}]")
    elif isinstance(value, str):
        yield path, value


class Detector:
    def __init__(self, config: Config | None = None):
        self.cfg = config or Config()
        self.tenant_re = re.compile(self.cfg.tenant_id_pattern)
        self.bot_referrer_re = re.compile(self.cfg.bot_referrer_pattern, re.I)
        self.pii_key_re = re.compile(self.cfg.pii_key_pattern, re.I)
        self.privacy_re = re.compile(self.cfg.privacy_request_pattern, re.I)
        self.aggregate_re = re.compile(self.cfg.aggregate_key_pattern, re.I)
        self.findings: list[dict] = []
        self.records: list[dict] = []
        self.line_event_ids: dict[int, str | None] = {}
        self.total_lines = 0

    # -- reporting -----------------------------------------------------------

    def add(self, line, event_id, category, action, deterministic, explanation, **details):
        assert action in SEVERITY, action
        finding = {
            "line": line,
            "event_id": event_id,
            "category": category,
            "action": action,
            "detection": "deterministic" if deterministic else "heuristic",
            "explanation": explanation,
        }
        if details:
            finding["details"] = details
        self.findings.append(finding)

    # -- per-line parsing ----------------------------------------------------

    def analyze(self, lines) -> dict:
        for number, raw in enumerate(lines, start=1):
            self.total_lines = number
            self.parse_line(number, raw)
        self.deduplicate()
        for rec in self.records:
            if not rec["duplicate_of"]:
                self.check_timestamps(rec)
                self.check_pii(rec)
        active = [r for r in self.records
                  if r["tenant_ok"] and r["event_id"] and not r["duplicate_of"]]
        links = self.stitch_identities(active)
        self.check_ordering(active)
        self.check_bots(active)
        self.check_aggregates(active, links)
        self.check_privacy(links)
        return self.build_report(links)

    def parse_line(self, number, raw):
        text = raw.strip()
        if not text:
            self.line_event_ids[number] = None
            self.add(number, None, "malformed_record", "quarantine", True, "Empty line.")
            return
        try:
            obj = json.loads(text)
        except json.JSONDecodeError as exc:
            match = EVENT_ID_IN_TEXT.search(text)
            recovered = match.group(1) if match else None
            self.line_event_ids[number] = recovered
            self.add(number, recovered, "malformed_record", "quarantine", True,
                     f"Invalid JSON ({exc.msg} at char {exc.pos}); quarantined to a dead-letter "
                     "queue without attempting repair. The event_id was recovered by pattern "
                     "match and is unverified." if recovered else
                     f"Invalid JSON ({exc.msg} at char {exc.pos}); quarantined without repair.")
            return
        if not isinstance(obj, dict):
            self.line_event_ids[number] = None
            self.add(number, None, "malformed_record", "quarantine", True,
                     f"Line is valid JSON but a {type(obj).__name__}, not an event object.")
            return

        event = self.normalize(number, obj)
        event_id = event.get("event_id")
        if not isinstance(event_id, str) or not event_id.strip():
            event_id = None
            self.add(number, None, "missing_event_id", "quarantine", True,
                     "No usable event_id, so the event cannot be deduplicated or audited.")
        self.line_event_ids[number] = event_id

        tenant = event.get("tenant_id")
        tenant_ok = isinstance(tenant, str) and bool(self.tenant_re.match(tenant))
        if not tenant_ok:
            reason = "missing or null" if tenant in (None, "") else "not a valid tenant identifier"
            self.add(number, event_id, "invalid_tenant", "quarantine", True,
                     f"tenant_id is {reason}. In a multi-tenant pipeline the event cannot be "
                     "attributed; it is held out of dedup, stitching, and dashboards rather "
                     "than guessed into a tenant.")

        etype = event.get("type")
        if not isinstance(etype, str) or not etype:
            self.add(number, event_id, "unknown_event_type", "quarantine", True,
                     "Event has no type.")
        elif etype not in self.cfg.known_event_types:
            self.add(number, event_id, "unknown_event_type", "flag", True,
                     f"Event type '{etype}' is not in the known type list.")

        self.records.append({
            "line": number,
            "event": event,
            "event_id": event_id,
            "tenant": tenant if tenant_ok else None,
            "tenant_ok": tenant_ok,
            "duplicate_of": None,
        })

    def normalize(self, number, obj) -> dict:
        """Map legacy SDK field names onto the canonical schema."""
        event = dict(obj)
        props = event.get("properties")
        props = dict(props) if isinstance(props, dict) else props
        event["properties"] = props
        renames, conflicts = [], []

        def apply_aliases(target, aliases, scope):
            for legacy, canonical in aliases.items():
                if legacy not in target:
                    continue
                value = target.pop(legacy)
                if canonical in target and target[canonical] != value:
                    conflicts.append(f"{scope}{legacy} vs {scope}{canonical}")
                elif canonical not in target:
                    target[canonical] = value
                    renames.append(f"{scope}{legacy} -> {scope}{canonical}")

        apply_aliases(event, self.cfg.field_aliases, "")
        if isinstance(props, dict):
            apply_aliases(props, self.cfg.property_aliases, "properties.")
        etype = event.get("type")
        if isinstance(etype, str) and etype in self.cfg.type_aliases:
            event["type"] = self.cfg.type_aliases[etype]
            renames.append(f"type '{etype}' -> '{event['type']}'")

        event_id = event.get("event_id") if isinstance(event.get("event_id"), str) else None
        if renames:
            self.add(number, event_id, "legacy_schema", "normalize", True,
                     "Event uses a legacy SDK shape; mapped onto the canonical schema "
                     "(the SDK cannot be changed, so this mapping is permanent).",
                     mappings=renames)
        if conflicts:
            self.add(number, event_id, "schema_conflict", "flag", True,
                     "Legacy and canonical fields are both present with different values; "
                     "the canonical value was kept and the conflict needs review.",
                     conflicting_fields=conflicts)
        if "received_at" not in event:
            self.add(number, event_id, "missing_received_at", "normalize", True,
                     "No server receipt timestamp. Ingestion would stamp received_at on "
                     "arrival; until then client clock skew cannot be measured for this event.")
        return event

    # -- deduplication -------------------------------------------------------

    def deduplicate(self):
        seen: dict[tuple, dict] = {}
        for rec in self.records:
            if not rec["tenant_ok"] or not rec["event_id"]:
                continue
            key = (rec["tenant"], rec["event_id"])
            first = seen.get(key)
            if first is None:
                seen[key] = rec
                continue
            if self._payload(first) == self._payload(rec):
                rec["duplicate_of"] = first["line"]
                self.add(rec["line"], rec["event_id"], "duplicate", "deduplicate", True,
                         f"Same tenant_id + event_id and identical payload as line {first['line']} "
                         "(only received_at differs), consistent with an SDK retry. Keep the "
                         "first copy, drop this one.",
                         duplicate_of_line=first["line"],
                         first_received_at=first["event"].get("received_at"),
                         this_received_at=rec["event"].get("received_at"))
            else:
                for conflicting in (first, rec):
                    self.add(conflicting["line"], rec["event_id"], "event_id_conflict", "escalate",
                             True, f"tenant_id + event_id is shared by lines {first['line']} and "
                             f"{rec['line']} but the payloads differ, so this is not a safe "
                             "duplicate. Neither copy is dropped automatically.")

    @staticmethod
    def _payload(rec):
        return {k: v for k, v in rec["event"].items() if k != "received_at"}

    # -- per-event checks ----------------------------------------------------

    def check_timestamps(self, rec):
        event, cfg = rec["event"], self.cfg
        ts = parse_ts(event.get("ts"))
        received = parse_ts(event.get("received_at"))
        rec["ts"], rec["received_at"] = ts, received
        rec["effective_ts"] = ts
        raw_ts, raw_received = event.get("ts"), event.get("received_at")

        def corrected(reason):
            return {"original_ts": raw_ts if raw_ts is None else str(raw_ts)[:64],
                    "received_at": raw_received,
                    "event_time_used": raw_received if received else None,
                    "event_time_source": "received_at" if received else "none",
                    "correction_reason": reason}

        if ts is None:
            rec["effective_ts"] = received
            self.add(rec["line"], rec["event_id"], "invalid_timestamp", "normalize", True,
                     "Client timestamp is missing, unparseable, or has no timezone; event time "
                     "falls back to server received_at. The original value is kept for audit.",
                     **corrected("client ts missing, unparseable, or without timezone"))
            return
        if received is None:
            return
        ahead = (ts - received).total_seconds()
        if ahead > cfg.far_future_threshold_s:
            rec["effective_ts"] = received
            self.add(rec["line"], rec["event_id"], "far_future_timestamp", "normalize", True,
                     f"Client ts is {ahead / 86400:.1f} days after the server received the event, "
                     "which is impossible. Event-time windows would mis-bucket it; use "
                     "received_at as event time and keep the raw ts for audit.",
                     seconds_ahead=round(ahead, 3),
                     **corrected(f"client ts more than {cfg.far_future_threshold_s:.0f}s "
                                 "after server receipt"))
        elif ahead > cfg.clock_ahead_tolerance_s:
            rec["effective_ts"] = received
            self.add(rec["line"], rec["event_id"], "client_clock_skew", "normalize", True,
                     f"Client ts is {ahead:.1f}s after server receipt (tolerance "
                     f"{cfg.clock_ahead_tolerance_s}s). The inconsistency is certain; the cause "
                     "(drifted clock, timezone bug) is inferred. Use received_at as event time "
                     "and keep the raw ts for audit.",
                     seconds_ahead=round(ahead, 3),
                     **corrected(f"client ts more than {cfg.clock_ahead_tolerance_s}s after "
                                 "server receipt"))
        elif -ahead > cfg.max_delivery_lag_s:
            self.add(rec["line"], rec["event_id"], "late_arrival", "flag", False,
                     f"Delivered {-ahead:.0f}s after the client ts (threshold "
                     f"{cfg.max_delivery_lag_s}s): offline buffering, retries, or a skewed clock "
                     "running behind. Route through late-event handling; event time is not "
                     "changed.",
                     original_ts=raw_ts, received_at=raw_received, event_time_source="ts")

    def check_pii(self, rec):
        props = rec["event"].get("properties")
        if not isinstance(props, dict):
            return
        hits = []
        for path, value in iter_strings(props):
            key = path.rsplit(".", 1)[-1]
            kinds = []
            if self.pii_key_re.search(key):
                kinds.append("sensitive_key_name")
            if EMAIL_VALUE.search(value):
                kinds.append("email_value")
            if (PHONE_VALUE.match(value) and not DATE_LIKE.match(value)
                    and sum(c.isdigit() for c in value) >= 7):
                kinds.append("phone_value")
            if kinds:
                hits.append({"property": f"properties.{path}", "signals": kinds})
        if hits:
            # An email-shaped value is a confirmed format match; phone-shaped digits and
            # suggestive key names are only suspicions.
            confirmed = any("email_value" in h["signals"] for h in hits)
            self.add(rec["line"], rec["event_id"], "pii_in_properties", "flag", confirmed,
                     "Free-form properties contain likely personal data. Redact or tokenize "
                     "before fan-out to dashboards and warehouse exports, and check the tenant's "
                     "consent basis. Values are withheld from this report.",
                     fields=hits)
            rec["has_pii"] = True

    # -- cross-event checks (valid tenant, not duplicate) --------------------

    def stitch_identities(self, active) -> dict:
        """Return {(tenant, anonymous_id): {user_id: first linking record}}."""
        links: dict[tuple, dict] = {}
        claims = []
        for rec in active:
            anon, user = rec["event"].get("anonymous_id"), rec["event"].get("user_id")
            if not (anon and user):
                continue
            if rec["event"].get("type") in self.cfg.unverified_link_event_types:
                claims.append((rec, anon, user))
            else:
                links.setdefault((rec["tenant"], anon), {}).setdefault(user, rec)

        self.unverified_claims = []
        for rec, anon, user in claims:
            corroborated = user in links.get((rec["tenant"], anon), {})
            claim = {"line": rec["line"], "event_id": rec["event_id"], "tenant_id": rec["tenant"],
                     "anonymous_id": anon, "user_id": user, "event_type": rec["event"]["type"],
                     "corroborated_by_trusted_link": corroborated}
            self.unverified_claims.append(claim)
            self.add(rec["line"], rec["event_id"], "unverified_identity_claim", "flag", True,
                     f"A {rec['event']['type']} event asserts {anon} belongs to {user}. This "
                     "event type is unauthenticated, so the pair is not added to the identity "
                     "graph" + (" (a trusted link already exists)." if corroborated else
                                "; no trusted event in the sample links them."),
                     **{k: v for k, v in claim.items() if k not in ("line", "event_id")})

        for (tenant, anon), users in links.items():
            anon_events = [r for r in active if r["tenant"] == tenant
                           and r["event"].get("anonymous_id") == anon
                           and not r["event"].get("user_id")]
            pii_events = [r["event_id"] for r in anon_events if r.get("has_pii")]
            for user, rec in users.items():
                details = {
                    "tenant_id": tenant, "anonymous_id": anon, "user_id": user,
                    "linked_by_event_type": rec["event"].get("type"),
                    "anonymous_events_to_attribute": [r["event_id"] for r in anon_events],
                }
                note = ""
                if pii_events:
                    details["pii_events_now_attributable"] = pii_events
                    note = " Earlier anonymous events containing PII become tied to this user."
                attribution = (f"{len(anon_events)} anonymous-only event(s) are attributed "
                               "retroactively." if anon_events else
                               "No anonymous-only events for this visitor in the sample.")
                self.add(rec["line"], rec["event_id"], "identity_link", "accept", True,
                         f"Links {anon} to {user} within {tenant}. {attribution}{note}",
                         **details)
            if len(users) > 1:
                for user, rec in users.items():
                    self.add(rec["line"], rec["event_id"], "identity_conflict", "escalate", True,
                             f"{anon} in {tenant} is linked to multiple users {sorted(users)}: a "
                             "shared device or a spoofed identify call. Do not merge "
                             "automatically.", tenant_id=tenant, anonymous_id=anon)
        return links

    def check_ordering(self, active):
        tol = self.cfg.ordering_tolerance_s
        streams: dict[tuple, list] = {}
        for rec in active:
            anon = rec["event"].get("anonymous_id")
            if anon and rec.get("ts"):
                streams.setdefault((rec["tenant"], anon), []).append(rec)
        for stream in streams.values():
            latest_raw = latest_eff = None
            for rec in stream:
                if latest_raw and (latest_raw["ts"] - rec["ts"]).total_seconds() > tol:
                    resolved = (latest_eff is None or rec["effective_ts"] is None or
                                (latest_eff["effective_ts"] - rec["effective_ts"]).total_seconds()
                                <= tol)
                    self.add(rec["line"], rec["event_id"], "out_of_order", "normalize" if resolved
                             else "flag", True,
                             f"Client ts is earlier than line {latest_raw['line']} "
                             f"({latest_raw['event_id']}) from the same visitor, which arrived "
                             "first. " + ("The regression disappears once skewed timestamps are "
                                          "replaced by received_at, so the root cause is the "
                                          "earlier event's clock." if resolved else
                                          "It persists after timestamp normalization; the "
                                          "pipeline's late-event watermark must absorb it."),
                             previous_line=latest_raw["line"],
                             previous_event_id=latest_raw["event_id"],
                             resolved_by_timestamp_normalization=resolved)
                if latest_raw is None or rec["ts"] > latest_raw["ts"]:
                    latest_raw = rec
                if rec["effective_ts"] and (latest_eff is None or
                                            rec["effective_ts"] > latest_eff["effective_ts"]):
                    latest_eff = rec

    def check_bots(self, active):
        cfg = self.cfg
        signals: dict[int, list] = {}
        visitors: dict[tuple, list] = {}
        for rec in active:
            props = rec["event"].get("properties") or {}
            referrer = props.get("referrer") if isinstance(props, dict) else None
            host = urlparse(referrer).hostname if isinstance(referrer, str) else None
            if host and self.bot_referrer_re.search(host):
                signals.setdefault(rec["line"], []).append(f"bot-like referrer host '{host}'")
            anon = rec["event"].get("anonymous_id")
            if anon and rec.get("effective_ts"):
                visitors.setdefault((rec["tenant"], anon), []).append(rec)

        for recs in visitors.values():
            recs = sorted(recs, key=lambda r: r["effective_ts"])
            in_burst = set()
            start = 0
            for end in range(len(recs)):
                while (recs[end]["effective_ts"] - recs[start]["effective_ts"]).total_seconds() \
                        > cfg.bot_burst_window_s:
                    start += 1
                if end - start + 1 >= cfg.bot_burst_min_events:
                    in_burst.update(r["line"] for r in recs[start:end + 1])
            for line in in_burst:
                signals.setdefault(line, []).append(
                    f">= {cfg.bot_burst_min_events} events from one visitor within "
                    f"{cfg.bot_burst_window_s}s")

        by_line = {r["line"]: r for r in active}
        for line in sorted(signals):
            rec = by_line[line]
            self.add(line, rec["event_id"], "suspected_bot", "flag", False,
                     "Suspected bot traffic (heuristic, not confirmed). Keep it in raw storage "
                     "but exclude it from behavioral "
                     "segments, personalization triggers, and dashboard counts until reviewed.",
                     signals=signals[line])

    def check_aggregates(self, active, links):
        for rec in active:
            props = rec["event"].get("properties")
            if not isinstance(props, dict):
                continue
            keys = [k for k, v in props.items() if isinstance(v, (int, float))
                    and not isinstance(v, bool) and self.aggregate_re.search(k)]
            if not keys:
                continue
            details = {"client_values": {k: props[k] for k in keys}}
            context = ""
            match = VIEWED_EVENT_NAME.match(str(props.get("name", "")))
            if match:
                token = match.group(1).lower()
                identity = self._identity(rec, active, links)
                seen = [r["event_id"] for r in active if r in identity
                        and r["event"].get("type") == "page_view"
                        and token in str((r["event"].get("properties") or {}).get("path", "")).lower()]
                details["server_side_matching_page_views_in_sample"] = seen
                context = (f" This sample holds {len(seen)} matching page_view event(s) for the "
                           "same stitched identity; the sample is partial, so that neither "
                           "confirms nor refutes the client's number.")
            self.add(rec["line"], rec["event_id"], "client_computed_aggregate", "flag", False,
                     "Event carries a client-computed aggregate. Segments such as 'viewed pricing "
                     "3x' should be computed server-side from raw events, not trusted from the "
                     f"client.{context}", **details)

    def _identity(self, rec, active, links):
        """All active events in rec's tenant belonging to rec's stitched identity."""
        tenant, event = rec["tenant"], rec["event"]
        users = {event.get("user_id")} - {None}
        anons = {event.get("anonymous_id")} - {None}
        for (t, anon), linked in links.items():
            if t == tenant and (anon in anons or users & set(linked)):
                anons.add(anon)
                users.update(linked)
        return [r for r in active if r["tenant"] == tenant and
                (r["event"].get("user_id") in users or r["event"].get("anonymous_id") in anons)]

    def check_privacy(self, links):
        active = [r for r in self.records
                  if r["tenant_ok"] and r["event_id"] and not r["duplicate_of"]]
        self.privacy_requests = []
        for rec in self.records:
            if rec["duplicate_of"]:
                continue
            event = rec["event"]
            props = event.get("properties") if isinstance(event.get("properties"), dict) else {}
            is_type = event.get("type") in self.cfg.privacy_event_types
            asks = [v for k, v in iter_strings(props)
                    if k in self.cfg.privacy_request_keys and self.privacy_re.search(v)]
            if not (is_type or asks):
                continue
            entry = {"line": rec["line"], "event_id": rec["event_id"], "tenant_id": rec["tenant"],
                     "user_id": event.get("user_id"), "anonymous_id": event.get("anonymous_id"),
                     "requested": asks, "auto_executed": False}
            if rec["tenant_ok"]:
                scope = self._identity(rec, active, links)
                entry["scope_status"] = "candidate_pending_identity_verification"
                entry["scope_basis"] = ("identifiers asserted by the request itself (unverified) "
                                        "plus trusted identity links in the same tenant")
                entry["deletion_scope_event_ids"] = [r["event_id"] for r in scope]
                entry["deletion_scope_lines"] = [r["line"] for r in scope]
                dup_lines = [r["line"] for r in self.records if r["duplicate_of"]
                             in entry["deletion_scope_lines"]]
                if dup_lines:
                    entry["duplicate_copies_in_scope_lines"] = dup_lines
                scope_note = (f" If verified, deletion would cover {len(scope)} candidate "
                              "event(s) in this tenant, including anonymous events reachable only "
                              "through identity stitching, plus raw archives and warehouse "
                              "exports.")
            else:
                scope_note = " The tenant is unknown, so no deletion scope can be computed."
            self.privacy_requests.append(entry)
            self.add(rec["line"], rec["event_id"], "privacy_request", "escalate", True,
                     "Privacy/deletion request received as an SDK event. It is not executed "
                     "automatically: a client-side event is unauthenticated and could be forged, "
                     "so identity must be verified through an authenticated channel, but it must "
                     f"not be ignored either (regulatory deadline).{scope_note}",
                     **{k: v for k, v in entry.items() if k not in ("line", "event_id")})

    # -- report --------------------------------------------------------------

    def build_report(self, links) -> dict:
        self.findings.sort(key=lambda f: (f["line"], SEVERITY[f["action"]] * -1, f["category"]))
        per_line = {}
        for f in self.findings:
            per_line.setdefault(f["line"], []).append(f)
        lines = []
        for number in range(1, self.total_lines + 1):
            found = per_line.get(number, [])
            disposition = max((f["action"] for f in found), key=SEVERITY.get, default="accept")
            lines.append({
                "line": number,
                "event_id": self.line_event_ids.get(number),
                "disposition": disposition,
                "categories": sorted({f["category"] for f in found}),
            })

        def count(values):
            out = {}
            for v in values:
                out[v] = out.get(v, 0) + 1
            return dict(sorted(out.items()))

        parsed = len(self.records)
        duplicates = sum(1 for r in self.records if r["duplicate_of"])
        graph = {}
        for (tenant, anon), users in sorted(links.items()):
            graph.setdefault(tenant, {})[anon] = sorted(users)
        return {
            "summary": {
                "total_lines": self.total_lines,
                "parsed_events": parsed,
                "malformed_lines": self.total_lines - parsed,
                "duplicate_lines": duplicates,
                "unique_events": parsed - duplicates,
                "lines_with_no_findings": sum(1 for l in lines if not l["categories"]),
                "lines_by_disposition": count(l["disposition"] for l in lines),
                "findings_by_category": count(f["category"] for f in self.findings),
                "findings_by_detection": count(f["detection"] for f in self.findings),
                "privacy_requests_auto_executed": 0,
            },
            "config": asdict(self.cfg),
            "lines": lines,
            "findings": self.findings,
            "identity_graph": graph,
            "unverified_identity_claims": self.unverified_claims,
            "privacy_requests": self.privacy_requests,
        }


def analyze_lines(lines, config: Config | None = None) -> dict:
    return Detector(config).analyze(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("input", type=Path, help="JSONL event file")
    parser.add_argument("--out", type=Path, help="write the JSON report here")
    parser.add_argument("--config", type=Path, help="JSON file overriding Config fields")
    args = parser.parse_args(argv)

    config = Config.from_file(args.config) if args.config else Config()
    data = args.input.read_bytes()
    report = analyze_lines(data.decode("utf-8").splitlines(), config)
    report = {"input": {"path": str(args.input), "sha256": hashlib.sha256(data).hexdigest()},
              **report}
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    s = report["summary"]
    print(f"input:   {args.input}")
    print(f"sha256:  {report['input']['sha256']}")
    print(f"lines: {s['total_lines']}  parsed: {s['parsed_events']}  malformed: "
          f"{s['malformed_lines']}  duplicates: {s['duplicate_lines']}  unique: "
          f"{s['unique_events']}  no findings: {s['lines_with_no_findings']}")
    print("dispositions:", ", ".join(f"{k}={v}" for k, v in s["lines_by_disposition"].items()))
    print("detection:   ", ", ".join(f"{k}={v}" for k, v in s["findings_by_detection"].items()))
    print()
    for f in report["findings"]:
        print(f"line {f['line']:>3}  {str(f['event_id']):<9}  {f['category']:<26} "
              f"{f['action']:<11} {f['detection']}")
    if args.out:
        print(f"\nreport written to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
