"""Unit tests for detect_anomalies.py.

Tests build their own synthetic events so they exercise detector behavior,
not the contents of the challenge fixture. Run from this directory:

    python3 -m unittest -v test_detect_anomalies
"""
from __future__ import annotations

import json
import unittest
from pathlib import Path

from detect_anomalies import ACTIONS, Config, analyze_lines

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "event_sample.jsonl"


def event(event_id="e-1", tenant="t-1", anon="a-1", user=None, etype="page_view",
          ts="2026-01-01T10:00:00.000Z", received="2026-01-01T10:00:00.200Z", **props):
    obj = {"event_id": event_id, "tenant_id": tenant, "anonymous_id": anon, "user_id": user,
           "type": etype, "ts": ts, "received_at": received, "properties": props or {"path": "/"}}
    return json.dumps(obj)


def at(seconds, base_minute=0):
    """Timestamp helper: 2026-01-01T10:<base_minute>:<seconds>Z with ms precision."""
    whole = int(seconds)
    ms = round((seconds - whole) * 1000)
    return f"2026-01-01T10:{base_minute:02d}:{whole:02d}.{ms:03d}Z"


def findings(report, category=None, line=None):
    return [f for f in report["findings"]
            if (category is None or f["category"] == category)
            and (line is None or f["line"] == line)]


def disposition(report, line):
    return report["lines"][line - 1]["disposition"]


class MalformedRecords(unittest.TestCase):
    def test_invalid_json_is_quarantined_and_processing_continues(self):
        lines = [event("e-1"), '{"event_id":"e-2","tenant_id":"t-1"', event("e-3")]
        report = analyze_lines(lines)
        bad = findings(report, "malformed_record")
        self.assertEqual(len(bad), 1)
        self.assertEqual(bad[0]["line"], 2)
        self.assertEqual(bad[0]["event_id"], "e-2")  # recovered by pattern match
        self.assertEqual(bad[0]["action"], "quarantine")
        self.assertEqual(report["summary"]["parsed_events"], 2)
        self.assertEqual(disposition(report, 3), "accept")

    def test_non_object_json_and_empty_lines_are_malformed(self):
        report = analyze_lines(['["not", "an", "event"]', "   ", "42"])
        self.assertEqual(len(findings(report, "malformed_record")), 3)
        self.assertEqual(report["summary"]["malformed_lines"], 3)

    def test_missing_event_id_is_quarantined(self):
        obj = json.loads(event())
        del obj["event_id"]
        report = analyze_lines([json.dumps(obj)])
        self.assertEqual(findings(report, "missing_event_id")[0]["action"], "quarantine")


class Duplicates(unittest.TestCase):
    def test_retry_with_new_received_at_is_deduplicated(self):
        first = event("e-1", received=at(0.2))
        retry = event("e-1", received=at(7.5))
        report = analyze_lines([first, retry])
        dup = findings(report, "duplicate")
        self.assertEqual([f["line"] for f in dup], [2])
        self.assertEqual(dup[0]["details"]["duplicate_of_line"], 1)
        self.assertEqual(disposition(report, 1), "accept")
        self.assertEqual(disposition(report, 2), "deduplicate")
        self.assertEqual(report["summary"]["unique_events"], 1)

    def test_same_event_id_in_different_tenants_is_not_a_duplicate(self):
        report = analyze_lines([event("e-1", tenant="t-1"), event("e-1", tenant="t-2")])
        self.assertEqual(findings(report, "duplicate"), [])
        self.assertEqual(findings(report, "event_id_conflict"), [])
        self.assertEqual(report["summary"]["unique_events"], 2)
        self.assertEqual(report["summary"]["lines_by_disposition"], {"accept": 2})

    def test_same_key_with_different_payload_is_escalated_not_dropped(self):
        report = analyze_lines([event("e-1", path="/a"), event("e-1", path="/b")])
        self.assertEqual(findings(report, "duplicate"), [])
        conflicts = findings(report, "event_id_conflict")
        self.assertEqual(sorted(f["line"] for f in conflicts), [1, 2])
        self.assertTrue(all(f["action"] == "escalate" for f in conflicts))


class LegacySchema(unittest.TestCase):
    def test_legacy_fields_types_and_properties_are_normalized(self):
        legacy = json.dumps({"event_id": "e-1", "tenant_id": "t-1", "anonymous_id": "a-1",
                             "user_id": None, "type": "pageview",
                             "timestamp": "2026-01-01T10:00:00Z",
                             "properties": {"page_path": "/pricing", "ref": "https://x.com"}})
        report = analyze_lines([legacy])
        mappings = findings(report, "legacy_schema")[0]["details"]["mappings"]
        self.assertIn("timestamp -> ts", mappings)
        self.assertIn("properties.page_path -> properties.path", mappings)
        self.assertIn("properties.ref -> properties.referrer", mappings)
        self.assertIn("type 'pageview' -> 'page_view'", mappings)
        self.assertEqual(findings(report, "missing_received_at")[0]["action"], "normalize")
        # Normalized ts parses, so no invalid-timestamp finding.
        self.assertEqual(findings(report, "invalid_timestamp"), [])
        self.assertEqual(findings(report, "unknown_event_type"), [])

    def test_conflicting_legacy_and_canonical_fields_are_flagged(self):
        obj = json.loads(event(ts=at(0)))
        obj["timestamp"] = at(30)
        report = analyze_lines([json.dumps(obj)])
        self.assertEqual(findings(report, "schema_conflict")[0]["action"], "flag")

    def test_canonical_event_has_no_legacy_finding(self):
        self.assertEqual(findings(analyze_lines([event()]), "legacy_schema"), [])


class Tenants(unittest.TestCase):
    def test_null_missing_and_malformed_tenants_are_quarantined(self):
        missing = json.loads(event("e-3"))
        del missing["tenant_id"]
        report = analyze_lines([event("e-1", tenant=None), event("e-2", tenant="tenant 42"),
                                json.dumps(missing)])
        bad = findings(report, "invalid_tenant")
        self.assertEqual(sorted(f["line"] for f in bad), [1, 2, 3])
        self.assertTrue(all(f["action"] == "quarantine" for f in bad))

    def test_tenantless_events_are_excluded_from_dedup_and_stitching(self):
        lines = [event("e-1", tenant=None, user="u-1", etype="identify"),
                 event("e-1", tenant=None, user="u-1", etype="identify")]
        report = analyze_lines(lines)
        self.assertEqual(findings(report, "duplicate"), [])
        self.assertEqual(findings(report, "identity_link"), [])

    def test_tenant_pattern_is_configurable(self):
        report = analyze_lines([event(tenant="acme")], Config(tenant_id_pattern=r"^[a-z]+$"))
        self.assertEqual(findings(report, "invalid_tenant"), [])


class Timestamps(unittest.TestCase):
    def test_client_clock_ahead_of_server_is_skew(self):
        report = analyze_lines([event(ts=at(50), received=at(2))])
        skew = findings(report, "client_clock_skew")
        self.assertEqual(len(skew), 1)
        self.assertEqual(skew[0]["detection"], "deterministic")
        self.assertEqual(findings(report, "far_future_timestamp"), [])

    def test_small_lead_within_tolerance_is_ignored(self):
        report = analyze_lines([event(ts=at(1.0), received=at(0.0))])
        self.assertEqual(findings(report, "client_clock_skew"), [])

    def test_far_future_is_classified_separately_from_skew(self):
        report = analyze_lines([event(ts="2027-01-01T10:00:00.000Z", received=at(0))])
        self.assertEqual(len(findings(report, "far_future_timestamp")), 1)
        self.assertEqual(findings(report, "client_clock_skew"), [])

    def test_far_future_threshold_is_configurable(self):
        line = event(ts="2026-01-01T12:00:00.000Z", received=at(0))  # 2h ahead
        default = analyze_lines([line])
        tight = analyze_lines([line], Config(far_future_threshold_s=3600))
        self.assertEqual(len(findings(default, "client_clock_skew")), 1)
        self.assertEqual(len(findings(tight, "far_future_timestamp")), 1)

    def test_corrected_timestamp_keeps_original_value_and_reason(self):
        raw = "2027-06-15T14:11:27.31Z"  # deliberately non-canonical precision
        report = analyze_lines([event(ts=raw, received="2026-06-15T14:11:27.522Z")])
        details = findings(report, "far_future_timestamp")[0]["details"]
        self.assertEqual(details["original_ts"], raw)
        self.assertEqual(details["event_time_used"], "2026-06-15T14:11:27.522Z")
        self.assertEqual(details["event_time_source"], "received_at")
        self.assertIn("after server receipt", details["correction_reason"])

    def test_skew_correction_records_original_and_source(self):
        details = findings(analyze_lines([event(ts=at(50), received=at(2))]),
                           "client_clock_skew")[0]["details"]
        self.assertEqual(details["original_ts"], at(50))
        self.assertEqual(details["event_time_used"], at(2))

    def test_invalid_timestamp_keeps_original_value(self):
        details = findings(analyze_lines([event(ts="yesterday")]),
                           "invalid_timestamp")[0]["details"]
        self.assertEqual(details["original_ts"], "yesterday")
        self.assertEqual(details["event_time_source"], "received_at")

    def test_late_arrival_is_heuristic(self):
        report = analyze_lines([event(ts=at(0, 0), received=at(0, 30))])
        late = findings(report, "late_arrival")
        self.assertEqual(late[0]["detection"], "heuristic")

    def test_unparseable_or_timezoneless_ts_falls_back(self):
        report = analyze_lines([event("e-1", ts="yesterday"),
                                event("e-2", ts="2026-01-01T10:00:00")])
        self.assertEqual(len(findings(report, "invalid_timestamp")), 2)

    def test_out_of_order_caused_by_skew_is_resolved_by_normalization(self):
        lines = [event("e-1", ts="2026-01-01T11:05:00.000Z", received=at(0)),  # 65 min ahead
                 event("e-2", ts=at(30), received=at(30.2))]
        report = analyze_lines(lines)
        ooo = findings(report, "out_of_order")
        self.assertEqual([f["line"] for f in ooo], [2])
        self.assertTrue(ooo[0]["details"]["resolved_by_timestamp_normalization"])

    def test_genuine_out_of_order_persists_after_normalization(self):
        lines = [event("e-1", ts=at(40), received=at(40.2)),
                 event("e-2", ts=at(10), received=at(41))]
        ooo = findings(analyze_lines(lines), "out_of_order")
        self.assertEqual(len(ooo), 1)
        self.assertFalse(ooo[0]["details"]["resolved_by_timestamp_normalization"])
        self.assertEqual(ooo[0]["action"], "flag")

    def test_ordering_is_tracked_per_visitor(self):
        lines = [event("e-1", anon="a-1", ts=at(40), received=at(40.2)),
                 event("e-2", anon="a-2", ts=at(10), received=at(41))]
        self.assertEqual(findings(analyze_lines(lines), "out_of_order"), [])


class Pii(unittest.TestCase):
    def test_email_and_phone_values_are_detected_without_leaking_values(self):
        line = event(etype="custom", name="quote", contact_email="jane.doe@corp.example",
                     phone="+1-555-0100")
        report = analyze_lines([line])
        pii = findings(report, "pii_in_properties")
        self.assertEqual(len(pii), 1)
        self.assertEqual(pii[0]["detection"], "deterministic")
        signals = {f["property"]: f["signals"] for f in pii[0]["details"]["fields"]}
        self.assertIn("email_value", signals["properties.contact_email"])
        self.assertIn("phone_value", signals["properties.phone"])
        dumped = json.dumps(report)
        self.assertNotIn("jane.doe", dumped)
        self.assertNotIn("555-0100", dumped)

    def test_email_under_innocent_key_is_still_detected(self):
        report = analyze_lines([event(etype="custom", note="reach me at a.b@c.io")])
        self.assertEqual(len(findings(report, "pii_in_properties")), 1)

    def test_key_name_only_match_is_heuristic(self):
        report = analyze_lines([event(etype="custom", email="[redacted]")])
        self.assertEqual(findings(report, "pii_in_properties")[0]["detection"], "heuristic")

    def test_phone_shaped_value_alone_is_heuristic(self):
        report = analyze_lines([event(etype="custom", note="+1 (415) 555-0100")])
        self.assertEqual(findings(report, "pii_in_properties")[0]["detection"], "heuristic")

    def test_dates_are_not_phone_numbers(self):
        report = analyze_lines([event(etype="custom", renewal="2026-06-15",
                                      started="2026-06-15T14:02:11.104Z")])
        self.assertEqual(findings(report, "pii_in_properties"), [])

    def test_ordinary_properties_are_not_pii(self):
        report = analyze_lines([event(selector="#cta-demo", path="/pricing", plan="trial",
                                      referrer="https://google.com", count=12345678)])
        self.assertEqual(findings(report, "pii_in_properties"), [])


class IdentityStitching(unittest.TestCase):
    def test_identify_links_prior_anonymous_events(self):
        lines = [event("e-1", ts=at(0)), event("e-2", ts=at(5)),
                 event("e-3", user="u-9", etype="identify", ts=at(9))]
        report = analyze_lines(lines)
        link = findings(report, "identity_link")[0]
        self.assertEqual(link["details"]["user_id"], "u-9")
        self.assertEqual(link["details"]["anonymous_events_to_attribute"], ["e-1", "e-2"])
        self.assertEqual(report["identity_graph"], {"t-1": {"a-1": ["u-9"]}})

    def test_stitching_is_isolated_per_tenant(self):
        lines = [event("e-1", tenant="t-2", anon="a-1"),
                 event("e-2", tenant="t-1", anon="a-1", user="u-9", etype="identify")]
        link = findings(analyze_lines(lines), "identity_link")[0]
        self.assertEqual(link["details"]["anonymous_events_to_attribute"], [])

    def test_anonymous_id_linked_to_two_users_is_escalated(self):
        lines = [event("e-1", user="u-1", etype="identify"),
                 event("e-2", user="u-2", etype="identify", ts=at(5), received=at(5.1))]
        conflicts = findings(analyze_lines(lines), "identity_conflict")
        self.assertEqual(sorted(f["line"] for f in conflicts), [1, 2])
        self.assertTrue(all(f["action"] == "escalate" for f in conflicts))

    def test_pii_becomes_attributable_after_identify(self):
        lines = [event("e-1", etype="custom", contact_email="x@y.io"),
                 event("e-2", user="u-1", etype="identify", ts=at(5), received=at(5.1))]
        link = findings(analyze_lines(lines), "identity_link")[0]
        self.assertEqual(link["details"]["pii_events_now_attributable"], ["e-1"])


class Bots(unittest.TestCase):
    def burst(self, n, spacing, anon="a-1", referrer=None):
        props = {"path": "/p"}
        if referrer:
            props["referrer"] = referrer
        return [event(f"e-{i}", anon=anon, ts=at(i * spacing), received=at(i * spacing + 0.1),
                      **props) for i in range(n)]

    def test_rapid_burst_from_one_visitor_is_flagged_heuristically(self):
        report = analyze_lines(self.burst(4, 0.02))
        bots = findings(report, "suspected_bot")
        self.assertEqual(len(bots), 4)
        self.assertTrue(all(f["detection"] == "heuristic" and f["action"] == "flag"
                            for f in bots))
        self.assertTrue(all("not confirmed" in f["explanation"] for f in bots))

    def test_human_paced_browsing_is_not_flagged(self):
        self.assertEqual(findings(analyze_lines(self.burst(4, 8)), "suspected_bot"), [])

    def test_burst_split_across_visitors_is_not_flagged(self):
        lines = self.burst(2, 0.02, anon="a-1") + [
            event(f"x-{i}", anon="a-2", ts=at(i * 0.02), received=at(i * 0.02 + 0.1))
            for i in range(2)]
        self.assertEqual(findings(analyze_lines(lines), "suspected_bot"), [])

    def test_bot_referrer_alone_is_flagged(self):
        report = analyze_lines(self.burst(1, 0, referrer="https://crawler.example.net/x"))
        bots = findings(report, "suspected_bot")
        self.assertEqual(len(bots), 1)
        self.assertIn("referrer", bots[0]["details"]["signals"][0])

    def test_burst_threshold_is_configurable(self):
        cfg = Config(bot_burst_min_events=6)
        self.assertEqual(findings(analyze_lines(self.burst(4, 0.02), cfg), "suspected_bot"), [])


class PrivacyRequests(unittest.TestCase):
    def test_deletion_request_is_escalated_never_executed(self):
        lines = [event("e-1", anon="a-7", ts=at(0)),
                 event("e-2", anon="a-7", user="u-7", etype="privacy_request", ts=at(5),
                       received=at(5.1), request="delete_all_data", regulation="GDPR"),
                 event("e-1", tenant="t-2", anon="a-7", ts=at(6), received=at(6.1))]
        report = analyze_lines(lines)
        req = findings(report, "privacy_request")
        self.assertEqual(len(req), 1)
        self.assertEqual(req[0]["action"], "escalate")
        self.assertFalse(req[0]["details"]["auto_executed"])
        # Scope reaches the stitched anonymous event, but not the other tenant's event.
        self.assertEqual(req[0]["details"]["deletion_scope_lines"], [1, 2])
        self.assertEqual(report["summary"]["privacy_requests_auto_executed"], 0)
        # Nothing is removed from the report: every input line is still accounted for.
        self.assertEqual(len(report["lines"]), 3)

    def test_deletion_scope_ignores_identity_links_from_other_tenants(self):
        # In t-2, a-8 belongs to u-7. That must not pull t-1's unrelated a-8 visitor
        # into a t-1 deletion request for u-7.
        lines = [event("e-1", tenant="t-1", anon="a-8"),
                 event("e-2", tenant="t-2", anon="a-8", user="u-7", etype="identify"),
                 event("e-3", tenant="t-1", anon="a-7", user="u-7", etype="privacy_request",
                       ts=at(5), received=at(5.1), request="delete_all_data")]
        req = findings(analyze_lines(lines), "privacy_request")[0]
        self.assertEqual(req["details"]["deletion_scope_lines"], [3])

    def test_privacy_event_cannot_establish_an_identity_link(self):
        # A (possibly forged) request claims visitor a-9 belongs to u-1.
        lines = [event("e-1", anon="a-9", path="/pricing"),
                 event("e-2", anon="a-9", user="u-1", etype="privacy_request", ts=at(5),
                       received=at(5.1), request="delete_all_data"),
                 event("e-3", anon="a-1", user="u-1", etype="custom", ts=at(6),
                       received=at(6.1), name="viewed_pricing", count_today=3)]
        report = analyze_lines(lines)
        self.assertEqual(report["identity_graph"], {"t-1": {"a-1": ["u-1"]}})
        claim = findings(report, "unverified_identity_claim")[0]
        self.assertEqual(claim["action"], "flag")
        self.assertFalse(claim["details"]["corroborated_by_trusted_link"])
        self.assertEqual(findings(report, "identity_link", line=2), [])
        # u-1's analytics identity does not absorb a-9's history.
        agg = findings(report, "client_computed_aggregate")[0]
        self.assertEqual(agg["details"]["server_side_matching_page_views_in_sample"], [])
        # The request's own scope is a candidate pending verification, never executed.
        req = findings(report, "privacy_request")[0]["details"]
        self.assertEqual(req["scope_status"], "candidate_pending_identity_verification")
        self.assertFalse(req["auto_executed"])

    def test_privacy_claim_matching_a_trusted_link_is_marked_corroborated(self):
        lines = [event("e-1", anon="a-7", user="u-7", etype="identify"),
                 event("e-2", anon="a-7", user="u-7", etype="privacy_request", ts=at(5),
                       received=at(5.1), request="delete_all_data")]
        claim = findings(analyze_lines(lines), "unverified_identity_claim")[0]
        self.assertTrue(claim["details"]["corroborated_by_trusted_link"])

    def test_privacy_wording_in_ordinary_properties_is_not_a_request(self):
        report = analyze_lines([event(path="/account/delete")])
        self.assertEqual(findings(report, "privacy_request"), [])

    def test_request_without_tenant_is_escalated_without_scope(self):
        line = event(tenant=None, user="u-7", etype="privacy_request", request="erase")
        req = findings(analyze_lines([line]), "privacy_request")[0]
        self.assertNotIn("deletion_scope_lines", req["details"])


class ClientAggregates(unittest.TestCase):
    def test_client_count_is_flagged_and_cross_checked(self):
        lines = [event("e-1", path="/pricing"),
                 event("e-2", etype="custom", ts=at(5), received=at(5.1),
                       name="viewed_pricing", count_today=3)]
        agg = findings(analyze_lines(lines), "client_computed_aggregate")
        self.assertEqual(len(agg), 1)
        self.assertEqual(agg[0]["detection"], "heuristic")
        self.assertEqual(agg[0]["details"]["server_side_matching_page_views_in_sample"], ["e-1"])

    def test_non_aggregate_numbers_are_not_flagged(self):
        report = analyze_lines([event(etype="custom", name="x", price=10, total_flag=True)])
        self.assertEqual(findings(report, "client_computed_aggregate"), [])


class ReportShape(unittest.TestCase):
    def test_every_line_is_accounted_for_with_valid_labels(self):
        lines = [event("e-1"), "{bad", event("e-1", received=at(9)), event("e-2", tenant=None)]
        report = analyze_lines(lines)
        self.assertEqual([l["line"] for l in report["lines"]], [1, 2, 3, 4])
        self.assertEqual(report["summary"]["total_lines"], 4)
        self.assertEqual(sum(report["summary"]["lines_by_disposition"].values()), 4)
        for f in report["findings"]:
            self.assertIn(f["action"], ACTIONS)
            self.assertIn(f["detection"], ("deterministic", "heuristic"))
            self.assertTrue(f["explanation"])

    @unittest.skipUnless(FIXTURE.exists(), "challenge fixture not present")
    def test_runs_end_to_end_on_the_challenge_fixture(self):
        raw = FIXTURE.read_text(encoding="utf-8").splitlines()
        report = analyze_lines(raw)
        self.assertEqual(report["summary"]["total_lines"], len(raw))
        self.assertEqual(len(report["lines"]), len(raw))


if __name__ == "__main__":
    unittest.main()
