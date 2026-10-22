# Detector changelog

## 2026-10-22 — Milestone 2 checkpoint fixes

Found by probing the properties the checkpoint asked about (tenant isolation, unauthenticated
privacy events, timestamp provenance, heuristic labelling). The original 42 tests all passed
before these fixes; each defect below now has a regression test that fails if the fix is reverted.

1. **Unauthenticated privacy events could create identity links.** A `privacy_request` carrying
   `anonymous_id` + `user_id` was added to the identity graph like an `identify` call, so a forged
   request could merge another visitor's history into a user (affecting segments and other
   deletion scopes). Now such pairs are reported as `unverified_identity_claim` (action `flag`),
   kept out of `identity_graph`, and listed under `unverified_identity_claims`. The event types
   treated this way are configurable (`unverified_link_event_types`). Deletion scopes are now
   labelled `scope_status: candidate_pending_identity_verification` with a `scope_basis`.
   Fixture effect: `evt-0017` no longer produces an `identity_link`; its candidate scope is
   unchanged (`evt-0006`, `evt-0017`) because it is based on the request's own identifiers.
2. **Dates were detected as phone numbers, labelled deterministic.** `"2026-06-15"` matched the
   phone pattern. ISO dates/datetimes are now excluded, and a phone-shaped value on its own is
   `heuristic`; only an email-shaped value makes a PII finding `deterministic`.
   Fixture effect: none (`evt-0007` contains an email, so it stays deterministic).
3. **Timestamp corrections did not preserve the original value verbatim or say what was used.**
   Findings re-serialised timestamps (e.g. `.31Z` became `.310000Z`). Corrections now carry
   `original_ts` (raw string), `received_at` (raw), `event_time_used`, `event_time_source`, and
   `correction_reason`.
4. **Wording:** `suspected_bot` explanations now say "heuristic, not confirmed".

Tests: 42 → 49, all passing. Fixture SHA-256 unchanged. See `../output/run_log.txt`.
