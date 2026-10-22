# Engineer 004 - solution artifact

Supporting artifact for `SUBMISSION.md` (Beat Claude, Engineer 004, brief version 2026-07).

## Attribution

The challenge brief and the fixture `fixtures/event_sample.jsonl` come from Single Grain's
public Beat Claude repository, https://github.com/ericosiu/beat-claude
(`challenges/engineer-004/`). The fixture is included unmodified so this repository runs on its
own (SHA-256 `1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235`). Everything
else here is our own work.

## What the prototype demonstrates

`artifact/detect_anomalies.py` is a standard-library Python model of the pipeline's
validate -> normalize -> dedupe -> flag stage. It reads the challenge fixture line by line,
never executes or obeys anything inside an event, never auto-executes deletion requests, keeps
tenants isolated in dedup and identity stitching, and labels every finding `deterministic` or
`heuristic`. It is a prototype of one stage, not a pipeline.

`artifact/sizing.py` is a deterministic capacity, latency, and cost model. Every input carries
a label (Observed / Benchmarked / Assumed) and a source; prices are us-east-1 on-demand list
prices from the AWS Price List API, written into the script with their service codes, so no
network access or downloaded price files are needed to reproduce the report.

## Layout

```
./  (repository root)
  SUBMISSION.md                  written answer + submission packet
  README.md                      this file
  artifact/
    detect_anomalies.py          anomaly detector (CLI + library)
    test_detect_anomalies.py     49 unit tests (unittest)
    sizing.py                    capacity / latency / cost model
    CHANGELOG.md                 detector defects found and fixed
  output/
    anomaly_report.json          detector output for the fixture
    sizing_report.md             sizing model output
    run_log.txt                  real commands and outputs
  fixtures/
    event_sample.jsonl           challenge fixture, unmodified (see Attribution)
  design/
    architecture.mmd             Mermaid diagram (renders on GitHub)
    architecture_review.md       design decisions, corrections, evidence classification
    migration.md                 migration, rollback, roadmap
```

## Requirements

Python 3 standard library only; tested with Python 3.13.2. No dependencies, accounts, keys, or
network access. Everything needed is inside this repository.

## Reproduce

Run from the repository root:

```
# 1. Fixture checksum (expected 1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235)
shasum -a 256 fixtures/event_sample.jsonl

# 2. Detector
python3 artifact/detect_anomalies.py fixtures/event_sample.jsonl --out output/anomaly_report.json

# 3. Tests
(cd artifact && python3 -m unittest -v test_detect_anomalies)

# 4. Sizing report
python3 artifact/sizing.py --out output/sizing_report.md
```

Optional pre-screen with the challenge's own validator (lives in the Beat Claude repository,
not here): `python3 <beat-claude>/scripts/validate_submission.py SUBMISSION.md --strict`.

Optional detector config: `--config thresholds.json` overrides any field of `Config`
(for example `{"bot_burst_min_events": 5}`).

## Expected output

- Detector summary: `lines: 25  parsed: 24  malformed: 1  duplicates: 1  unique: 23  no findings: 7`
  and `detection: deterministic=17, heuristic=5`; 22 findings listed by line.
- Tests: `Ran 49 tests ... OK`.
- Sizing: identical `output/sizing_report.md` on every run; monthly totals $3,036 (normal),
  $3,163 (short spike), $4,279 (sustained high) - estimates, not measurements.
- `output/run_log.txt` contains the actual outputs of these commands.

## Known limitations

- Personal-data detection is pattern-based (emails, phone-shaped values, suggestive key names);
  it misses names and addresses in free text and data inside URLs.
- Bot detection is two heuristics (burst rate, referrer host pattern); the default burst
  threshold (4 events in 1 s) equals the size of the fixture's burst, so it is not evidence of
  general accuracy.
- Malformed lines are quarantined without repair, so their contents are never analyzed.
- The tenant-id pattern `^t-\d+$` mirrors the fixture; production would check a tenant registry.
- The whole file is read into memory (to hash it); fine for a prototype, not for large inputs.
- `sizing.py` is arithmetic over assumptions. No load test or AWS deployment has been run;
  collector throughput, Valkey throughput, per-event cost, event size, and compression ratios
  are assumptions that must be measured.
- The Mermaid diagram was not rendered locally.
