# Test fixtures

Three subdirectories, each with a distinct role:

- **`inputs/`** — test-author-controlled inputs. Committed; edit these to add scenarios.
- **`expected/`** — golden files. The harness diffs the live tables against these byte-for-byte. Committed.
- **`outputs/`** — actuals the harness writes per run. Gitignored except for `.gitkeep`. Used to refresh `expected/` after intentional changes.

The harness `tests/cost/test_with_fixtures.py`:
1. Reads `inputs/` as synthetic input to scoring + suppression + routing.
2. Runs the same `_scoring.score_cluster` / `_routing.build_cost_alert_blocks` / `_slack.post` that prod uses.
3. Writes the resulting `cluster_scores`, `event_log`, and `cluster_alerts_latest` snapshots into `outputs/`.
4. Diffs `outputs/*.csv` against `expected/*.csv` for the G-series assertions.

## Inputs (`inputs/`)

| File | What it represents | Key edits to try |
|---|---|---|
| `pricing_rates.csv` | Snapshot of `pricing_rates` table | Add a node type; drop one to force `UNKNOWN`; tweak `dbu_per_hour` |
| `workspace_baseline.csv` | Single-row `workspace_baseline` | Lower `mean_log_daily_dollars` to widen WARNING; bump `stddev_log_daily_dollars` to flatten |
| `clusters_live.csv` | What `WorkspaceClient.clusters.list()` would return | Add scenarios; flip `photon`; bump `num_workers` |
| `cluster_scores_prior.csv` | Pre-existing rows in the **pre-`record_id` schema** | Used to test schema evolution. Don't add `record_id` to this file. |
| `event_log_prior.csv` | Existing event_log rows for suppression scenarios | Adjust `event_ts` (absolute timestamp) to slide rows in/out of the suppression window |
| `excluded_clusters.py` | Same shape as `config/excluded_clusters.py` | Populate all three lists to exercise each exclusion path |
| `scenario_expected.csv` | Ground-truth mapping `cluster_id → expected_signal` (assertion A1) | Update when you change `clusters_live.csv` |

## Expected (`expected/`)

Golden files holding the byte-exact contents we expect each output table to have. Three diffs:

- `expected/cluster_scores.csv` — every scored row after monitor runs (prior rows + freshly scored)
- `expected/event_log.csv` — every event_log row after suppression + routing
- `expected/cluster_alerts_latest.csv` — every row the view returns

On the very first run after the test overhaul, `expected/` is empty. The G-series assertions report `BOOTSTRAP` (not PASS, not FAIL). Refresh expected files like this:

1. Run the harness with `cleanup=no`.
2. Inspect `outputs/*.csv` — read every row, confirm the values are correct.
3. `cp outputs/*.csv expected/` and commit.

From the next run onward, the G-series PASSes if the output matches and FAILs on any drift.

## Determinism contract

For the goldens to be byte-stable, the harness pins every nondeterministic input:

| Source | Pinned to | Mechanism |
|---|---|---|
| Python wall-clock (`datetime.now()`) | `test_now` widget (default `2026-06-04T12:00:00+00:00`) | Parsed once into `NOW`, used everywhere |
| SQL `current_timestamp()` | Same `test_now` | Substituted as `TIMESTAMP '...'` in view + suppression SQL |
| `uuid.uuid4()` in `_scoring.score_cluster` | Sequential counter | Injected via `id_factory` parameter → `00000000-0000-0000-0000-{N:012d}` |
| `WORKSPACE_ID` from runtime context | `9999999999999999` | Hard-coded constant |
| `_user_short` from email | `"test"` | Hard-coded constant |
| Slack `message_ts` (stub mode) | Sequential counter | `stub-ts-{N:08d}` |
| Cluster iteration order | Sorted by `cluster_id` | `cluster_dicts.sort(...)` before scoring |

With all of these pinned, identical input fixtures produce identical output bytes, so the diff is meaningful.

## Baseline math (target scores deliberately)

Baseline shipped in `workspace_baseline.csv`: `mean_log_daily_dollars=4.605`, `stddev_log_daily_dollars=1.0`. Thresholds: budget=$5000, z_warn=2.0, z_crit=3.0.

| projected_daily_dollars | log-z | tier |
|---|---|---|
| $135 | 1.3 | OK |
| $400 | 2.4 | WARNING |
| $1,000 | 2.3 | WARNING |
| $2,800 | 3.3 | CRITICAL (via z) |
| $5,001+ | n/a | CRITICAL (via budget) |

Per-hour rates for the nodes in the fixture pricing:

| Node | Non-Photon $/hr | Photon $/hr |
|---|---|---|
| `m5.large` | $0.16 | — |
| `r5d.2xlarge` | $0.80 | $3.80 |
| `r5d.24xlarge` | $9.60 | $45.60 |
| `i3.4xlarge` | $1.00 | — (falls back to non-Photon) |
| `rd-fleet.2xlarge` | $0.80 | $3.80 (alias) |

Daily $ for uniform-type clusters: `(1 + num_workers) × $/hr × 24`.

## Adding a new scenario

1. Append a row to `inputs/clusters_live.csv` with a new `cluster_id`.
2. Append a row to `inputs/scenario_expected.csv` with the expected signal (A1's mapping).
3. Re-run the harness with `cleanup=no`.
4. Review `outputs/cluster_scores.csv` to confirm the new row scored correctly.
5. Copy `outputs/*.csv` to `expected/*.csv` and commit, including the new cluster's row.
