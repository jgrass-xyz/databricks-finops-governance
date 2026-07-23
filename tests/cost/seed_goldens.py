"""Pure-Python regenerator for tests/cost/fixtures/expected/*.csv.

Mirrors the test harness in `test_with_fixtures.py` but without Spark — reads
the same `fixtures/inputs/` CSVs, runs the same `_scoring`/`_routing`/`_alerting`
helpers, and writes the three golden CSVs the harness diffs against:

- cluster_scores.csv         (prior rows + new scored rows, schema-merged)
- event_log.csv              (prior rows + new send_log rows)
- cluster_alerts_latest.csv  (the SQL view materialized in Python)

The shared helpers (`_scoring`, `_routing`, `_alerting`) are pure Python so the
seeder is one source of truth with the harness for that logic. The two SQL
queries the harness runs (`cluster_alerts_latest_ddl` and `suppression_sql`)
are re-expressed below in Python — kept tight so drift is obvious.

Run from the repo root: `python tests/cost/seed_goldens.py`
"""

import fnmatch
import json
import os
import runpy
import sys
from datetime import datetime, timedelta

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))
SRC_DIR = os.path.join(REPO_ROOT, "src")
COST_DIR = os.path.join(SRC_DIR, "cost")
for d in (SRC_DIR, COST_DIR):
    if d not in sys.path:
        sys.path.insert(0, d)

import _scoring       # from src/cost/
import _alerting      # from src/cost/
import _routing       # from src/
# (_event_log/_owner_lookup/_slack not needed — no Spark, no network here)

FIXTURES_DIR = os.path.join(HERE, "fixtures")
INPUTS_DIR = os.path.join(FIXTURES_DIR, "inputs")
EXPECTED_DIR = os.path.join(FIXTURES_DIR, "expected")
os.makedirs(EXPECTED_DIR, exist_ok=True)

# Pinned harness parameters — must stay in sync with test_with_fixtures.py §1.
TEST_NOW_ISO = "2026-06-04T12:00:00+00:00"
NOW = datetime.fromisoformat(TEST_NOW_ISO)
WORKSPACE_ID = 9999999999999999
BUDGET = 5000.0
Z_WARN = 2.0
Z_CRIT = 3.0
WARN_HOURS = 24
CRIT_HOURS = 4
WORKSPACE_HOST = "example-workspace.cloud.databricks.com"
SLACK_CHANNEL = "#cost-alerts-test"  # stub-mode default in the harness

# Hard-coded record_ids keyed on cluster_id — keeps goldens byte-stable.
RECORD_IDS = {
    "c-budget-002":                    "00000000-0000-0000-0000-000000000001",
    "c-critical-nonphoton-budget-012": "00000000-0000-0000-0000-000000000002",
    "c-critical-nonphoton-z-011":      "00000000-0000-0000-0000-000000000003",
    "c-excluded-006":                  "00000000-0000-0000-0000-000000000004",
    "c-excluded-by-creator-014":       "00000000-0000-0000-0000-000000000005",
    "c-excluded-by-id-013":            "00000000-0000-0000-0000-000000000006",
    "c-fleet-007":                     "00000000-0000-0000-0000-000000000007",
    "c-normal-001":                    "00000000-0000-0000-0000-000000000008",
    "c-ok-photon-009":                 "00000000-0000-0000-0000-000000000009",
    "c-photon-mismatch-008":           "00000000-0000-0000-0000-000000000010",
    "c-unknown-005":                   "00000000-0000-0000-0000-000000000011",
    "c-warning-nonphoton-010":         "00000000-0000-0000-0000-000000000012",
    "c-zcrit-004":                     "00000000-0000-0000-0000-000000000013",
    "c-zstat-003":                     "00000000-0000-0000-0000-000000000014",
}


def _load_inputs():
    pricing = pd.read_csv(os.path.join(INPUTS_DIR, "pricing_rates.csv"))
    baseline = pd.read_csv(os.path.join(INPUTS_DIR, "workspace_baseline.csv"))
    clusters = pd.read_csv(os.path.join(INPUTS_DIR, "clusters_live.csv"))
    prior_scores = pd.read_csv(os.path.join(INPUTS_DIR, "cluster_scores_prior.csv"))
    prior_log = pd.read_csv(os.path.join(INPUTS_DIR, "event_log_prior.csv"))
    # Match harness §3: message_ts must round-trip as STRING (CHECK-referenced col).
    prior_log["message_ts"] = prior_log["message_ts"].astype(str)
    return pricing, baseline, clusters, prior_scores, prior_log


def _load_exclusions():
    mod = runpy.run_path(os.path.join(INPUTS_DIR, "excluded_clusters.py"))
    ids = set(mod.get("CLUSTER_IDS", []))
    patterns = list(mod.get("CLUSTER_NAME_PATTERNS", []))
    creators = set(mod.get("CREATORS", []))

    def exclusion_reason(cluster_id, cluster_name, creator):
        if cluster_id in ids:
            return f"cluster_id {cluster_id} in excluded_clusters.py"
        for pat in patterns:
            if cluster_name and fnmatch.fnmatch(cluster_name, pat):
                return f"cluster_name matches pattern '{pat}'"
        if creator in creators:
            return f"creator {creator} in excluded_clusters.py"
        return None

    return exclusion_reason


def _score_clusters(clusters_df, pricing_df, baseline_df, exclusion_reason):
    pricing = {
        (r["node_type"], bool(r["photon"])): (
            float(r["dbu_per_hour"]),
            float(r["dollars_per_hour"]),
            r["sku_name"],
        )
        for _, r in pricing_df.iterrows()
    }
    sku_by_photon = {bool(r["photon"]): r["sku_name"] for _, r in pricing_df.iterrows()}
    baseline = baseline_df.iloc[0].to_dict()
    thresholds = {"budget": BUDGET, "z_warning": Z_WARN, "z_critical": Z_CRIT}

    cluster_dicts = clusters_df.to_dict(orient="records")
    cluster_dicts.sort(key=lambda d: d["cluster_id"])
    for d in cluster_dicts:
        d["num_workers"] = int(d["num_workers"])
        d["photon"] = bool(d["photon"]) if not isinstance(d["photon"], bool) else d["photon"]

    scored = [
        _scoring.score_cluster(
            facts, pricing, sku_by_photon, baseline, thresholds,
            exclusion_reason, NOW, WORKSPACE_ID,
        )
        for facts in cluster_dicts
    ]
    for s in scored:
        s["record_id"] = RECORD_IDS[s["cluster_id"]]
    return scored


def _build_cluster_scores_csv(prior_df, scored):
    """Schema-merge mimic of harness §6: prior cols first, record_id appended last."""
    prior_cols = list(prior_df.columns)
    assert "record_id" not in prior_cols, "prior fixture leaked record_id — re-check CSV"
    final_cols = prior_cols + ["record_id"]

    new_df = pd.DataFrame(scored)
    # Reorder to match the merged schema (prior order + record_id at end).
    new_df = new_df[final_cols]
    prior_with_rid = prior_df.copy()
    prior_with_rid["record_id"] = None  # NULL for prior rows (harness asserts this in A4)

    combined = pd.concat([prior_with_rid, new_df], ignore_index=True)
    return combined


def _materialize_alerts_latest(scores_df):
    """Python equivalent of `_alerting.cluster_alerts_latest_ddl(...)`.

    SQL:  row_number() over (partition by workspace_id, cluster_id order by snapshot_ts desc) rn
          where rn=1 and signal in (WARNING, CRITICAL)
                and snapshot_ts >= NOW - INTERVAL 30 MINUTES
    """
    df = scores_df.copy()
    df["snapshot_ts"] = pd.to_datetime(df["snapshot_ts"], utc=True)
    df = df.sort_values(
        by=["workspace_id", "cluster_id", "snapshot_ts"],
        ascending=[True, True, False],
    )
    latest = df.drop_duplicates(subset=["workspace_id", "cluster_id"], keep="first")
    cutoff = pd.Timestamp(NOW) - timedelta(minutes=30)
    latest = latest[latest["signal"].isin(["WARNING", "CRITICAL"])]
    latest = latest[latest["snapshot_ts"] >= cutoff]
    view_cols = [
        "record_id", "workspace_id", "snapshot_ts", "cluster_id", "cluster_name",
        "creator", "node_type", "num_workers", "photon", "sku_name",
        "dbu_per_hour", "dollars_per_hour", "projected_daily_dollars",
        "z_score", "signal", "reason",
    ]
    return latest[view_cols].reset_index(drop=True)


def _apply_suppression(alerts_latest_df, prior_log_df):
    """Python equivalent of `_alerting.suppression_sql(...)`.

    Anti-join against prior `message_sent` events per (asset_id, severity);
    cooldown windows from WARN_HOURS / CRIT_HOURS.
    """
    sent = prior_log_df[
        (prior_log_df["workspace_id"] == WORKSPACE_ID)
        & (prior_log_df["detector_name"] == _alerting.DETECTOR_NAME)
        & (prior_log_df["event_type"] == "message_sent")
        & (prior_log_df["severity"].isin(["WARNING", "CRITICAL"]))
    ].copy()
    sent["event_ts"] = pd.to_datetime(sent["event_ts"], utc=True)
    last_send = (
        sent.groupby(["asset_id", "severity"])["event_ts"].max().reset_index()
        .rename(columns={"asset_id": "cluster_id", "event_ts": "last_sent_at",
                         "severity": "signal"})
    )

    a = alerts_latest_df[alerts_latest_df["workspace_id"] == WORKSPACE_ID].merge(
        last_send, on=["cluster_id", "signal"], how="left"
    )
    now_ts = pd.Timestamp(NOW)
    warn_cutoff = now_ts - timedelta(hours=WARN_HOURS)
    crit_cutoff = now_ts - timedelta(hours=CRIT_HOURS)
    keep = (
        a["last_sent_at"].isna()
        | ((a["signal"] == "CRITICAL") & (a["last_sent_at"] < crit_cutoff))
        | ((a["signal"] == "WARNING") & (a["last_sent_at"] < warn_cutoff))
    )
    to_send = a[keep].drop(columns=["last_sent_at"]).sort_values("cluster_id")
    return to_send.reset_index(drop=True)


def _build_send_log(to_send_df, baseline_df):
    """Mirror harness §10 but with deterministic stub Slack timestamps."""
    base_mean = float(baseline_df.iloc[0].get("mean_daily_dollars") or 0.0)
    rows = []
    for i, row in enumerate(to_send_df.to_dict(orient="records"), start=1):
        creator = (row["creator"] or "").strip() or None
        projected = row["projected_daily_dollars"]
        formatted_dollars = _routing.format_dollars(projected)
        formatted_percent = _alerting.format_pct_above_baseline(projected, base_mean)
        blocks = _routing.build_cost_alert_blocks(
            recipient=SLACK_CHANNEL,
            cluster_name=row["cluster_name"],
            cluster_id=row["cluster_id"],
            formatted_dollars=formatted_dollars,
            formatted_percent=formatted_percent,
            severity=row["signal"],
            creator=creator,
            workspace_host=WORKSPACE_HOST,
            creator_user_id=None,
        )
        plain = _alerting.format_plain_text(
            row["signal"], row["cluster_name"], formatted_dollars, formatted_percent,
        )
        stub_ts = f"stub-ts-{i:08d}"
        rows.append(_alerting.build_event_log_row(
            alert_row=row, now=NOW, workspace_id=WORKSPACE_ID,
            recipient=SLACK_CHANNEL, routing="channel_only",
            message_ts=stub_ts, plain=plain, blocks=blocks,
        ))
    return rows


def _build_event_log_csv(prior_log_df, send_log_new):
    log_cols = [
        "event_ts", "event_type", "detector_name", "workspace_id", "record_id",
        "asset_type", "asset_id", "asset_name", "severity",
        "recipient", "routing", "message_ts", "plain", "blocks", "details_json",
    ]
    prior = prior_log_df[log_cols].copy()
    new_df = pd.DataFrame(send_log_new)[log_cols] if send_log_new else pd.DataFrame(columns=log_cols)
    return pd.concat([prior, new_df], ignore_index=True)


def _dump(name, df):
    """Mirror harness `_dump`: sort by every column, write to expected/<name>."""
    pdf = df.sort_values(by=sorted(df.columns)).reset_index(drop=True)
    out = os.path.join(EXPECTED_DIR, name)
    pdf.to_csv(out, index=False)
    print(f"wrote {out}  ({len(pdf)} rows)")


def main():
    pricing_df, baseline_df, clusters_df, prior_scores_df, prior_log_df = _load_inputs()
    exclusion_reason = _load_exclusions()
    scored = _score_clusters(clusters_df, pricing_df, baseline_df, exclusion_reason)

    cluster_scores_df = _build_cluster_scores_csv(prior_scores_df, scored)
    alerts_latest_df = _materialize_alerts_latest(cluster_scores_df)
    to_send_df = _apply_suppression(alerts_latest_df, prior_log_df)
    send_log_new = _build_send_log(to_send_df, baseline_df)
    event_log_df = _build_event_log_csv(prior_log_df, send_log_new)

    _dump("cluster_scores.csv", cluster_scores_df)
    _dump("event_log.csv", event_log_df)
    _dump("cluster_alerts_latest.csv", alerts_latest_df)
    print(f"seeded {EXPECTED_DIR}")


if __name__ == "__main__":
    main()
