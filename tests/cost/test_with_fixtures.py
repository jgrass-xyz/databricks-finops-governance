# Databricks notebook source
# MAGIC %md
# MAGIC # test_with_fixtures
# MAGIC
# MAGIC **Golden-file test harness for the cost detector.** Inputs in `./fixtures/inputs/`
# MAGIC drive scoring + suppression + routing through the same code paths prod uses
# MAGIC (`_scoring`, `_routing`, `_slack`, `_owner_lookup`, `_event_log`). After the run,
# MAGIC the actuals (written to `./fixtures/outputs/`) are diffed byte-for-byte against
# MAGIC the expected files in `./fixtures/expected/`. PASS = exact match.
# MAGIC
# MAGIC ## Determinism
# MAGIC
# MAGIC The harness pins every source of randomness/wall-clock that prod uses:
# MAGIC
# MAGIC - `test_now` widget — replaces `datetime.now()` and SQL `current_timestamp()`
# MAGIC   anywhere we substitute it. Fixtures use absolute timestamps anchored to it.
# MAGIC - `WORKSPACE_ID` and `_user_short` hard-coded so `TEST_SCHEMA` and `event_log`
# MAGIC   rows have predictable values regardless of who/where runs the test.
# MAGIC - `record_id` on each scored row is overwritten post-scoring from the
# MAGIC   `RECORD_IDS` mapping keyed on `cluster_id`.
# MAGIC
# MAGIC With these pinned, identical inputs produce identical outputs and the golden
# MAGIC diff catches any drift.
# MAGIC
# MAGIC ## Parameters
# MAGIC
# MAGIC | Widget | Default | Notes |
# MAGIC |---|---|---|
# MAGIC | `test_now` | `2026-06-04T12:00:00+00:00` | Frozen "now" for the entire run. Substituted into both Python and SQL. |
# MAGIC | `slack_mode` | `sandbox` | `stub` (no network) or `sandbox` (real send via `_slack.post`) |
# MAGIC | `slack_secret_scope` | `databricks-cost-alerts` | Only used when `slack_mode=sandbox` |
# MAGIC | `slack_secret_key` | `slack-bot-token` | Only used when `slack_mode=sandbox` |
# MAGIC | `slack_recipient` | `TEST_RECIPIENT` | `#channel`, channel ID, or member ID (`U…`) for DM |
# MAGIC | `cleanup` | `yes` | Drop the test schema at the end |
# MAGIC
# MAGIC ## Output
# MAGIC
# MAGIC Assertion summary table at the bottom. A-series = behavior checks; B-series =
# MAGIC event_log constraint checks; G-series = golden-file diffs. Any non-PASS = regression.
# MAGIC
# MAGIC ## Refreshing the goldens after an intentional change
# MAGIC
# MAGIC 1. Make the code change that legitimately alters the output.
# MAGIC 2. Run this harness with `cleanup=no`.
# MAGIC 3. Manually review the diff between `fixtures/outputs/*.csv` and `fixtures/expected/*.csv`.
# MAGIC 4. If the new output is correct, copy the outputs over: `cp fixtures/outputs/*.csv fixtures/expected/`.
# MAGIC 5. Commit the updated `expected/` files.

# COMMAND ----------

import itertools
import json
import os
import runpy
import sys
import uuid
import fnmatch
from datetime import datetime, timedelta
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType,
    IntegerType, TimestampType, LongType
)

# COMMAND ----------

dbutils.widgets.text("test_now", "2026-06-04T12:00:00+00:00")
dbutils.widgets.dropdown("slack_mode", "sandbox", ["stub", "sandbox"])
dbutils.widgets.text("slack_secret_scope", "databricks-cost-alerts")
dbutils.widgets.text("slack_secret_key", "slack-bot-token")
dbutils.widgets.text("slack_recipient", "TEST_RECIPIENT")
dbutils.widgets.dropdown("cleanup", "yes", ["yes", "no"])

WORKSPACE_ID = 9999999999999999
TEST_CATALOG = "main"
TEST_SCHEMA  = f"{TEST_CATALOG}.tripwire_test"

TEST_NOW_ISO = dbutils.widgets.get("test_now").strip()
NOW = datetime.fromisoformat(TEST_NOW_ISO)

# Production thresholds — pinned so log-z math against the fixture baseline is stable.
BUDGET = 5000.0
Z_WARN = 2.0
Z_CRIT = 3.0

SLACK_MODE = dbutils.widgets.get("slack_mode")
CLEANUP = dbutils.widgets.get("cleanup") == "yes"

_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

_notebook_dir = os.path.dirname("/Workspace" + _ctx.notebookPath().get())
FIXTURES_DIR = os.path.abspath(os.path.join(_notebook_dir, "fixtures"))
INPUTS_DIR   = os.path.join(FIXTURES_DIR, "inputs")
EXPECTED_DIR = os.path.join(FIXTURES_DIR, "expected")
OUTPUTS_DIR  = os.path.join(FIXTURES_DIR, "outputs")

print(f"test_now       = {TEST_NOW_ISO}")
print(f"workspace_id   = {WORKSPACE_ID}")
print(f"test_schema    = {TEST_SCHEMA}")
print(f"fixtures_dir   = {FIXTURES_DIR}")
print(f"slack_mode     = {SLACK_MODE}")

# COMMAND ----------

# MAGIC %md ## §2. Import shared helpers
# MAGIC
# MAGIC Same `_scoring` / `_routing` / `_slack` / `_owner_lookup` / `_event_log` the
# MAGIC prod cost notebooks import. Reuse is what makes this a meaningful regression suite.

# COMMAND ----------

_tests_dir = os.path.dirname(_notebook_dir)             # .../tests
_repo_root = os.path.dirname(_tests_dir)                # .../
_src_dir   = os.path.join(_repo_root, "src")            # .../src
_cost_dir  = os.path.join(_src_dir, "cost")             # .../src/cost
for d in (_src_dir, _cost_dir):
    if d not in sys.path:
        sys.path.insert(0, d)
import _scoring       # noqa: E402  — from src/cost/
import _alerting        # noqa: E402  — from src/cost/
import _routing       # noqa: E402  — from src/
import _slack         # noqa: E402  — from src/
import _owner_lookup  # noqa: E402  — from src/
import _event_log     # noqa: E402  — from src/

# COMMAND ----------

# MAGIC %md ## §3. Set up the test schema + load input fixtures

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {TEST_CATALOG}")
spark.sql(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE")
spark.sql(f"CREATE SCHEMA {TEST_SCHEMA}")
print(f"created fresh schema {TEST_SCHEMA}")

# COMMAND ----------

def _read_input_csv(name):
    """Read a CSV from fixtures/inputs/ — these are the test author's deterministic inputs."""
    path = f"{INPUTS_DIR}/{name}"
    return spark.read.option("header", True).option("inferSchema", True).csv(f"file:{path}")

pricing_df       = _read_input_csv("pricing_rates.csv")
baseline_df      = _read_input_csv("workspace_baseline.csv")
clusters_df      = _read_input_csv("clusters_live.csv")
prior_scores_raw = _read_input_csv("cluster_scores_prior.csv")
prior_log_raw    = _read_input_csv("event_log_prior.csv")
# inferSchema reads "1717000000.000001" as DOUBLE; force STRING so the append
# doesn't try to widen event_log.message_ts (a CHECK-referenced column).
prior_log_raw    = prior_log_raw.withColumn("message_ts", prior_log_raw["message_ts"].cast("string"))
expected_signals_df = _read_input_csv("scenario_expected.csv")

display(pricing_df)
display(baseline_df)
display(clusters_df)
display(prior_scores_raw)
display(prior_log_raw)
display(expected_signals_df)

# COMMAND ----------

# Exclusions config — mirrors how 01_monitor loads config/excluded_clusters.py in prod.
_excluded_path = os.path.join(INPUTS_DIR, "excluded_clusters.py")
_excluded_module = runpy.run_path(_excluded_path)
EXCLUDED_IDS = set(_excluded_module.get("CLUSTER_IDS", []))
EXCLUDED_NAME_PATTERNS = list(_excluded_module.get("CLUSTER_NAME_PATTERNS", []))
EXCLUDED_CREATORS = set(_excluded_module.get("CREATORS", []))
print(f"exclusions: ids={EXCLUDED_IDS}, patterns={EXCLUDED_NAME_PATTERNS}, creators={EXCLUDED_CREATORS}")

def exclusion_reason(cluster_id, cluster_name, creator):
    if cluster_id in EXCLUDED_IDS:
        return f"cluster_id {cluster_id} in excluded_clusters.py"
    for pat in EXCLUDED_NAME_PATTERNS:
        if cluster_name and fnmatch.fnmatch(cluster_name, pat):
            return f"cluster_name matches pattern '{pat}'"
    if creator in EXCLUDED_CREATORS:
        return f"creator {creator} in excluded_clusters.py"
    return None

# COMMAND ----------

# MAGIC %md ## §4. Build cluster_facts dicts

# COMMAND ----------

cluster_dicts = [r.asDict() for r in clusters_df.collect()]
cluster_dicts.sort(key=lambda d: d["cluster_id"])
for d in cluster_dicts:
    d["num_workers"] = int(d["num_workers"])
    d["photon"] = bool(d["photon"]) if not isinstance(d["photon"], bool) else d["photon"]

# COMMAND ----------

# MAGIC %md ## §5. Scoring
# MAGIC
# MAGIC `score_cluster()` generates a random UUID per row. After scoring, we overwrite
# MAGIC each row's `record_id` with a hard-coded value keyed on `cluster_id` so golden
# MAGIC diffs are byte-stable.

# COMMAND ----------

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


pricing_rows = pricing_df.collect()
pricing = {
    (r["node_type"], bool(r["photon"])): (
        float(r["dbu_per_hour"]),
        float(r["dollars_per_hour"]),
        r["sku_name"],
    )
    for r in pricing_rows
}
sku_by_photon = {bool(r["photon"]): r["sku_name"] for r in pricing_rows}

baseline = baseline_df.collect()[0].asDict()
thresholds = {"budget": BUDGET, "z_warning": Z_WARN, "z_critical": Z_CRIT}

scored = [
    _scoring.score_cluster(
        facts, pricing, sku_by_photon, baseline, thresholds,
        exclusion_reason, NOW, WORKSPACE_ID,
    )
    for facts in cluster_dicts
]
for s in scored:
    s["record_id"] = RECORD_IDS[s["cluster_id"]]

display(spark.createDataFrame(scored))

# COMMAND ----------

# MAGIC %md ## §6. Schema-evolution probe + materialize cluster_scores

# COMMAND ----------

# Prior rows use absolute timestamps in the fixture; just materialize as-is.
prior_count = prior_scores_raw.count()
assert "record_id" not in prior_scores_raw.columns, "prior fixture leaked record_id — re-check CSV"

(prior_scores_raw.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(f"{TEST_SCHEMA}.cluster_scores"))
print(f"seeded cluster_scores with {prior_count} prior rows (pre-record_id schema)")

# Append new scored rows with mergeSchema=true exactly as prod does.
new_schema = StructType([
    StructField("record_id", StringType()),
    StructField("workspace_id", LongType()),
    StructField("snapshot_ts", TimestampType()),
    StructField("cluster_id", StringType()),
    StructField("cluster_name", StringType()),
    StructField("creator", StringType()),
    StructField("cluster_source", StringType()),
    StructField("node_type", StringType()),
    StructField("driver_node_type", StringType()),
    StructField("num_workers", IntegerType()),
    StructField("photon", BooleanType()),
    StructField("sku_name", StringType()),
    StructField("dbu_per_hour", DoubleType()),
    StructField("dollars_per_hour", DoubleType()),
    StructField("projected_daily_dollars", DoubleType()),
    StructField("baseline_mean", DoubleType()),
    StructField("baseline_stddev", DoubleType()),
    StructField("z_score", DoubleType()),
    StructField("signal", StringType()),
    StructField("reason", StringType()),
])
new_df = spark.createDataFrame(scored, schema=new_schema)
(new_df.write
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{TEST_SCHEMA}.cluster_scores"))
print(f"appended {new_df.count()} new rows with mergeSchema=true")

# COMMAND ----------

# MAGIC %md ## §7. cluster_alerts_latest view — frozen `test_now` substituted for `current_timestamp()`

# COMMAND ----------

spark.sql(_alerting.cluster_alerts_latest_ddl(TEST_SCHEMA, now_sql=f"TIMESTAMP '{TEST_NOW_ISO}'"))
view_cols = [r["col_name"] for r in spark.sql(f"DESCRIBE TABLE {TEST_SCHEMA}.cluster_alerts_latest").collect()]
VIEW_HAS_RECORD_ID = "record_id" in view_cols
print(f"cluster_alerts_latest columns: {view_cols}")

# COMMAND ----------

# MAGIC %md ## §8. Materialize event_log via shared `_event_log.ensure()` + seed prior rows
# MAGIC
# MAGIC `_event_log.ensure()` is the same code prod's `00_baseline_refresh` calls, so this
# MAGIC verifies the table comes up with the NOT NULL + CHECK constraints. B1-B3 below
# MAGIC assert the constraints are present and reject malformed writes.

# COMMAND ----------

_event_log.ensure(spark, f"{TEST_SCHEMA}.event_log")
print("event_log created via _event_log.ensure()")

# Seed prior rows from fixture (already have absolute event_ts — just load).
prior_log_count = prior_log_raw.count()
log_columns = [
    "event_ts", "event_type", "detector_name", "workspace_id", "record_id",
    "asset_type", "asset_id", "asset_name", "severity",
    "recipient", "routing", "message_ts", "plain", "blocks", "details_json",
]
(prior_log_raw.select(*log_columns).write
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{TEST_SCHEMA}.event_log"))
print(f"event_log seeded with {prior_log_count} prior rows")

# COMMAND ----------

# MAGIC %md ## §9. Suppression — frozen `test_now` substituted for `current_timestamp()`

# COMMAND ----------

WARN_HOURS = 24
CRIT_HOURS = 4
to_send = spark.sql(_alerting.suppression_sql(
    TEST_SCHEMA, WORKSPACE_ID, WARN_HOURS, CRIT_HOURS,
    now_sql=f"TIMESTAMP '{TEST_NOW_ISO}'",
)).collect()
print(f"{len(to_send)} row(s) survive suppression")
for r in to_send:
    print(f"  → {r['cluster_id']} {r['signal']}")

# COMMAND ----------

# MAGIC %md ## §10. Render + send (real Slack via `_slack.post` in sandbox mode)

# COMMAND ----------

WORKSPACE_HOST = "example-workspace.cloud.databricks.com"

SLACK_TOKEN = None
SLACK_CHANNEL = "#cost-alerts-test"  # stub default
if SLACK_MODE == "sandbox":
    SLACK_TOKEN = dbutils.secrets.get(
        scope=dbutils.widgets.get("slack_secret_scope"),
        key=dbutils.widgets.get("slack_secret_key"),
    )
    SLACK_CHANNEL = dbutils.widgets.get("slack_recipient").strip()
    if not SLACK_CHANNEL:
        raise RuntimeError(
            "slack_mode=sandbox requires slack_recipient widget to be set "
            "(#channel, channel ID, or your member ID like TEST_MEMBER_ID)"
        )
    # Verify token early — same auth.test prod does in 03_slack_alerting.
    auth = _slack.post(SLACK_TOKEN, "auth.test", {})
    print(f"Authed as bot: {auth['user']} in team {auth['team']}")
    print(f"slack_recipient = {SLACK_CHANNEL!r}")

_stub_counter = itertools.count(1)
def _send_or_stub(plain, blocks, channel):
    """Real Slack send via _slack.post in sandbox; deterministic stub ts otherwise.

    Uses the same _slack.post() call that prod's 03_slack_alerting uses, so
    network-side regressions show up here too.
    """
    if SLACK_MODE == "sandbox":
        resp = _slack.post(SLACK_TOKEN, "chat.postMessage", {
            "channel": channel, "text": plain, "blocks": blocks,
            "unfurl_links": False, "unfurl_media": False,
        })
        return resp["ts"], resp["channel"]
    return f"stub-ts-{next(_stub_counter):08d}", channel

send_log_new = []
base_mean = float(baseline.get("mean_daily_dollars") or 0.0)
for row in to_send:
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
    plain = _alerting.format_plain_text(row["signal"], row["cluster_name"], formatted_dollars, formatted_percent)
    ts, _channel = _send_or_stub(plain, blocks, SLACK_CHANNEL)

    send_log_new.append(_alerting.build_event_log_row(
        alert_row=row, now=NOW, workspace_id=WORKSPACE_ID,
        recipient=SLACK_CHANNEL, routing="channel_only",
        message_ts=ts, plain=plain, blocks=blocks,
    ))

print(f"send_log_new rows: {len(send_log_new)}")

# COMMAND ----------

# Persist sends to event_log so downstream queries see them.
if send_log_new:
    sent_schema = StructType([
        StructField("event_ts", TimestampType()),
        StructField("event_type", StringType()),
        StructField("detector_name", StringType()),
        StructField("workspace_id", LongType()),
        StructField("record_id", StringType()),
        StructField("asset_type", StringType()),
        StructField("asset_id", StringType()),
        StructField("asset_name", StringType()),
        StructField("severity", StringType()),
        StructField("recipient", StringType()),
        StructField("routing", StringType()),
        StructField("message_ts", StringType()),
        StructField("plain", StringType()),
        StructField("blocks", StringType()),
        StructField("details_json", StringType()),
    ])
    sent_df = spark.createDataFrame(send_log_new, schema=sent_schema)
    (sent_df.write.mode("append").option("mergeSchema", "true")
        .saveAsTable(f"{TEST_SCHEMA}.event_log"))

# COMMAND ----------

# MAGIC %md ## §11. Write actuals to fixtures/outputs/ for diffing

# COMMAND ----------

import pandas as pd

os.makedirs(OUTPUTS_DIR, exist_ok=True)

def _dump(name, df):
    """Write a Spark DataFrame to fixtures/outputs/<name>, sorted for stable diffs."""
    out = os.path.join(OUTPUTS_DIR, name)
    pdf = df.toPandas()
    # Sort by every column for a fully stable byte-comparable output.
    pdf = pdf.sort_values(by=sorted(pdf.columns)).reset_index(drop=True)
    pdf.to_csv(out, index=False)
    print(f"wrote {out}  ({len(pdf)} rows)")

# Drop volatile/unstable bytes from the diffable views so byte comparison works:
# - blocks/plain/details_json are deterministic but huge; keep them
# - message_ts is fine because we stub deterministically
# Note: cluster_alerts_latest is a view; query it as a snapshot.
cluster_scores_now = spark.sql(
    f"SELECT * FROM {TEST_SCHEMA}.cluster_scores"
)
event_log_now = spark.sql(
    f"SELECT * FROM {TEST_SCHEMA}.event_log"
)
alerts_latest_now = spark.sql(
    f"SELECT * FROM {TEST_SCHEMA}.cluster_alerts_latest"
)

_dump("cluster_scores.csv", cluster_scores_now)
_dump("event_log.csv", event_log_now)
_dump("cluster_alerts_latest.csv", alerts_latest_now)

# COMMAND ----------

# MAGIC %md ## §12. Assertions
# MAGIC
# MAGIC - **A-series**: behavior — scoring, schema evolution, send-log shape
# MAGIC - **B-series**: event_log constraints from `_event_log.ensure()`
# MAGIC - **G-series**: golden-file diffs of full table contents

# COMMAND ----------

results = []  # list of (id, status, detail)

def check(aid, ok, detail=""):
    results.append((aid, "PASS" if ok else "FAIL", detail))

# --- A1: scoring matches scenario_expected.csv -------------------------------
expected_map = {r["cluster_id"]: r["expected_signal"] for r in expected_signals_df.collect()}
mismatches = [
    f"{s['cluster_id']}: expected={expected_map.get(s['cluster_id'])} got={s['signal']}"
    for s in scored if expected_map.get(s["cluster_id"]) != s["signal"]
]
check("A1", not mismatches, "; ".join(mismatches) if mismatches else f"{len(scored)} scenarios all matched")

# --- A2: every scored row has a record_id ------------------------------------
missing_rid = [s for s in scored if not s.get("record_id")]
check("A2", not missing_rid, f"{len(missing_rid)} scored rows missing record_id" if missing_rid else "all rows carry record_id")

# --- A3: record_ids are unique within the run --------------------------------
rid_counts = {}
for s in scored:
    rid_counts[s["record_id"]] = rid_counts.get(s["record_id"], 0) + 1
dupes = [r for r, n in rid_counts.items() if n > 1]
check("A3", not dupes, f"{len(dupes)} duplicate record_ids" if dupes else f"{len(rid_counts)} unique record_ids")

# --- A4: schema evolution materialized correctly -----------------------------
post_rid_nulls = spark.sql(
    f"SELECT count(*) AS n FROM {TEST_SCHEMA}.cluster_scores WHERE record_id IS NULL"
).collect()[0]["n"]
post_rid_set = spark.sql(
    f"SELECT count(*) AS n FROM {TEST_SCHEMA}.cluster_scores WHERE record_id IS NOT NULL"
).collect()[0]["n"]
a4_ok = post_rid_nulls == prior_count and post_rid_set == len(scored)
check("A4", a4_ok,
      f"NULLs={post_rid_nulls} (expected {prior_count}); populated={post_rid_set} (expected {len(scored)})")

# --- A5: cluster_alerts_latest view exposes record_id ------------------------
check("A5", VIEW_HAS_RECORD_ID,
      "record_id present" if VIEW_HAS_RECORD_ID
      else "view dropped record_id — 03_slack_alerting will KeyError on row['record_id']")

# --- A6: every send_log row joins back to cluster_scores via record_id -------
if not send_log_new:
    check("A6", True, "no sends issued (vacuously true)")
else:
    rid_list = ", ".join(f"'{r['record_id']}'" for r in send_log_new)
    join_check = spark.sql(f"""
        SELECT s.record_id, count(*) AS n
        FROM {TEST_SCHEMA}.cluster_scores s
        WHERE s.record_id IN ({rid_list})
        GROUP BY s.record_id
        HAVING count(*) != 1
    """).collect()
    check("A6", not join_check, f"{len(join_check)} record_ids did not join 1:1" if join_check else f"{len(send_log_new)} sends joined cleanly")

# --- A7: rendered Block Kit is well-formed -----------------------------------
a7_problems = []
for r in send_log_new:
    try:
        blocks = json.loads(r["blocks"])
    except json.JSONDecodeError as e:
        a7_problems.append(f"{r['asset_id']}: blocks not valid JSON ({e})")
        continue
    if not blocks or blocks[0].get("type") != "header":
        a7_problems.append(f"{r['asset_id']}: first block is not a header")
    button_url = None
    for b in blocks:
        if b.get("type") == "actions":
            for el in b.get("elements", []):
                if el.get("type") == "button":
                    button_url = el.get("url", "")
    if button_url is None:
        a7_problems.append(f"{r['asset_id']}: no actions button")
    elif r["asset_id"] not in button_url:
        a7_problems.append(f"{r['asset_id']}: button URL missing cluster_id")
check("A7", not a7_problems, "; ".join(a7_problems) if a7_problems else f"all {len(send_log_new)} payloads valid")

# --- A8: suppression — fixture has c-zstat-003 sent 1h ago, c-zcrit-004 sent 5h ago.
# With test_now frozen, c-zstat-003 WARNING must be suppressed (24h window > 1h),
# c-zcrit-004 CRITICAL must NOT be suppressed (4h window < 5h),
# c-budget-002 CRITICAL has no prior so must fire.
sent_pairs = {(r["asset_id"], r["severity"]) for r in send_log_new}
a8_problems = []
if ("c-zstat-003", "WARNING") in sent_pairs:
    a8_problems.append("c-zstat-003/WARNING should have been suppressed (last_sent=1h ago)")
if ("c-zcrit-004", "CRITICAL") not in sent_pairs:
    a8_problems.append("c-zcrit-004/CRITICAL should NOT have been suppressed (last_sent=5h ago, window=4h)")
if ("c-budget-002", "CRITICAL") not in sent_pairs:
    a8_problems.append("c-budget-002/CRITICAL has no prior and should fire")
check("A8", not a8_problems, "; ".join(a8_problems) if a8_problems else "suppression behaved as designed")

# --- A9: recipient is the pre-resolution channel/user string -----------------
if not send_log_new:
    check("A9", True, "no sends (vacuously true)")
else:
    mismatch = [r for r in send_log_new if r["recipient"] != SLACK_CHANNEL]
    check("A9", not mismatch, f"{len(mismatch)} recipients didn't match {SLACK_CHANNEL!r}" if mismatch else f"all recipients = {SLACK_CHANNEL!r}")

# COMMAND ----------

# MAGIC %md ### B-series: event_log constraints

# COMMAND ----------

constraint_props = {
    row["key"]: row["value"]
    for row in spark.sql(f"SHOW TBLPROPERTIES {TEST_SCHEMA}.event_log").collect()
    if row["key"].startswith("delta.constraints.")
}

# --- B1: event_type_recognized constraint present ----------------------------
check("B1", "delta.constraints.event_type_recognized" in constraint_props,
      "constraint present" if "delta.constraints.event_type_recognized" in constraint_props
      else "constraint missing — _event_log.ensure() did not run")

# --- B2: at least one message_*_complete constraint present ------------------
complete_constraints = [k for k in constraint_props if k.endswith("_complete")]
check("B2", bool(complete_constraints),
      f"present: {sorted(k.rsplit('.', 1)[-1] for k in complete_constraints)}" if complete_constraints
      else "no *_complete constraints found")

# --- B3: malformed write is rejected at insert time --------------------------
# Try to insert an obviously-bad row (event_type='msg_sent' — not in allowed set).
# We expect Delta to raise a constraint violation; B3 PASSes when the write fails.
b3_rejected = False
b3_detail = ""
try:
    bad_row = spark.createDataFrame(
        [(NOW, "msg_sent", "cluster_cost", WORKSPACE_ID,
          "bad-rid", "cluster", "c-bad", "bad-asset", "WARNING",
          "#x", "channel_only", "t", "p", "[]", "{}")],
        schema=sent_schema,
    )
    bad_row.write.mode("append").saveAsTable(f"{TEST_SCHEMA}.event_log")
    b3_detail = "Delta accepted event_type='msg_sent' — event_type_recognized constraint is not enforcing"
except Exception as e:
    b3_rejected = True
    b3_detail = f"correctly rejected: {type(e).__name__}"
check("B3", b3_rejected, b3_detail)

# --- B4: every sent row tags detector_name='cluster_cost' --------------------
wrong_detector = [r for r in send_log_new if r["detector_name"] != "cluster_cost"]
check("B4", not wrong_detector,
      f"{len(wrong_detector)} rows with wrong detector_name" if wrong_detector
      else f"all {len(send_log_new)} rows tagged cluster_cost")

# --- B5: every sent row tags asset_type='cluster' ----------------------------
wrong_asset_type = [r for r in send_log_new if r["asset_type"] != "cluster"]
check("B5", not wrong_asset_type,
      f"{len(wrong_asset_type)} rows with wrong asset_type" if wrong_asset_type
      else f"all {len(send_log_new)} rows tagged cluster")

# COMMAND ----------

# MAGIC %md ### G-series: golden-file diff
# MAGIC
# MAGIC Each diff loads `fixtures/expected/<table>.csv` and compares to the just-written
# MAGIC `fixtures/outputs/<table>.csv` byte-for-byte. If expected/ is empty (first-run
# MAGIC bootstrap), the assertion is informational only and tells you to seed expected/
# MAGIC from outputs/ after manual review.

# COMMAND ----------

def _golden_diff(name):
    """Return (status_str, detail) for the diff of outputs/<name> vs expected/<name>."""
    out_path = os.path.join(OUTPUTS_DIR, name)
    exp_path = os.path.join(EXPECTED_DIR, name)
    if not os.path.exists(exp_path):
        return ("BOOTSTRAP", f"no expected file at {exp_path} — copy outputs/{name} to expected/{name} after review")
    out_text = open(out_path).read()
    exp_text = open(exp_path).read()
    if out_text == exp_text:
        return ("PASS", f"byte-identical ({len(out_text)} bytes)")
    # Find the first diff line for a quick hint.
    out_lines = out_text.splitlines()
    exp_lines = exp_text.splitlines()
    for i, (o, e) in enumerate(zip(out_lines, exp_lines), start=1):
        if o != e:
            return ("FAIL", f"line {i} differs: expected={e!r} actual={o!r}")
    if len(out_lines) != len(exp_lines):
        return ("FAIL", f"line-count differs: expected={len(exp_lines)} actual={len(out_lines)}")
    return ("FAIL", "files differ but no line-level diff found (whitespace?)")

for name, label in [
    ("cluster_scores.csv", "G1"),
    ("event_log.csv", "G2"),
    ("cluster_alerts_latest.csv", "G3"),
]:
    status, detail = _golden_diff(name)
    if status == "BOOTSTRAP":
        # Mark as PASS so first-run doesn't fail the whole suite, but the detail
        # text tells the user to seed expected/.
        results.append((label, "BOOTSTRAP", detail))
    else:
        check(label, status == "PASS", detail)

# COMMAND ----------

# MAGIC %md ## §13. Summary + cleanup

# COMMAND ----------

print("\n=========== test_with_fixtures summary ===========")
print(f"{'ID':<5} {'STATUS':<11} DETAIL")
print("-" * 80)
any_failed = False
for aid, status, detail in results:
    print(f"{aid:<5} {status:<11} {detail}")
    if status == "FAIL":
        any_failed = True
print("-" * 80)
print(f"slack_mode={SLACK_MODE}, sends={len(send_log_new)}, schema={TEST_SCHEMA}")
print(f"test_now={TEST_NOW_ISO}, workspace_id={WORKSPACE_ID}")

if CLEANUP:
    spark.sql(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE")
    print(f"dropped {TEST_SCHEMA}")
else:
    print(f"kept {TEST_SCHEMA} for inspection (set cleanup=yes to drop)")

if any_failed:
    raise RuntimeError("test_with_fixtures: one or more assertions FAILED — see summary above")
print("test_with_fixtures: OK")
