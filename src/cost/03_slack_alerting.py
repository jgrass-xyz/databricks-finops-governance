# Databricks notebook source
# MAGIC %md
# MAGIC # 03_slack_alerting
# MAGIC
# MAGIC **Notebook Summary:** Reads `cluster_alerts_latest`, applies per-(cluster, severity) suppression against `event_log` (filtered to `detector_name='cluster_cost'`), and sends a Slack message for each remaining flagged cluster. Two routing modes:
# MAGIC
# MAGIC - **`channel_only`** *(default during testing)* — every alert posts to a single configurable channel. Safe to run before owner-DM behavior has been validated; doesn't surprise end users.
# MAGIC - **`dm_with_fallback`** — DM the cluster owner via `users.lookupByEmail`; fall back to the same channel if the email doesn't match a Slack user.
# MAGIC
# MAGIC Runs every 30 minutes. Pairs with `01_monitor` (15 min): worst-case detection-to-DM latency is ~30 min. Re-nudge cadence is governed by suppression windows, not the schedule.
# MAGIC
# MAGIC Depends on `00_baseline_refresh` having run at least once (creates `event_log` and the `cluster_alerts_latest` view).
# MAGIC
# MAGIC ## Parameters
# MAGIC
# MAGIC | Widget | Default | What it controls |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `main` | UC catalog where the tables live. Must match `00_baseline_refresh`. |
# MAGIC | `schema` | `finops_observability` | UC schema under `catalog`. Must match `00_baseline_refresh`. |
# MAGIC | `secret_scope` | `databricks-cost-alerts` | Databricks secret scope holding the Slack bot token. |
# MAGIC | `secret_key` | `slack-bot-token` | Key inside the scope holding the `xoxb-...` token. |
# MAGIC | `routing_mode` | `channel_only` | `channel_only` posts every alert to `alert_channel`. `dm_with_fallback` DMs cluster owners and falls back to `alert_channel`. |
# MAGIC | `alert_channel` | *(empty — must be set)* | Channel name (`#name`) or ID (`C0...`). Primary destination in `channel_only` mode; fallback in `dm_with_fallback` mode. Bot must be invited unless it has `chat:write.public`. |
# MAGIC | `tee_to_channel` | `true` | In `dm_with_fallback`, also post a copy of each owner-DMed alert to `alert_channel` so there's a central feed of everything sent. `false` = DM only. No effect in `channel_only`. |
# MAGIC | `warning_suppression_hours` | `24` | Hours to suppress repeat WARNING messages for the same cluster. |
# MAGIC | `critical_suppression_hours` | `4` | Hours to suppress repeat CRITICAL messages for the same cluster. |
# MAGIC | `workspace_host` | *(empty — auto-detect)* | Workspace host (e.g. `example-workspace.cloud.databricks.com`) used to build cluster deep-link URLs. Leave empty to auto-detect from runtime context; set explicitly when auto-detection is wrong (PrivateLink, custom domains) or when you want a specific host. |
# MAGIC
# MAGIC ## Required bot scopes
# MAGIC
# MAGIC `chat:write`, `users:read`, `users:read.email`, `channels:read`, `groups:read`, `im:read`, `mpim:read`. Optionally `chat:write.public` to post to channels without invite.

# COMMAND ----------

import os
import sys
import json
from urllib.parse import urlparse
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
from datetime import datetime, timezone

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "finops_observability")
dbutils.widgets.text("secret_scope", "databricks-cost-alerts")
dbutils.widgets.text("secret_key", "slack-bot-token")
dbutils.widgets.dropdown("routing_mode", "channel_only", ["channel_only", "dm_with_fallback"])
dbutils.widgets.text("alert_channel", "")
dbutils.widgets.dropdown("tee_to_channel", "true", ["true", "false"])
dbutils.widgets.text("warning_suppression_hours", "24")
dbutils.widgets.text("critical_suppression_hours", "4")
dbutils.widgets.text("workspace_host", "")

CATALOG        = dbutils.widgets.get("catalog")
SCHEMA         = dbutils.widgets.get("schema")
SCOPE          = dbutils.widgets.get("secret_scope")
KEY            = dbutils.widgets.get("secret_key")
ROUTING_MODE   = dbutils.widgets.get("routing_mode")
ALERT_CHANNEL  = dbutils.widgets.get("alert_channel").strip()
TEE_TO_CHANNEL = dbutils.widgets.get("tee_to_channel") == "true"
WARN_HOURS     = int(dbutils.widgets.get("warning_suppression_hours"))
CRIT_HOURS     = int(dbutils.widgets.get("critical_suppression_hours"))
HOST_OVERRIDE  = dbutils.widgets.get("workspace_host").strip()

if not ALERT_CHANNEL:
    raise RuntimeError(
        "alert_channel widget is empty — set it to the channel name (#cost-alerts) "
        "or ID (C0...) where alerts should land."
    )

CATALOG_SCHEMA = f"{CATALOG}.{SCHEMA}"
BASELINE_TABLE = f"{CATALOG_SCHEMA}.workspace_baseline"
EVENT_LOG_TABLE = f"{CATALOG_SCHEMA}.event_log"

_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
WORKSPACE_ID = int(_ctx.workspaceId().get())

# Resolve the workspace host used to build cluster deep-link URLs in alerts.
# Priority: explicit `workspace_host` widget > browserHostName (PrivateLink-correct
# but only populated in interactive context) > apiUrl as last resort.
if HOST_OVERRIDE:
    # Strip scheme if user pasted a full URL.
    WORKSPACE_HOST = urlparse(HOST_OVERRIDE).netloc if HOST_OVERRIDE.startswith("http") else HOST_OVERRIDE
else:
    try:
        WORKSPACE_HOST = _ctx.browserHostName().get()
    except Exception:
        WORKSPACE_HOST = None
    if not WORKSPACE_HOST:
        WORKSPACE_HOST = urlparse(_ctx.apiUrl().get()).netloc

print(
    f"workspace_id={WORKSPACE_ID}, host={WORKSPACE_HOST}, mode={ROUTING_MODE}, "
    f"channel={ALERT_CHANNEL}, warn_suppression={WARN_HOURS}h, crit_suppression={CRIT_HOURS}h"
)

# Import shared helpers: _routing (Block Kit + format helpers), _slack (Slack API
# wrapper), _owner_lookup (email → Slack user ID). All three live one directory up
# from this notebook (in src/, shared by every detector silo).
_notebook_dir = os.path.dirname("/Workspace" + _ctx.notebookPath().get())   # .../cost
_src_dir      = os.path.dirname(_notebook_dir)                              # .../src
for d in (_notebook_dir, _src_dir):
    if d not in sys.path:
        sys.path.insert(0, d)
import _routing
import _slack
import _alerting  # cost-silo helpers, shared with the test harness
from _owner_lookup import lookup_user_id

# COMMAND ----------

# MAGIC %md ## Slack auth check

# COMMAND ----------

TOKEN = dbutils.secrets.get(scope=SCOPE, key=KEY)

# Verify the token early so a misconfigured scope/secret fails loud instead of mid-loop.
auth = _slack.post(TOKEN, "auth.test", {})
print(f"Authed as bot: {auth['user']} in workspace: {auth['team']}  (bot_id={auth.get('bot_id')})")

# COMMAND ----------

# MAGIC %md ## Message rendering
# MAGIC
# MAGIC The Block Kit builder lives in `_routing.py` so the test harness can render
# MAGIC identical payloads. `format_dollars` is also imported from there.

# COMMAND ----------

format_dollars = _routing.format_dollars

# COMMAND ----------

# MAGIC %md ## Read alerts + apply suppression

# COMMAND ----------

to_send = spark.sql(
    _alerting.suppression_sql(CATALOG_SCHEMA, WORKSPACE_ID, WARN_HOURS, CRIT_HOURS)
).collect()
print(
    f"{len(to_send)} cluster row(s) to alert on after suppression "
    f"(WARNING={WARN_HOURS}h, CRITICAL={CRIT_HOURS}h)"
)

# COMMAND ----------

# MAGIC %md ## Read workspace baseline mean (for "% above average" framing)

# COMMAND ----------

# Most recent baseline row, not "today's" — `00_baseline_refresh` runs at 2 AM and
# we don't want this notebook to crash on early-morning runs that haven't seen a
# fresh baseline yet, or if the baseline job failed.
baseline_rows = spark.sql(
    f"SELECT mean_daily_dollars FROM {BASELINE_TABLE} ORDER BY computed_at DESC LIMIT 1"
).collect()
if not baseline_rows or baseline_rows[0]["mean_daily_dollars"] is None:
    print(
        f"WARNING: {BASELINE_TABLE} has no rows yet — '% above average' will show '?' "
        "until 00_baseline_refresh runs successfully."
    )
    BASELINE_MEAN = None
else:
    BASELINE_MEAN = float(baseline_rows[0]["mean_daily_dollars"])
    print(f"baseline mean: ${BASELINE_MEAN:,.2f}/day")

# COMMAND ----------

# MAGIC %md ## Send

# COMMAND ----------

now = datetime.now(timezone.utc)
send_log_rows = []

for row in to_send:
    cluster_id   = row["cluster_id"]
    cluster_name = row["cluster_name"]
    creator      = (row["creator"] or "").strip() or None
    projected    = row["projected_daily_dollars"]
    severity     = row["signal"]

    formatted_dollars = format_dollars(projected)
    formatted_percent = _alerting.format_pct_above_baseline(projected, BASELINE_MEAN)

    # Resolve owner's Slack ID once. Used for routing in dm_with_fallback AND for
    # @-mentioning the owner in channel mode so they get a real notification.
    creator_user_id = lookup_user_id(TOKEN, creator) if creator else None

    plain = _alerting.format_plain_text(severity, cluster_name, formatted_dollars, formatted_percent)

    # Fan out to every destination for this alert (owner DM, central channel, or
    # both — see resolve_send_targets). Each send is logged to event_log separately.
    targets = _alerting.resolve_send_targets(ROUTING_MODE, creator_user_id, ALERT_CHANNEL, TEE_TO_CHANNEL)
    if not creator_user_id and ROUTING_MODE == "dm_with_fallback":
        who = creator or "unowned cluster"
        print(f"  {who} → no Slack match, routing {cluster_name} to {ALERT_CHANNEL}")

    for recipient, routing in targets:
        blocks = _routing.build_cost_alert_blocks(
            recipient=recipient,
            cluster_name=cluster_name,
            cluster_id=cluster_id,
            formatted_dollars=formatted_dollars,
            formatted_percent=formatted_percent,
            severity=severity,
            creator=creator,
            workspace_host=WORKSPACE_HOST,
            creator_user_id=creator_user_id,
        )
        resp = _slack.post(TOKEN, "chat.postMessage", {
            "channel": recipient,
            "text": plain,
            "blocks": blocks,
            "unfurl_links": False,
            "unfurl_media": False,
        })
        print(f"  → posted {severity} alert for {cluster_name} via {routing} (channel={resp['channel']}, ts={resp['ts']})")

        send_log_rows.append(
            _alerting.build_event_log_row(
                alert_row=row,
                now=now,
                workspace_id=WORKSPACE_ID,
                recipient=recipient,
                routing=routing,
                message_ts=resp["ts"],
                plain=plain,
                blocks=blocks,
            )
        )

# COMMAND ----------

# MAGIC %md ## Persist sends to `event_log`

# COMMAND ----------

if send_log_rows:
    log_schema = StructType([
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
    log_df = spark.createDataFrame(send_log_rows, schema=log_schema)
    log_df.write.mode("append").option("mergeSchema","true").saveAsTable(EVENT_LOG_TABLE)
    print(f"Logged {len(send_log_rows)} send(s) to {EVENT_LOG_TABLE}")
else:
    print("No sends to log.")
