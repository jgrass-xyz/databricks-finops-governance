"""Cost-alerting helpers shared by 03_slack_alerting and the test harness.

``now_sql`` lets tests substitute a frozen ``TIMESTAMP '...'`` for ``current_timestamp()``.
"""

import json

DETECTOR_NAME = "cluster_cost"


def cluster_alerts_latest_ddl(catalog_schema, now_sql="current_timestamp()"):
    return f"""
CREATE OR REPLACE VIEW {catalog_schema}.cluster_alerts_latest AS
WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY workspace_id, cluster_id ORDER BY snapshot_ts DESC) AS rn
  FROM {catalog_schema}.cluster_scores
)
SELECT record_id, workspace_id, snapshot_ts, cluster_id, cluster_name, creator,
       node_type, num_workers, photon, sku_name, dbu_per_hour, dollars_per_hour,
       projected_daily_dollars, z_score, signal, reason
FROM ranked
WHERE rn = 1
  AND signal IN ('WARNING', 'CRITICAL')
  AND snapshot_ts >= {now_sql} - INTERVAL 30 MINUTES
  AND record_id IS NOT NULL
"""


def suppression_sql(catalog_schema, workspace_id, warn_hours, crit_hours,
                    now_sql="current_timestamp()"):
    return f"""
WITH last_send AS (
  SELECT asset_id, severity, MAX(event_ts) AS last_sent_at
  FROM {catalog_schema}.event_log
  WHERE workspace_id = {workspace_id}
    AND detector_name = '{DETECTOR_NAME}'
    AND event_type = 'message_sent'
    AND severity IN ('WARNING', 'CRITICAL')
  GROUP BY asset_id, severity
)
SELECT a.*
FROM {catalog_schema}.cluster_alerts_latest a
LEFT JOIN last_send ls
  ON ls.asset_id = a.cluster_id
 AND ls.severity = a.signal
WHERE a.workspace_id = {workspace_id}
  AND (
    ls.last_sent_at IS NULL
    OR (a.signal = 'CRITICAL' AND ls.last_sent_at < {now_sql} - INTERVAL {crit_hours} HOURS)
    OR (a.signal = 'WARNING'  AND ls.last_sent_at < {now_sql} - INTERVAL {warn_hours} HOURS)
  )
ORDER BY a.cluster_id
"""


def format_pct_above_baseline(projected, baseline_mean):
    if baseline_mean and baseline_mean > 0 and projected:
        return f"{(projected - baseline_mean) / baseline_mean * 100.0:,.0f}%"
    return "?"


def format_plain_text(severity, cluster_name, formatted_dollars, formatted_percent):
    return (
        f"{severity}: cluster {cluster_name} projected at "
        f"{formatted_dollars}/day ({formatted_percent} above average)"
    )


def resolve_send_targets(routing_mode, creator_user_id, alert_channel, tee_to_channel):
    """Ordered ``[(recipient, routing)]`` destinations for a single alert.

    - ``channel_only``: always the central channel.
    - ``dm_with_fallback`` + owner resolved: DM the owner; when ``tee_to_channel``
      also post a copy to the central channel so there's a central feed of every
      alert.
    - ``dm_with_fallback`` + owner unresolved: fall back to the channel once (never
      duplicated — the alert already reached the channel).

    Pure function so both ``03_slack_alerting`` and the test harness share the exact
    routing decision.
    """
    if routing_mode == "dm_with_fallback":
        if creator_user_id:
            targets = [(creator_user_id, "dm")]
            if tee_to_channel:
                targets.append((alert_channel, "tee_channel"))
            return targets
        return [(alert_channel, "fallback_channel")]
    return [(alert_channel, "channel_only")]


def build_event_log_row(alert_row, now, workspace_id, recipient, routing,
                        message_ts, plain, blocks):
    """Build the dict appended to event_log for one message_sent event."""
    return {
        "event_ts": now,
        "event_type": "message_sent",
        "detector_name": DETECTOR_NAME,
        "workspace_id": workspace_id,
        "record_id": alert_row["record_id"],
        "asset_type": "cluster",
        "asset_id": alert_row["cluster_id"],
        "asset_name": alert_row["cluster_name"],
        "severity": alert_row["signal"],
        "recipient": recipient,
        "routing": routing,
        "message_ts": message_ts,
        "plain": plain,
        "blocks": json.dumps(blocks),
        "details_json": json.dumps({
            "projected_daily_dollars": alert_row["projected_daily_dollars"],
            "z_score": alert_row["z_score"],
            "dollars_per_hour": alert_row["dollars_per_hour"],
        }),
    }
