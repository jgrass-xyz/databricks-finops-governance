# Databricks notebook source
# MAGIC %md
# MAGIC # Governance cost rollup
# MAGIC
# MAGIC Prices billing usage in two shared scans regardless of how many asset adapters
# MAGIC are enabled: one for asset cost and one for cost by tag.

# COMMAND ----------

import hashlib
import json
import os
import runpy
import sys

from databricks.sdk import WorkspaceClient

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "finops_observability")
dbutils.widgets.text("correction_lookback_days", "35")
dbutils.widgets.text("initial_backfill_days", "365")

CATALOG_SCHEMA = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"
CORRECTION_DAYS = int(dbutils.widgets.get("correction_lookback_days"))
INITIAL_BACKFILL_DAYS = int(dbutils.widgets.get("initial_backfill_days"))
if CORRECTION_DAYS < 1 or INITIAL_BACKFILL_DAYS < CORRECTION_DAYS:
    raise ValueError("initial_backfill_days must be >= correction_lookback_days >= 1")

_notebook_dir = os.getcwd()
if _notebook_dir not in sys.path:
    sys.path.insert(0, _notebook_dir)
from _collectors import COST_RESOLVERS

_config_path = os.path.abspath(os.path.join(
    _notebook_dir, "..", "..", "config", "governance_assets.py"))
ASSET_TYPES = runpy.run_path(_config_path)["ASSET_TYPES"]
workspace_id = str(WorkspaceClient().get_workspace_id())

# COMMAND ----------

enabled = []
for asset_type, config in sorted(ASSET_TYPES.items()):
    if not config.get("enabled", False):
        continue
    resolver_name = config["cost_resolver"]
    if resolver_name not in COST_RESOLVERS:
        raise ValueError(f"Unknown cost_resolver '{resolver_name}' for '{asset_type}'")
    resolver_hash = hashlib.sha256(
        json.dumps(COST_RESOLVERS[resolver_name], sort_keys=True).encode("utf-8")
    ).hexdigest()
    state_rows = spark.sql(f"""
      SELECT resolver_hash
      FROM {CATALOG_SCHEMA}.governance_silver_cost_refresh_state
      WHERE workspace_id = '{workspace_id}' AND asset_type = '{asset_type}'
      LIMIT 1
    """).collect()
    stored_hash = state_rows[0]["resolver_hash"] if state_rows else None
    # Existing deployments predate resolver hashes. Only serving endpoints changed
    # semantics in this migration; preserve cluster/job history without a rebuild.
    initialized = bool(state_rows) and (
        stored_hash == resolver_hash
        or (stored_hash is None and asset_type != "serving_endpoint")
    )
    lookback_days = CORRECTION_DAYS if initialized else INITIAL_BACKFILL_DAYS
    enabled.append((
        asset_type, config, COST_RESOLVERS[resolver_name], lookback_days, resolver_hash,
    ))

if not enabled:
    dbutils.notebook.exit("No governance asset types enabled")

for asset_type, _config, _resolver, lookback_days, _resolver_hash in enabled:
    for table in (
        "governance_silver_asset_cost_daily",
        "governance_silver_asset_tag_cost_daily",
    ):
        spark.sql(f"""
        DELETE FROM {CATALOG_SCHEMA}.{table}
        WHERE workspace_id = '{workspace_id}'
          AND asset_type = '{asset_type}'
          AND usage_date >= current_date() - INTERVAL {lookback_days} DAYS
        """)

max_lookback_days = max(value[3] for value in enabled)

def quote(value):
    return str(value).replace("'", "''")


stack_items = []
for asset_type, config, resolver, lookback_days, _resolver_hash in enabled:
    # Nulling the ID when its adapter filter does not match lets one STACK expression
    # apply different billing rules to each configured asset lens.
    asset_id = (
        f"CASE WHEN {resolver['extra_filter_sql']} "
        f"THEN CAST({resolver['asset_id_sql']} AS STRING) END"
    )
    stack_items.append(
        f"'{quote(asset_type)}', '{quote(config['product'])}', {asset_id}, {lookback_days}"
    )

stack_sql = f"STACK({len(stack_items)}, {', '.join(stack_items)})"

priced_usage_cte = f"""
priced_usage AS (
  SELECT
    u.workspace_id,
    u.usage_date,
    u.usage_unit,
    u.usage_quantity,
    u.custom_tags,
    u.usage_metadata,
    u.billing_origin_product,
    u.usage_quantity * COALESCE(
      ap.pricing.default, lp.pricing.effective_list.default, lp.pricing.default
    ) AS actual_dollars,
    COALESCE(ap.currency_code, lp.currency_code) AS currency_code,
    CASE WHEN ap.sku_name IS NOT NULL THEN 'ACCOUNT_PRICE' ELSE 'LIST_PRICE' END
      AS pricing_source
  FROM system.billing.usage u
  LEFT JOIN system.billing.account_prices ap
    ON ap.account_id = u.account_id
   AND ap.cloud = u.cloud
   AND ap.sku_name = u.sku_name
   AND u.usage_start_time >= ap.price_start_time
   AND u.usage_start_time < COALESCE(ap.price_end_time, TIMESTAMP '9999-01-01')
  LEFT JOIN system.billing.list_prices lp
    ON lp.account_id = u.account_id
   AND lp.cloud = u.cloud
   AND lp.sku_name = u.sku_name
   AND u.usage_start_time >= lp.price_start_time
   AND u.usage_start_time < COALESCE(lp.price_end_time, TIMESTAMP '9999-01-01')
  WHERE CAST(u.workspace_id AS STRING) = '{workspace_id}'
    AND u.usage_date >= current_date() - INTERVAL {max_lookback_days} DAYS
    AND COALESCE(
      ap.pricing.default, lp.pricing.effective_list.default, lp.pricing.default
    ) IS NOT NULL
), lensed AS (
  SELECT
    u.*, lens_asset_type AS asset_type, lens_product AS product,
    lens_asset_id AS asset_id, lens_lookback_days AS lookback_days
  FROM priced_usage u
  LATERAL VIEW {stack_sql} lens
    AS lens_asset_type, lens_product, lens_asset_id, lens_lookback_days
  WHERE lens_asset_id IS NOT NULL
    AND u.usage_date >= DATE_SUB(current_date(), lens_lookback_days)
)
"""

# COMMAND ----------

spark.sql(f"""
INSERT INTO {CATALOG_SCHEMA}.governance_silver_asset_cost_daily
WITH {priced_usage_cte}
SELECT
  CAST(workspace_id AS STRING), usage_date, product, asset_type, asset_id,
  usage_unit,
  SUM(usage_quantity) AS actual_daily_usage_quantity,
  SUM(actual_dollars) AS actual_daily_dollars,
  SUM(CASE WHEN COALESCE(SIZE(custom_tags), 0) > 0 THEN actual_dollars ELSE 0 END)
    AS billing_tagged_daily_dollars,
  SUM(CASE WHEN COALESCE(SIZE(custom_tags), 0) = 0 THEN actual_dollars ELSE 0 END)
    AS billing_untagged_daily_dollars,
  currency_code,
  pricing_source
FROM lensed
GROUP BY ALL
""")

spark.sql(f"""
INSERT INTO {CATALOG_SCHEMA}.governance_silver_asset_tag_cost_daily
WITH {priced_usage_cte}
SELECT
  CAST(workspace_id AS STRING), usage_date, product, asset_type, asset_id,
  tag_key, tag_value,
  SUM(actual_dollars) AS actual_daily_dollars,
  currency_code,
  pricing_source
FROM lensed
LATERAL VIEW EXPLODE(custom_tags) tags AS tag_key, tag_value
GROUP BY ALL
""")

# Mark initial history complete only after both shared inserts succeed.
for asset_type, _config, _resolver, lookback_days, resolver_hash in enabled:
    spark.sql(f"""
    MERGE INTO {CATALOG_SCHEMA}.governance_silver_cost_refresh_state target
    USING (
      SELECT
        '{workspace_id}' AS workspace_id,
        '{quote(asset_type)}' AS asset_type,
        current_date() - INTERVAL {lookback_days} DAYS AS history_start_date,
        current_timestamp() AS last_success_ts,
        '{resolver_hash}' AS resolver_hash
    ) source
    ON target.workspace_id = source.workspace_id AND target.asset_type = source.asset_type
    WHEN MATCHED THEN UPDATE SET
      target.history_start_date = LEAST(target.history_start_date, source.history_start_date),
      target.last_success_ts = source.last_success_ts,
      target.resolver_hash = source.resolver_hash
    WHEN NOT MATCHED THEN INSERT *
    """)

print(f"Refreshed cost for {len(enabled)} asset types with two shared billing scans")
