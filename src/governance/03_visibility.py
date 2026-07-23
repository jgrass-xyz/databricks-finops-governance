# Databricks notebook source
# MAGIC %md
# MAGIC # Compact asset visibility
# MAGIC
# MAGIC Publishes three deliberately policy-free outputs on top of the existing
# MAGIC governance collectors/resolvers: current service principals, current
# MAGIC configured assets, and historical daily configured-asset cost.

# COMMAND ----------

import os
import sys
from datetime import datetime, timezone

from databricks.sdk import AccountClient, WorkspaceClient


dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "finops_observability")
dbutils.widgets.text("billing_tag_key", "cost_center")

CATALOG_SCHEMA = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"
BILLING_TAG_KEY = dbutils.widgets.get("billing_tag_key").strip()
if not BILLING_TAG_KEY:
    raise ValueError("billing_tag_key must not be empty")

_notebook_dir = os.getcwd()
if _notebook_dir not in sys.path:
    sys.path.insert(0, _notebook_dir)
from _visibility import collect_service_principals


def quote(value):
    return str(value).replace("'", "''")


w = WorkspaceClient()
workspace_id = str(w.get_workspace_id())
snapshot_ts = datetime.now(timezone.utc)

# Account credentials and the manager API are not universal.  Collection still
# succeeds and each principal carries an explicit UNAVAILABLE/ERROR status.
try:
    account_client = AccountClient()
except Exception as error:
    account_client = None
    print(f"AccountClient unavailable; direct owners will be marked unavailable: {error}")

principal_rows = collect_service_principals(w, account_client)
for row in principal_rows:
    row.update({"workspace_id": workspace_id, "snapshot_ts": snapshot_ts})

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG_SCHEMA}.visibility_service_principals_current (
  workspace_id STRING NOT NULL,
  snapshot_ts TIMESTAMP NOT NULL,
  service_principal_id STRING NOT NULL,
  application_id STRING,
  display_name STRING,
  active BOOLEAN NOT NULL,
  direct_owners ARRAY<STRING> NOT NULL,
  owner_resolution_status STRING NOT NULL,
  owner_resolution_error STRING
)
USING DELTA
""")

# Replace only after SDK collection completes. Empty is a valid current inventory.
principal_schema = """
  service_principal_id STRING, application_id STRING, display_name STRING,
  active BOOLEAN, direct_owners ARRAY<STRING>, owner_resolution_status STRING,
  owner_resolution_error STRING, workspace_id STRING, snapshot_ts TIMESTAMP
"""
principal_df = spark.createDataFrame(principal_rows, schema=principal_schema)
spark.sql(f"DELETE FROM {CATALOG_SCHEMA}.visibility_service_principals_current")
(principal_df.select(
    "workspace_id", "snapshot_ts", "service_principal_id", "application_id",
    "display_name", "active", "direct_owners", "owner_resolution_status",
    "owner_resolution_error",
).write.mode("append").saveAsTable(
    f"{CATALOG_SCHEMA}.visibility_service_principals_current"))

# COMMAND ----------

# Current configured assets only: policies and requirement status intentionally do
# not leak into this compact visibility contract. Billing-only rows belong in the
# cost output, not in a list claiming to represent currently configured assets.
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG_SCHEMA}.visibility_assets_current AS
WITH matched AS (
  SELECT
    a.workspace_id,
    a.snapshot_ts,
    a.product,
    a.asset_type,
    a.asset_id,
    a.asset_name,
    a.owner,
    a.lifecycle_state,
    SORT_ARRAY(TRANSFORM(
      MAP_ENTRIES(a.tags), tag -> NAMED_STRUCT('key', tag.key, 'value', tag.value)
    )) AS tags,
    p.service_principal_id AS owner_service_principal_id,
    p.application_id AS owner_service_principal_application_id,
    p.display_name AS owner_service_principal_name,
    p.active AS owner_service_principal_active,
    ROW_NUMBER() OVER (
      PARTITION BY a.workspace_id, a.asset_type, a.asset_id
      ORDER BY p.service_principal_id
    ) AS match_number
  FROM {CATALOG_SCHEMA}.governance_gold_asset_current a
  LEFT JOIN {CATALOG_SCHEMA}.visibility_service_principals_current p
    ON p.workspace_id = a.workspace_id
   AND LOWER(TRIM(a.owner)) IN (
     LOWER(TRIM(p.service_principal_id)),
     LOWER(TRIM(p.application_id)),
     LOWER(TRIM(p.display_name))
   )
  WHERE a.inventory_status = 'OBSERVED'
)
SELECT * EXCEPT (match_number) FROM matched WHERE match_number = 1
""")

# COMMAND ----------

# Aggregate before joining to avoid multiplying costs across usage units/pricing
# sources. The left joins are intentional: a billed asset with no inventory or no
# selected billing tag remains visible as BILLING_ONLY / NULL tag value.
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG_SCHEMA}.visibility_asset_cost_daily AS
WITH cost AS (
  SELECT
    workspace_id, usage_date, product, asset_type, asset_id,
    SUM(actual_daily_usage_quantity) AS actual_daily_usage_quantity,
    SUM(actual_daily_dollars) AS actual_daily_dollars,
    COLLECT_SET(usage_unit) AS usage_units,
    COLLECT_SET(currency_code) AS currency_codes,
    COLLECT_SET(pricing_source) AS pricing_sources
  FROM {CATALOG_SCHEMA}.governance_silver_asset_cost_daily
  GROUP BY workspace_id, usage_date, product, asset_type, asset_id
), selected_tag AS (
  SELECT
    workspace_id, usage_date, product, asset_type, asset_id,
    MAX(tag_value) AS billing_tag_value,
    SUM(actual_daily_dollars) AS tagged_daily_dollars
  FROM {CATALOG_SCHEMA}.governance_silver_asset_tag_cost_daily
  WHERE LOWER(tag_key) = LOWER('{quote(BILLING_TAG_KEY)}')
  GROUP BY workspace_id, usage_date, product, asset_type, asset_id
), inventory_intervals AS (
  SELECT
    workspace_id, product, asset_type, asset_id, asset_name, owner, snapshot_date,
    LEAD(snapshot_date, 1, DATE '9999-12-31') OVER (
      PARTITION BY workspace_id, asset_type, asset_id ORDER BY snapshot_date
    ) AS next_snapshot_date
  FROM (
    SELECT *, ROW_NUMBER() OVER (
      PARTITION BY workspace_id, asset_type, asset_id, snapshot_date
      ORDER BY snapshot_ts DESC
    ) AS snapshot_number
    FROM {CATALOG_SCHEMA}.governance_silver_asset_inventory_snapshot
  )
  WHERE snapshot_number = 1
), joined AS (
  SELECT
    c.*,
    '{quote(BILLING_TAG_KEY)}' AS billing_tag_key,
    t.billing_tag_value,
    COALESCE(t.tagged_daily_dollars, 0) AS tagged_daily_dollars,
    CASE WHEN i.asset_id IS NULL THEN 'BILLING_ONLY' ELSE 'OBSERVED' END
      AS inventory_status,
    COALESCE(i.asset_name, c.asset_id) AS asset_name,
    i.owner,
    p.service_principal_id AS owner_service_principal_id,
    p.application_id AS owner_service_principal_application_id,
    p.display_name AS owner_service_principal_name,
    p.active AS owner_service_principal_active,
    ROW_NUMBER() OVER (
      PARTITION BY c.workspace_id, c.usage_date, c.asset_type, c.asset_id
      ORDER BY p.service_principal_id
    ) AS match_number
  FROM cost c
  LEFT JOIN inventory_intervals i
    ON i.workspace_id = c.workspace_id
   AND i.asset_type = c.asset_type
   AND i.asset_id = c.asset_id
   AND c.usage_date >= i.snapshot_date
   AND c.usage_date < i.next_snapshot_date
  LEFT JOIN selected_tag t
    ON t.workspace_id = c.workspace_id
   AND t.usage_date = c.usage_date
   AND t.asset_type = c.asset_type
   AND t.asset_id = c.asset_id
  LEFT JOIN {CATALOG_SCHEMA}.visibility_service_principals_current p
    ON p.workspace_id = c.workspace_id
   AND LOWER(TRIM(i.owner)) IN (
     LOWER(TRIM(p.service_principal_id)),
     LOWER(TRIM(p.application_id)),
     LOWER(TRIM(p.display_name))
   )
)
SELECT * EXCEPT (match_number) FROM joined WHERE match_number = 1
""")

unavailable = sum(
    row["owner_resolution_status"] != "RESOLVED" for row in principal_rows)
print(
    f"Published visibility outputs for {len(principal_rows)} service principals; "
    f"direct-owner resolution unavailable/failed for {unavailable}"
)
