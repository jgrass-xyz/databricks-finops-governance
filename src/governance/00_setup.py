# Databricks notebook source
# MAGIC %md
# MAGIC # Governance setup
# MAGIC
# MAGIC Creates the Silver snapshot/cost tables and Gold dashboard views shared by
# MAGIC clusters, jobs, and serving endpoints.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "finops_observability")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
CATALOG_SCHEMA = f"{CATALOG}.{SCHEMA}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_SCHEMA}")

# COMMAND ----------

# Preserve deployed history while moving the governance module into an explicit
# naming namespace. ALTER TABLE retains Delta history, permissions, and data.
SILVER_TABLE_RENAMES = {
    "governance_collection_run": "governance_silver_collection_run",
    "governance_requirement_snapshot": "governance_silver_requirement_snapshot",
    "asset_inventory_snapshot": "governance_silver_asset_inventory_snapshot",
    "asset_tag_snapshot": "governance_silver_asset_tag_snapshot",
    "policy_snapshot": "governance_silver_policy_snapshot",
    "policy_term_snapshot": "governance_silver_policy_term_snapshot",
    "asset_cost_daily": "governance_silver_asset_cost_daily",
    "asset_tag_cost_daily": "governance_silver_asset_tag_cost_daily",
    "governance_cost_refresh_state": "governance_silver_cost_refresh_state",
}

for legacy_name, current_name in SILVER_TABLE_RENAMES.items():
    legacy_table = f"{CATALOG_SCHEMA}.{legacy_name}"
    current_table = f"{CATALOG_SCHEMA}.{current_name}"
    if spark.catalog.tableExists(legacy_table) and not spark.catalog.tableExists(current_table):
        spark.sql(f"ALTER TABLE {legacy_table} RENAME TO {current_table}")
        print(f"Renamed {legacy_table} to {current_table}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG_SCHEMA}.governance_silver_collection_run (
  workspace_id STRING NOT NULL,
  collection_run_id STRING NOT NULL,
  snapshot_ts TIMESTAMP NOT NULL,
  snapshot_date DATE GENERATED ALWAYS AS (CAST(snapshot_ts AS DATE)),
  asset_type STRING NOT NULL,
  status STRING NOT NULL,
  asset_count BIGINT,
  error_message STRING
)
USING DELTA
PARTITIONED BY (snapshot_date)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG_SCHEMA}.governance_silver_requirement_snapshot (
  workspace_id STRING NOT NULL,
  collection_run_id STRING NOT NULL,
  snapshot_ts TIMESTAMP NOT NULL,
  snapshot_date DATE GENERATED ALWAYS AS (CAST(snapshot_ts AS DATE)),
  product STRING NOT NULL,
  asset_type STRING NOT NULL,
  enabled BOOLEAN NOT NULL,
  collector STRING NOT NULL,
  cost_resolver STRING NOT NULL,
  required_tags ARRAY<STRING> NOT NULL,
  required_policies ARRAY<STRING> NOT NULL,
  config_hash STRING NOT NULL,
  raw_config STRING NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG_SCHEMA}.governance_silver_asset_inventory_snapshot (
  workspace_id STRING NOT NULL,
  collection_run_id STRING NOT NULL,
  snapshot_ts TIMESTAMP NOT NULL,
  snapshot_date DATE GENERATED ALWAYS AS (CAST(snapshot_ts AS DATE)),
  product STRING NOT NULL,
  asset_type STRING NOT NULL,
  asset_id STRING NOT NULL,
  asset_name STRING NOT NULL,
  owner STRING,
  lifecycle_state STRING,
  tags MAP<STRING, STRING> NOT NULL,
  policies ARRAY<STRUCT<policy_type:STRING, policy_id:STRING, policy_name:STRING>> NOT NULL,
  discovery_source STRING,
  api_enriched BOOLEAN,
  tag_observation_complete BOOLEAN,
  policy_observation_complete BOOLEAN,
  raw_payload STRING
)
USING DELTA
PARTITIONED BY (snapshot_date)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG_SCHEMA}.governance_silver_asset_tag_snapshot (
  workspace_id STRING NOT NULL,
  collection_run_id STRING NOT NULL,
  snapshot_ts TIMESTAMP NOT NULL,
  snapshot_date DATE GENERATED ALWAYS AS (CAST(snapshot_ts AS DATE)),
  asset_type STRING NOT NULL,
  asset_id STRING NOT NULL,
  tag_key STRING NOT NULL,
  tag_value STRING NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG_SCHEMA}.governance_silver_policy_snapshot (
  workspace_id STRING NOT NULL,
  collection_run_id STRING NOT NULL,
  snapshot_ts TIMESTAMP NOT NULL,
  snapshot_date DATE GENERATED ALWAYS AS (CAST(snapshot_ts AS DATE)),
  policy_type STRING NOT NULL,
  policy_id STRING NOT NULL,
  policy_name STRING,
  description STRING,
  definition_json STRING,
  policy_family_id STRING,
  raw_payload STRING
)
USING DELTA
PARTITIONED BY (snapshot_date)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG_SCHEMA}.governance_silver_policy_term_snapshot (
  workspace_id STRING NOT NULL,
  collection_run_id STRING NOT NULL,
  snapshot_ts TIMESTAMP NOT NULL,
  snapshot_date DATE GENERATED ALWAYS AS (CAST(snapshot_ts AS DATE)),
  policy_type STRING NOT NULL,
  policy_id STRING NOT NULL,
  definition_path STRING NOT NULL,
  rule_type STRING NOT NULL,
  rule_json STRING NOT NULL,
  hidden BOOLEAN NOT NULL,
  is_optional BOOLEAN NOT NULL
)
USING DELTA
PARTITIONED BY (snapshot_date)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG_SCHEMA}.governance_silver_asset_cost_daily (
  workspace_id STRING NOT NULL,
  usage_date DATE NOT NULL,
  product STRING NOT NULL,
  asset_type STRING NOT NULL,
  asset_id STRING NOT NULL,
  usage_unit STRING,
  actual_daily_usage_quantity DECIMAL(38, 18),
  actual_daily_dollars DECIMAL(38, 12),
  billing_tagged_daily_dollars DECIMAL(38, 12),
  billing_untagged_daily_dollars DECIMAL(38, 12),
  currency_code STRING,
  pricing_source STRING
)
USING DELTA
PARTITIONED BY (usage_date)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG_SCHEMA}.governance_silver_asset_tag_cost_daily (
  workspace_id STRING NOT NULL,
  usage_date DATE NOT NULL,
  product STRING NOT NULL,
  asset_type STRING NOT NULL,
  asset_id STRING NOT NULL,
  tag_key STRING NOT NULL,
  tag_value STRING NOT NULL,
  actual_daily_dollars DECIMAL(38, 12),
  currency_code STRING,
  pricing_source STRING
)
USING DELTA
PARTITIONED BY (usage_date)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG_SCHEMA}.governance_silver_cost_refresh_state (
  workspace_id STRING NOT NULL,
  asset_type STRING NOT NULL,
  history_start_date DATE NOT NULL,
  last_success_ts TIMESTAMP NOT NULL,
  resolver_hash STRING
)
USING DELTA
""")

# Lightweight forward migration for dev/prod schemas created by an earlier bundle.
for table in ("governance_silver_asset_cost_daily", "governance_silver_asset_tag_cost_daily"):
    if "pricing_source" not in spark.table(f"{CATALOG_SCHEMA}.{table}").columns:
        spark.sql(f"ALTER TABLE {CATALOG_SCHEMA}.{table} ADD COLUMNS (pricing_source STRING)")

inventory_columns = spark.table(
    f"{CATALOG_SCHEMA}.governance_silver_asset_inventory_snapshot").columns
if "discovery_source" not in inventory_columns:
    spark.sql(f"""
      ALTER TABLE {CATALOG_SCHEMA}.governance_silver_asset_inventory_snapshot
      ADD COLUMNS (discovery_source STRING, api_enriched BOOLEAN)
    """)
inventory_columns = spark.table(
    f"{CATALOG_SCHEMA}.governance_silver_asset_inventory_snapshot").columns
if "tag_observation_complete" not in inventory_columns:
    spark.sql(f"""
      ALTER TABLE {CATALOG_SCHEMA}.governance_silver_asset_inventory_snapshot
      ADD COLUMNS (
        tag_observation_complete BOOLEAN,
        policy_observation_complete BOOLEAN
      )
    """)

refresh_state_columns = spark.table(
    f"{CATALOG_SCHEMA}.governance_silver_cost_refresh_state").columns
if "resolver_hash" not in refresh_state_columns:
    spark.sql(f"""
      ALTER TABLE {CATALOG_SCHEMA}.governance_silver_cost_refresh_state
      ADD COLUMNS (resolver_hash STRING)
    """)

# COMMAND ----------

# Historical evaluation is joined to the requirement snapshot from the exact same
# collection run. Changing today's config therefore never rewrites yesterday's result.
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG_SCHEMA}.governance_gold_asset_history AS
WITH observed AS (
  SELECT
    a.*,
    r.required_tags,
    r.required_policies,
    r.config_hash,
    TRANSFORM(
      MAP_KEYS(MAP_FILTER(a.tags, (key, value) -> value IS NOT NULL AND TRIM(value) != '')),
      key -> LOWER(key)
    ) AS observed_tag_keys,
    TRANSFORM(a.policies, policy -> policy.policy_type) AS observed_policy_types
  FROM {CATALOG_SCHEMA}.governance_silver_asset_inventory_snapshot a
  JOIN {CATALOG_SCHEMA}.governance_silver_requirement_snapshot r
    ON r.workspace_id = a.workspace_id
   AND r.collection_run_id = a.collection_run_id
   AND r.asset_type = a.asset_type
), evaluated AS (
  SELECT
    *,
    FILTER(required_tags, required -> NOT ARRAY_CONTAINS(observed_tag_keys, LOWER(required)))
      AS evaluated_missing_required_tags,
    FILTER(required_policies, required -> NOT ARRAY_CONTAINS(observed_policy_types, UPPER(required)))
      AS evaluated_missing_required_policies
  FROM observed
)
SELECT
  * EXCEPT (
    observed_tag_keys, observed_policy_types,
    evaluated_missing_required_tags, evaluated_missing_required_policies
  ),
  CASE
    WHEN COALESCE(tag_observation_complete, FALSE)
      THEN evaluated_missing_required_tags
    ELSE CAST(NULL AS ARRAY<STRING>)
  END AS missing_required_tags,
  CASE
    WHEN COALESCE(policy_observation_complete, FALSE)
      THEN evaluated_missing_required_policies
    ELSE CAST(NULL AS ARRAY<STRING>)
  END AS missing_required_policies,
  CASE
    WHEN SIZE(required_tags) = 0 THEN 'NOT_CONFIGURED'
    WHEN NOT COALESCE(tag_observation_complete, FALSE) THEN 'UNKNOWN'
    WHEN SIZE(evaluated_missing_required_tags) = 0 THEN 'APPLIED'
    WHEN SIZE(evaluated_missing_required_tags) < SIZE(required_tags) THEN 'PARTIAL'
    ELSE 'NOT_APPLIED'
  END AS tag_status,
  CASE
    WHEN SIZE(required_policies) = 0 THEN 'NOT_CONFIGURED'
    WHEN NOT COALESCE(policy_observation_complete, FALSE) THEN 'UNKNOWN'
    WHEN SIZE(evaluated_missing_required_policies) = 0 THEN 'APPLIED'
    WHEN SIZE(evaluated_missing_required_policies) < SIZE(required_policies) THEN 'PARTIAL'
    ELSE 'NOT_APPLIED'
  END AS policy_status
FROM evaluated
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG_SCHEMA}.governance_gold_asset_current AS
WITH latest_success AS (
  SELECT workspace_id, asset_type, collection_run_id
  FROM (
    SELECT
      workspace_id, asset_type, collection_run_id,
      ROW_NUMBER() OVER (
        PARTITION BY workspace_id, asset_type ORDER BY snapshot_ts DESC
      ) AS row_num
    FROM {CATALOG_SCHEMA}.governance_silver_collection_run
    WHERE status = 'SUCCESS'
  )
  WHERE row_num = 1
), inventory AS (
  SELECT h.*
  FROM {CATALOG_SCHEMA}.governance_gold_asset_history h
  JOIN latest_success s
    ON s.workspace_id = h.workspace_id
   AND s.asset_type = h.asset_type
   AND s.collection_run_id = h.collection_run_id
), requirements AS (
  SELECT r.*
  FROM {CATALOG_SCHEMA}.governance_silver_requirement_snapshot r
  JOIN latest_success s
    ON s.workspace_id = r.workspace_id
   AND s.asset_type = r.asset_type
   AND s.collection_run_id = r.collection_run_id
), cost AS (
  SELECT
    workspace_id, product, asset_type, asset_id,
    SUM(CASE WHEN usage_date = current_date()
      THEN actual_daily_dollars ELSE 0 END) AS current_day_dollars,
    SUM(CASE WHEN usage_date = current_date() - INTERVAL 1 DAY
      THEN actual_daily_dollars ELSE 0 END) AS previous_day_dollars,
    SUM(CASE WHEN usage_date BETWEEN current_date() - INTERVAL 7 DAYS
      AND current_date() - INTERVAL 1 DAY
      THEN actual_daily_dollars ELSE 0 END) AS trailing_7d_dollars,
    SUM(CASE WHEN usage_date BETWEEN current_date() - INTERVAL 30 DAYS
      AND current_date() - INTERVAL 1 DAY
      THEN actual_daily_dollars ELSE 0 END) AS trailing_30d_dollars,
    SUM(CASE WHEN usage_date BETWEEN current_date() - INTERVAL 90 DAYS
      AND current_date() - INTERVAL 1 DAY
      THEN actual_daily_dollars ELSE 0 END) AS trailing_90d_dollars,
    SUM(CASE WHEN usage_date >= DATE_TRUNC('MONTH', current_date())
      THEN actual_daily_dollars ELSE 0 END) AS month_to_date_dollars,
    SUM(CASE WHEN usage_date >= DATE_TRUNC('YEAR', current_date())
      THEN actual_daily_dollars ELSE 0 END) AS year_to_date_dollars,
    SUM(actual_daily_dollars) AS total_observed_dollars,
    MIN(usage_date) AS first_cost_date,
    MAX(usage_date) AS last_cost_date,
    COLLECT_SET(currency_code) AS currency_codes
  FROM {CATALOG_SCHEMA}.governance_silver_asset_cost_daily
  GROUP BY workspace_id, product, asset_type, asset_id
), observed AS (
  SELECT
    i.*,
    'OBSERVED' AS inventory_status,
    COALESCE(c.current_day_dollars, 0) AS current_day_dollars,
    COALESCE(c.previous_day_dollars, 0) AS previous_day_dollars,
    COALESCE(c.previous_day_dollars, 0) AS actual_daily_dollars,
    COALESCE(c.trailing_7d_dollars, 0) AS trailing_7d_dollars,
    COALESCE(c.trailing_30d_dollars, 0) AS trailing_30d_dollars,
    COALESCE(c.trailing_90d_dollars, 0) AS trailing_90d_dollars,
    COALESCE(c.month_to_date_dollars, 0) AS month_to_date_dollars,
    COALESCE(c.year_to_date_dollars, 0) AS year_to_date_dollars,
    COALESCE(c.total_observed_dollars, 0) AS total_observed_dollars,
    c.first_cost_date,
    c.last_cost_date,
    c.currency_codes
  FROM inventory i
  LEFT JOIN cost c
    ON c.workspace_id = i.workspace_id
   AND c.asset_type = i.asset_type
   AND c.asset_id = i.asset_id
), billing_only AS (
  SELECT
    c.workspace_id,
    r.collection_run_id,
    r.snapshot_ts,
    r.snapshot_date,
    c.product,
    c.asset_type,
    c.asset_id,
    c.asset_id AS asset_name,
    CAST(NULL AS STRING) AS owner,
    'BILLING_ONLY' AS lifecycle_state,
    CAST(MAP() AS MAP<STRING, STRING>) AS tags,
    CAST(ARRAY() AS ARRAY<STRUCT<policy_type:STRING, policy_id:STRING, policy_name:STRING>>)
      AS policies,
    'BILLING' AS discovery_source,
    FALSE AS api_enriched,
    FALSE AS tag_observation_complete,
    FALSE AS policy_observation_complete,
    CAST(NULL AS STRING) AS raw_payload,
    r.required_tags,
    r.required_policies,
    r.config_hash,
    CAST(NULL AS ARRAY<STRING>) AS missing_required_tags,
    CAST(NULL AS ARRAY<STRING>) AS missing_required_policies,
    'UNKNOWN' AS tag_status,
    'UNKNOWN' AS policy_status,
    'BILLING_ONLY' AS inventory_status,
    c.current_day_dollars,
    c.previous_day_dollars,
    c.previous_day_dollars AS actual_daily_dollars,
    c.trailing_7d_dollars,
    c.trailing_30d_dollars,
    c.trailing_90d_dollars,
    c.month_to_date_dollars,
    c.year_to_date_dollars,
    c.total_observed_dollars,
    c.first_cost_date,
    c.last_cost_date,
    c.currency_codes
  FROM cost c
  JOIN requirements r
    ON r.workspace_id = c.workspace_id AND r.asset_type = c.asset_type
  LEFT ANTI JOIN inventory i
    ON i.workspace_id = c.workspace_id
   AND i.asset_type = c.asset_type
   AND i.asset_id = c.asset_id
  WHERE c.last_cost_date >= current_date() - INTERVAL 30 DAYS
)
SELECT * FROM observed
UNION ALL
SELECT * FROM billing_only
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG_SCHEMA}.governance_gold_asset_daily AS
SELECT
  h.workspace_id,
  h.snapshot_date,
  h.product,
  h.asset_type,
  h.tag_status,
  h.policy_status,
  COUNT(*) AS asset_count,
  COALESCE(SUM(c.actual_daily_dollars), 0) AS actual_daily_dollars
FROM {CATALOG_SCHEMA}.governance_gold_asset_history h
LEFT JOIN {CATALOG_SCHEMA}.governance_silver_asset_cost_daily c
  ON c.workspace_id = h.workspace_id
 AND c.usage_date = h.snapshot_date
 AND c.asset_type = h.asset_type
 AND c.asset_id = h.asset_id
GROUP BY ALL
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG_SCHEMA}.governance_gold_cost_coverage_daily AS
WITH latest_success AS (
  SELECT workspace_id, asset_type, collection_run_id
  FROM (
    SELECT
      workspace_id, asset_type, collection_run_id,
      ROW_NUMBER() OVER (
        PARTITION BY workspace_id, asset_type ORDER BY snapshot_ts DESC
      ) AS row_num
    FROM {CATALOG_SCHEMA}.governance_silver_collection_run
    WHERE status = 'SUCCESS'
  )
  WHERE row_num = 1
), inventory AS (
  SELECT a.workspace_id, a.asset_type, a.asset_id
  FROM {CATALOG_SCHEMA}.governance_silver_asset_inventory_snapshot a
  JOIN latest_success s
    ON s.workspace_id = a.workspace_id
   AND s.asset_type = a.asset_type
   AND s.collection_run_id = a.collection_run_id
), cost AS (
  SELECT
    workspace_id, usage_date, product, asset_type, asset_id,
    SUM(actual_daily_dollars) AS actual_daily_dollars
  FROM {CATALOG_SCHEMA}.governance_silver_asset_cost_daily
  GROUP BY ALL
)
SELECT
  c.workspace_id,
  c.usage_date,
  c.product,
  c.asset_type,
  COUNT(DISTINCT c.asset_id) AS billed_asset_count,
  COUNT(DISTINCT CASE WHEN i.asset_id IS NOT NULL THEN c.asset_id END)
    AS inventory_matched_asset_count,
  COUNT(DISTINCT CASE WHEN i.asset_id IS NULL THEN c.asset_id END)
    AS billing_only_asset_count,
  SUM(c.actual_daily_dollars) AS total_billed_dollars,
  SUM(CASE WHEN i.asset_id IS NOT NULL THEN c.actual_daily_dollars ELSE 0 END)
    AS inventory_matched_dollars,
  SUM(CASE WHEN i.asset_id IS NULL THEN c.actual_daily_dollars ELSE 0 END)
    AS billing_only_dollars,
  100.0 * COUNT(DISTINCT CASE WHEN i.asset_id IS NOT NULL THEN c.asset_id END)
    / NULLIF(COUNT(DISTINCT c.asset_id), 0) AS asset_coverage_pct,
  100.0 * SUM(CASE WHEN i.asset_id IS NOT NULL THEN c.actual_daily_dollars ELSE 0 END)
    / NULLIF(SUM(c.actual_daily_dollars), 0) AS dollar_coverage_pct
FROM cost c
LEFT JOIN inventory i
  ON i.workspace_id = c.workspace_id
 AND i.asset_type = c.asset_type
 AND i.asset_id = c.asset_id
GROUP BY c.workspace_id, c.usage_date, c.product, c.asset_type
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG_SCHEMA}.governance_gold_config_history AS
SELECT * FROM {CATALOG_SCHEMA}.governance_silver_requirement_snapshot
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG_SCHEMA}.governance_gold_observed_tags_policies_current AS
WITH current_assets AS (
  SELECT * FROM {CATALOG_SCHEMA}.governance_gold_asset_current
), values AS (
  SELECT
    workspace_id, product, asset_type, asset_id,
    'TAG' AS governance_kind,
    tag_key AS governance_type,
    tag_value AS governance_id,
    CAST(NULL AS STRING) AS governance_name,
    trailing_30d_dollars
  FROM current_assets
  LATERAL VIEW EXPLODE(tags) tags_view AS tag_key, tag_value
  UNION ALL
  SELECT
    workspace_id, product, asset_type, asset_id,
    'POLICY' AS governance_kind,
    policy.policy_type AS governance_type,
    policy.policy_id AS governance_id,
    policy.policy_name AS governance_name,
    trailing_30d_dollars
  FROM current_assets
  LATERAL VIEW EXPLODE(policies) policies_view AS policy
)
SELECT
  workspace_id, product, asset_type, governance_kind,
  governance_type, governance_id, governance_name,
  COUNT(DISTINCT asset_id) AS asset_count,
  SUM(trailing_30d_dollars) AS trailing_30d_dollars
FROM values
GROUP BY ALL
""")

# Remove the pre-standardization Gold names only after every replacement view is
# available. Silver legacy names were renamed above, so no duplicate tables remain.
for legacy_view in (
    "gold_asset_governance_history",
    "gold_asset_governance_current",
    "gold_asset_governance_daily",
    "gold_governance_config_history",
    "gold_observed_tags_policies_current",
):
    spark.sql(f"DROP VIEW IF EXISTS {CATALOG_SCHEMA}.{legacy_view}")

print(f"Governance Silver tables and Gold views ensured in {CATALOG_SCHEMA}")
