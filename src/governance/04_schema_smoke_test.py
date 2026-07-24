# Databricks notebook source
# MAGIC %md
# MAGIC # Governance schema smoke test
# MAGIC
# MAGIC Asserts the exact schemas created by `00_setup.py`. Run only against the
# MAGIC disposable schema configured by the manual `governance_schema_smoke_test` job.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "finops_schema_smoke_test")

CATALOG_SCHEMA = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"

EXPECTED_COLUMNS = {
    "governance_silver_collection_run": [
        "workspace_id", "collection_run_id", "snapshot_ts", "snapshot_date",
        "asset_type", "status", "asset_count", "error_message",
    ],
    "governance_silver_requirement_snapshot": [
        "workspace_id", "collection_run_id", "snapshot_ts", "snapshot_date",
        "product", "asset_type", "enabled", "collector", "cost_resolver",
        "required_tags", "config_hash", "raw_config",
    ],
    "governance_silver_asset_inventory_snapshot": [
        "workspace_id", "collection_run_id", "snapshot_ts", "snapshot_date",
        "product", "asset_type", "asset_id", "asset_name", "owner",
        "lifecycle_state", "tags", "discovery_source", "api_enriched",
        "tag_observation_complete", "raw_payload",
    ],
    "governance_silver_asset_tag_snapshot": [
        "workspace_id", "collection_run_id", "snapshot_ts", "snapshot_date",
        "asset_type", "asset_id", "tag_key", "tag_value",
    ],
    "governance_silver_asset_cost_daily": [
        "workspace_id", "usage_date", "product", "asset_type", "asset_id",
        "usage_unit", "actual_daily_usage_quantity", "actual_daily_dollars",
        "billing_tagged_daily_dollars", "billing_untagged_daily_dollars",
        "currency_code", "pricing_source",
    ],
    "governance_silver_asset_tag_cost_daily": [
        "workspace_id", "usage_date", "product", "asset_type", "asset_id",
        "tag_key", "tag_value", "actual_daily_dollars", "currency_code",
        "pricing_source",
    ],
    "governance_silver_cost_refresh_state": [
        "workspace_id", "asset_type", "history_start_date", "last_success_ts",
        "resolver_hash",
    ],
    "governance_gold_asset_history": [
        "workspace_id", "collection_run_id", "snapshot_ts", "snapshot_date",
        "product", "asset_type", "asset_id", "asset_name", "owner",
        "lifecycle_state", "tags", "discovery_source", "api_enriched",
        "tag_observation_complete", "raw_payload", "required_tags", "config_hash",
        "missing_required_tags", "tag_status",
    ],
    "governance_gold_asset_current": [
        "workspace_id", "collection_run_id", "snapshot_ts", "snapshot_date",
        "product", "asset_type", "asset_id", "asset_name", "owner",
        "lifecycle_state", "tags", "discovery_source", "api_enriched",
        "tag_observation_complete", "raw_payload", "required_tags", "config_hash",
        "missing_required_tags", "tag_status", "inventory_status",
        "current_day_dollars", "previous_day_dollars", "actual_daily_dollars",
        "trailing_7d_dollars", "trailing_30d_dollars", "trailing_90d_dollars",
        "month_to_date_dollars", "year_to_date_dollars", "total_observed_dollars",
        "first_cost_date", "last_cost_date", "currency_codes",
    ],
    "governance_gold_asset_daily": [
        "workspace_id", "snapshot_date", "product", "asset_type", "tag_status",
        "asset_count", "actual_daily_dollars",
    ],
    "governance_gold_cost_coverage_daily": [
        "workspace_id", "usage_date", "product", "asset_type", "billed_asset_count",
        "inventory_matched_asset_count", "billing_only_asset_count",
        "total_billed_dollars", "inventory_matched_dollars", "billing_only_dollars",
        "asset_coverage_pct", "dollar_coverage_pct",
    ],
    "governance_gold_config_history": [
        "workspace_id", "collection_run_id", "snapshot_ts", "snapshot_date",
        "product", "asset_type", "enabled", "collector", "cost_resolver",
        "required_tags", "config_hash", "raw_config",
    ],
    "governance_gold_observed_tags_current": [
        "workspace_id", "product", "asset_type", "tag_name", "tag_value",
        "asset_count", "trailing_30d_dollars",
    ],
}

failures = []
for object_name, expected in EXPECTED_COLUMNS.items():
    qualified_name = f"{CATALOG_SCHEMA}.{object_name}"
    try:
        actual = spark.table(qualified_name).columns
    except Exception as error:
        failures.append(f"{object_name}: unavailable: {type(error).__name__}: {error}")
        continue
    if actual != expected:
        failures.append(
            f"{object_name}:\n  expected={expected}\n  actual={actual}"
        )
    else:
        print(f"PASS {object_name}: {len(actual)} columns")

# The preceding inventory_write task must have successfully written the current
# runtime contract into the constrained Delta tables. Verify that this was a real
# write, not merely a successful setup/schema compilation.
written_counts = {
    table: spark.table(f"{CATALOG_SCHEMA}.{table}").count()
    for table in (
        "governance_silver_collection_run",
        "governance_silver_requirement_snapshot",
        "governance_silver_asset_inventory_snapshot",
    )
}
for table, row_count in written_counts.items():
    if row_count == 0:
        failures.append(f"{table}: inventory write produced no rows")
    else:
        print(f"PASS {table}: inventory wrote {row_count} rows")

if failures:
    raise AssertionError("Schema smoke test failed:\n\n" + "\n\n".join(failures))

print(
    f"PASS: validated {len(EXPECTED_COLUMNS)} schemas and inventory writes "
    f"in {CATALOG_SCHEMA}"
)
