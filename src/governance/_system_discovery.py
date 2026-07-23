"""System-table asset discovery for governance inventory.

System tables provide account-complete, permission-independent history. Workspace
APIs are merged later as enrichment and never define the asset universe.
"""

import json

from _normalize import asset_record


def _literal(value):
    return str(value).replace("'", "''")


def _cluster_sql(workspace_id):
    return f"""
      SELECT cluster_id, cluster_name, owned_by, tags, cluster_source, policy_id,
             create_time, delete_time, change_time
      FROM system.compute.clusters
      WHERE workspace_id = '{_literal(workspace_id)}'
        AND UPPER(cluster_source) IN ('UI', 'API')
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC
      ) = 1
        AND delete_time IS NULL
    """


def _job_sql(workspace_id):
    return f"""
      SELECT job_id, name,
             COALESCE(run_as_user_name, creator_user_name, run_as, creator_id) AS owner,
             tags, paused, create_time, delete_time, change_time
      FROM system.lakeflow.jobs
      WHERE workspace_id = '{_literal(workspace_id)}'
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY workspace_id, job_id ORDER BY change_time DESC
      ) = 1
        AND delete_time IS NULL
    """


def _serving_endpoint_sql(workspace_id):
    return f"""
      SELECT endpoint_name, endpoint_id, created_by, endpoint_config_version,
             endpoint_delete_time, change_time
      FROM system.serving.served_entities
      WHERE workspace_id = '{_literal(workspace_id)}'
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY workspace_id, endpoint_name
        ORDER BY change_time DESC, endpoint_config_version DESC
      ) = 1
        AND endpoint_delete_time IS NULL
    """


def _warehouse_sql(workspace_id):
    return f"""
      SELECT warehouse_id, warehouse_name, warehouse_type, warehouse_size,
             created_by, tags, change_time, delete_time
      FROM system.compute.warehouses
      WHERE workspace_id = '{_literal(workspace_id)}'
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY workspace_id, warehouse_id ORDER BY change_time DESC
      ) = 1
        AND delete_time IS NULL
    """


DISCOVERY_SQL = {
    "clusters": _cluster_sql,
    "jobs": _job_sql,
    "serving_endpoints": _serving_endpoint_sql,
    "warehouses": _warehouse_sql,
}


def _raw(row):
    return json.dumps(row, sort_keys=True, default=str)


def discover_clusters(spark, context):
    assets = []
    for row in spark.sql(_cluster_sql(context["workspace_id"])).collect():
        value = row.asDict(recursive=True)
        policies = []
        if value.get("policy_id"):
            policies.append({
                "policy_type": "COMPUTE_POLICY",
                "policy_id": str(value["policy_id"]),
                "policy_name": None,
            })
        assets.append(asset_record(
            **context,
            asset_id=value["cluster_id"],
            asset_name=value.get("cluster_name"),
            owner=value.get("owned_by"),
            lifecycle_state="ACTIVE",
            tags=value.get("tags"),
            policies=policies,
            discovery_source="SYSTEM_TABLE",
            api_enriched=False,
            tag_observation_complete=True,
            policy_observation_complete=True,
            raw_payload=_raw(value),
        ))
    return assets


def discover_jobs(spark, context):
    assets = []
    for row in spark.sql(_job_sql(context["workspace_id"])).collect():
        value = row.asDict(recursive=True)
        assets.append(asset_record(
            **context,
            asset_id=value["job_id"],
            asset_name=value.get("name"),
            owner=value.get("owner"),
            lifecycle_state="PAUSED" if value.get("paused") else "ACTIVE",
            tags=value.get("tags"),
            policies=[],
            discovery_source="SYSTEM_TABLE",
            api_enriched=False,
            tag_observation_complete=True,
            policy_observation_complete=False,
            raw_payload=_raw(value),
        ))
    return assets


def discover_serving_endpoints(spark, context):
    assets = []
    for row in spark.sql(_serving_endpoint_sql(context["workspace_id"])).collect():
        value = row.asDict(recursive=True)
        assets.append(asset_record(
            **context,
            asset_id=value["endpoint_name"],
            asset_name=value.get("endpoint_name"),
            owner=value.get("created_by"),
            lifecycle_state="ACTIVE",
            tags={},
            policies=[],
            discovery_source="SYSTEM_TABLE",
            api_enriched=False,
            tag_observation_complete=False,
            policy_observation_complete=False,
            raw_payload=_raw(value),
        ))
    return assets


def discover_warehouses(spark, context):
    assets = []
    for row in spark.sql(_warehouse_sql(context["workspace_id"])).collect():
        value = row.asDict(recursive=True)
        assets.append(asset_record(
            **context,
            asset_id=value["warehouse_id"],
            asset_name=value.get("warehouse_name"),
            owner=value.get("created_by"),
            lifecycle_state="ACTIVE",
            tags=value.get("tags"),
            policies=[],
            discovery_source="SYSTEM_TABLE",
            api_enriched=False,
            tag_observation_complete=True,
            # SQL warehouses expose no policy attachment; nothing to observe.
            policy_observation_complete=True,
            raw_payload=_raw(value),
        ))
    return assets


SYSTEM_DISCOVERERS = {
    "clusters": discover_clusters,
    "jobs": discover_jobs,
    "serving_endpoints": discover_serving_endpoints,
    "warehouses": discover_warehouses,
}
