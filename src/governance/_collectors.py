"""Prebuilt asset collectors enabled through config/governance_assets.py."""

import json

from _normalize import asset_record


def _policy(policy_type, policy_id, policy_name=None):
    if not policy_id:
        return None
    return {
        "policy_type": policy_type,
        "policy_id": str(policy_id),
        "policy_name": policy_name,
    }


def _nested_compute_policy_ids(settings):
    policy_ids = set()
    for job_cluster in settings.get("job_clusters") or []:
        policy_id = (job_cluster.get("new_cluster") or {}).get("policy_id")
        if policy_id:
            policy_ids.add(str(policy_id))

    def visit_task(task):
        policy_id = (task.get("new_cluster") or {}).get("policy_id")
        if policy_id:
            policy_ids.add(str(policy_id))
        nested = (task.get("for_each_task") or {}).get("task")
        if nested:
            visit_task(nested)

    for task in settings.get("tasks") or []:
        visit_task(task)
    return sorted(policy_ids)


def collect_clusters(client, context):
    rows = []
    for cluster in client.clusters.list():
        raw = cluster.as_dict()
        source = str(raw.get("cluster_source") or "").upper()
        if source not in {"UI", "API"}:
            continue
        policies = [_policy("COMPUTE_POLICY", raw.get("policy_id"))]
        rows.append(asset_record(
            **context,
            asset_id=raw.get("cluster_id"),
            asset_name=raw.get("cluster_name"),
            owner=raw.get("creator_user_name"),
            lifecycle_state=str(raw.get("state") or "UNKNOWN"),
            tags=raw.get("custom_tags"),
            policies=[p for p in policies if p],
            raw_payload=json.dumps(raw, sort_keys=True, default=str),
        ))
    return rows


def collect_jobs(client, context):
    rows = []
    for summary in client.jobs.list(expand_tasks=True):
        raw = summary.as_dict()
        # API 2.2 expands normal jobs in the list response. Only unusually large
        # jobs need the paginated get call, avoiding an N+1 request per workspace job.
        if raw.get("has_more"):
            raw = client.jobs.get(summary.job_id).as_dict()
        settings = raw.get("settings") or {}
        policies = [
            _policy(
                "BUDGET_POLICY",
                raw.get("effective_budget_policy_id") or settings.get("budget_policy_id"),
            ),
            _policy(
                "USAGE_POLICY",
                raw.get("effective_usage_policy_id") or settings.get("usage_policy_id"),
            ),
        ]
        policies.extend(
            _policy("COMPUTE_POLICY", policy_id)
            for policy_id in _nested_compute_policy_ids(settings)
        )
        pause_status = ((raw.get("trigger_state") or {}).get("pause_status"))
        rows.append(asset_record(
            **context,
            asset_id=raw.get("job_id"),
            asset_name=settings.get("name"),
            owner=raw.get("run_as_user_name") or raw.get("creator_user_name"),
            lifecycle_state=str(pause_status or "ACTIVE"),
            tags=settings.get("tags"),
            policies=[p for p in policies if p],
            raw_payload=json.dumps(raw, sort_keys=True, default=str),
        ))
    return rows


def collect_serving_endpoints(client, context):
    rows = []
    for endpoint in client.serving_endpoints.list():
        raw = endpoint.as_dict()
        policies = [
            _policy("BUDGET_POLICY", raw.get("budget_policy_id")),
            _policy("USAGE_POLICY", raw.get("usage_policy_id")),
        ]
        state = raw.get("state") or {}
        lifecycle_state = state.get("ready") or state.get("config_update") or "UNKNOWN"
        # Endpoint name is the billing-stable key exposed as usage_metadata.endpoint_name.
        rows.append(asset_record(
            **context,
            asset_id=raw.get("name"),
            asset_name=raw.get("name"),
            owner=raw.get("creator"),
            lifecycle_state=str(lifecycle_state),
            tags=raw.get("tags"),
            policies=[p for p in policies if p],
            raw_payload=json.dumps(raw, sort_keys=True, default=str),
        ))
    return rows


COLLECTORS = {
    "clusters": collect_clusters,
    "jobs": collect_jobs,
    "serving_endpoints": collect_serving_endpoints,
}


COST_RESOLVERS = {
    "cluster": {
        "asset_id_sql": "u.usage_metadata.cluster_id",
        "extra_filter_sql": (
            "u.billing_origin_product = 'ALL_PURPOSE' "
            "AND u.usage_metadata.cluster_id IS NOT NULL"
        ),
    },
    "job": {
        "asset_id_sql": "u.usage_metadata.job_id",
        "extra_filter_sql": "u.usage_metadata.job_id IS NOT NULL",
    },
    "serving_endpoint": {
        "asset_id_sql": "u.usage_metadata.endpoint_name",
        "extra_filter_sql": (
            "u.billing_origin_product = 'MODEL_SERVING' "
            "AND u.usage_metadata.endpoint_name IS NOT NULL"
        ),
    },
}
