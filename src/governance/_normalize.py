"""Pure normalization and requirement-evaluation helpers."""

import json


def normalize_tags(tags):
    """Normalize SDK map or key/value-list tag representations."""
    if isinstance(tags, list):
        tags = {
            item.get("key"): item.get("value", "")
            for item in tags
            if isinstance(item, dict) and item.get("key")
        }
    return {
        str(key).strip(): str(value).strip()
        for key, value in (tags or {}).items()
        if key is not None and str(key).strip() and value is not None
    }


def normalize_policies(policies):
    """Deduplicate policies by type and ID while preserving a stable order."""
    unique = {}
    for policy in policies or []:
        policy_type = str(policy.get("policy_type") or "").strip().upper()
        policy_id = str(policy.get("policy_id") or "").strip()
        if not policy_type or not policy_id:
            continue
        unique[(policy_type, policy_id)] = {
            "policy_type": policy_type,
            "policy_id": policy_id,
            "policy_name": policy.get("policy_name"),
        }
    return [unique[key] for key in sorted(unique)]


def evaluate_requirements(
    tags, policies, required_tags, required_policies,
    tag_observation_complete=True, policy_observation_complete=True,
):
    """Evaluate the observed state against requirements active for that snapshot."""
    observed_tag_keys = {
        key.casefold()
        for key, value in normalize_tags(tags).items()
        if value
    }
    required_tag_keys = [
        str(key).strip().casefold() for key in required_tags or [] if str(key).strip()
    ]
    missing_tags = [key for key in required_tag_keys if key not in observed_tag_keys]

    observed_policy_types = {
        policy["policy_type"] for policy in normalize_policies(policies)
    }
    required_policy_types = [
        str(value).strip().upper()
        for value in required_policies or []
        if str(value).strip()
    ]
    missing_policies = [
        value for value in required_policy_types if value not in observed_policy_types
    ]

    def status(required, missing, observation_complete):
        if not required:
            return "NOT_CONFIGURED"
        if not observation_complete:
            return "UNKNOWN"
        if not missing:
            return "APPLIED"
        if len(missing) < len(required):
            return "PARTIAL"
        return "NOT_APPLIED"

    return {
        "tag_status": status(
            required_tag_keys, missing_tags, tag_observation_complete),
        "missing_required_tags": missing_tags if tag_observation_complete else None,
        "policy_status": status(
            required_policy_types, missing_policies, policy_observation_complete),
        "missing_required_policies": (
            missing_policies if policy_observation_complete else None),
    }


def asset_record(
    *, workspace_id, run_id, snapshot_ts, product, asset_type, asset_id,
    asset_name, owner, lifecycle_state, tags, policies, raw_payload,
    discovery_source="API", api_enriched=True,
    tag_observation_complete=True, policy_observation_complete=True,
):
    """Build the common Silver inventory row emitted by every collector."""
    return {
        "workspace_id": str(workspace_id),
        "collection_run_id": run_id,
        "snapshot_ts": snapshot_ts,
        "product": product,
        "asset_type": asset_type,
        "asset_id": str(asset_id),
        "asset_name": str(asset_name or asset_id),
        "owner": owner,
        "lifecycle_state": lifecycle_state,
        "tags": normalize_tags(tags),
        "policies": normalize_policies(policies),
        "discovery_source": discovery_source,
        "api_enriched": bool(api_enriched),
        "tag_observation_complete": bool(tag_observation_complete),
        "policy_observation_complete": bool(policy_observation_complete),
        "raw_payload": raw_payload,
    }


def merge_discovery_and_enrichment(discovered, enriched):
    """Merge system-table discovery with permission-scoped API enrichment.

    System tables establish the asset universe. API-only assets are retained to
    cover system-table publication lag, but API visibility never removes an asset.
    """
    merged = {(row["asset_type"], row["asset_id"]): dict(row) for row in discovered}
    for api_row in enriched:
        key = (api_row["asset_type"], api_row["asset_id"])
        if key not in merged:
            row = dict(api_row)
            row["discovery_source"] = "API_ONLY"
            row["api_enriched"] = True
            row["tag_observation_complete"] = True
            row["policy_observation_complete"] = True
            merged[key] = row
            continue

        row = merged[key]
        row["asset_name"] = row.get("asset_name") or api_row.get("asset_name")
        row["owner"] = row.get("owner") or api_row.get("owner")
        row["lifecycle_state"] = (
            api_row.get("lifecycle_state") or row.get("lifecycle_state")
        )
        row["tags"] = normalize_tags({
            **normalize_tags(row.get("tags")),
            **normalize_tags(api_row.get("tags")),
        })
        row["policies"] = normalize_policies(
            list(row.get("policies") or []) + list(api_row.get("policies") or [])
        )
        row["api_enriched"] = True
        row["tag_observation_complete"] = True
        row["policy_observation_complete"] = True
        row["raw_payload"] = json.dumps({
            "system_table": row.get("raw_payload"),
            "api": api_row.get("raw_payload"),
        }, sort_keys=True)
    return [merged[key] for key in sorted(merged)]
