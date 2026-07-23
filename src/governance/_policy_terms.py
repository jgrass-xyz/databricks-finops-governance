"""Helpers for turning compute policy JSON into dashboard-friendly rows."""

import json


def parse_definition(definition):
    if not definition:
        return {}
    if isinstance(definition, dict):
        return definition
    try:
        value = json.loads(definition)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def merge_policy_definitions(base_definition, overrides):
    """Apply policy-family overrides to the inherited definition by rule path."""
    merged = parse_definition(base_definition).copy()
    merged.update(parse_definition(overrides))
    return merged


def flatten_policy_terms(definition):
    """Return one row per policy path while preserving the complete rule JSON."""
    rows = []
    for path, rule in sorted(parse_definition(definition).items()):
        rule_dict = rule if isinstance(rule, dict) else {"value": rule}
        rows.append({
            "definition_path": path,
            "rule_type": rule_dict.get("type", "UNKNOWN"),
            "rule_json": json.dumps(rule_dict, sort_keys=True, default=str),
            "hidden": bool(rule_dict.get("hidden", False)),
            "is_optional": bool(rule_dict.get("isOptional", False)),
        })
    return rows
