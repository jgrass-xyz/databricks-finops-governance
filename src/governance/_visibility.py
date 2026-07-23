"""SDK helpers for the compact asset-visibility pipeline."""


def _as_dict(value):
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    return value.as_dict()


def _manager_principal_refs(rule_set):
    """Return direct principals granted roles/servicePrincipal.manager."""
    raw = _as_dict(rule_set)
    grants = raw.get("grant_rules") or raw.get("grantRules") or []
    refs = []
    for grant in grants:
        grant = _as_dict(grant)
        if grant.get("role") != "roles/servicePrincipal.manager":
            continue
        refs.extend(str(value) for value in (grant.get("principals") or []))
    return sorted(set(refs))


def _resolve_account_principal(account_client, reference):
    """Resolve one direct rule-set principal; groups are never expanded."""
    parts = reference.strip("/").split("/")
    if len(parts) != 2:
        raise ValueError(f"Unsupported account principal reference: {reference}")
    kind, principal_id = parts
    surfaces = {
        "users": ("USER", "users"),
        "groups": ("GROUP", "groups"),
        "servicePrincipals": ("SERVICE_PRINCIPAL", "service_principals"),
        "service_principals": ("SERVICE_PRINCIPAL", "service_principals"),
    }
    if kind not in surfaces:
        raise ValueError(f"Unsupported account principal type: {kind}")
    owner_type, surface = surfaces[kind]
    raw = _as_dict(getattr(account_client, surface).get(principal_id))
    name = (
        raw.get("display_name") or raw.get("displayName")
        or raw.get("user_name") or raw.get("userName")
        or raw.get("application_id") or raw.get("applicationId")
    )
    return {"id": str(raw.get("id") or principal_id), "name": name, "type": owner_type}


def resolve_direct_owners(account_client, account_id, application_id):
    """Resolve direct manager grants through the account access-control rule set."""
    if account_client is None:
        return [], "UNAVAILABLE", "AccountClient credentials are unavailable"
    if not account_id:
        return [], "UNAVAILABLE", "Databricks account ID is unavailable"
    if not application_id:
        return [], "UNAVAILABLE", "Service-principal application ID is unavailable"
    access_control = getattr(account_client, "access_control", None)
    if access_control is None:
        return [], "UNAVAILABLE", "AccountClient.access_control is unavailable"

    name = (
        f"accounts/{account_id}/servicePrincipals/{application_id}/ruleSets/default"
    )
    try:
        rule_set = access_control.get_rule_set(name=name, etag="")
        owners = [
            _resolve_account_principal(account_client, reference)
            for reference in _manager_principal_refs(rule_set)
        ]
        return sorted(owners, key=lambda value: (value["type"], value["id"])), "RESOLVED", None
    except Exception as error:
        # Missing account auth, permissions, unsupported clouds, and an unresolvable
        # direct grant are all visible rather than silently becoming "no owners".
        return [], "ERROR", f"{type(error).__name__}: {error}"


def collect_service_principals(workspace_client, account_client=None, account_id=None):
    """Collect workspace SP inventory and account-level direct manager grants."""
    principal_api = workspace_client.service_principals
    listed_principals = list(principal_api.list())

    rows = []
    for listed in listed_principals:
        raw = _as_dict(listed)
        principal_id = raw.get("id")
        try:
            raw = _as_dict(principal_api.get(str(principal_id)))
        except Exception:
            pass
        application_id = raw.get("application_id") or raw.get("applicationId")
        owners, status, error = resolve_direct_owners(
            account_client, account_id, application_id)
        rows.append({
            "service_principal_id": str(principal_id),
            "application_id": application_id,
            "display_name": raw.get("display_name") or raw.get("displayName"),
            "active": bool(raw.get("active", True)),
            "direct_owners": owners,
            "owner_resolution_status": status,
            "owner_resolution_error": error,
        })
    return rows


def principal_aliases(principal):
    """Normalized identifiers accepted when matching an asset owner/run-as value."""
    return {
        str(value).strip().casefold()
        for value in (
            principal.get("service_principal_id"),
            principal.get("application_id"),
            principal.get("display_name"),
        )
        if value is not None and str(value).strip()
    }


def match_owner_to_principal(owner, principals):
    """Return one deterministic direct identity match, or None."""
    key = str(owner or "").strip().casefold()
    matches = [p for p in principals if key and key in principal_aliases(p)]
    if not matches:
        return None
    return sorted(matches, key=lambda p: p["service_principal_id"])[0]
