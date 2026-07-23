"""Pure helpers for the compact asset-visibility pipeline.

Principal inventory intentionally comes only from Databricks SDK APIs.  The
AccountClient manager surface is optional because it is not available in every
SDK version, cloud, or credential context.
"""


def _as_dict(value):
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    return value.as_dict()


def _owner_values(payload):
    """Extract direct manager/owner identities without depending on SDK models."""
    values = payload if isinstance(payload, list) else [payload]
    owners = []
    for value in values:
        raw = _as_dict(value)
        # Accommodate singular/plural response shapes across SDK generations.
        candidates = raw.get("owners") or raw.get("managers") or [
            raw.get("owner") or raw.get("manager") or raw
        ]
        if not isinstance(candidates, list):
            candidates = [candidates]
        for candidate in candidates:
            item = _as_dict(candidate)
            identity = (
                item.get("display_name") or item.get("displayName")
                or item.get("user_name") or item.get("userName")
                or item.get("application_id") or item.get("applicationId")
                or item.get("id")
            )
            if identity:
                owners.append(str(identity))
    return sorted(set(owners))


def resolve_direct_owners(account_client, principal_id):
    """Return (owners, status, error), visibly degrading unsupported manager APIs."""
    if account_client is None:
        return [], "UNAVAILABLE", "AccountClient credentials are unavailable"
    manager_api = getattr(account_client, "service_principal_manager", None)
    if manager_api is None:
        return [], "UNAVAILABLE", "AccountClient.service_principal_manager is unavailable"
    method = getattr(manager_api, "get", None) or getattr(manager_api, "list", None)
    if method is None:
        return [], "UNAVAILABLE", "Service-principal manager get/list API is unavailable"
    try:
        try:
            response = method(service_principal_id=str(principal_id))
        except TypeError:
            response = method(str(principal_id))
        return _owner_values(response), "RESOLVED", None
    except Exception as error:  # permissions and preview/API availability vary by account
        return [], "ERROR", f"{type(error).__name__}: {error}"


def collect_service_principals(workspace_client, account_client=None):
    """Collect current principals via account SDK, falling back to workspace SDK."""
    principal_api = getattr(account_client, "service_principals", None)
    try:
        listed_principals = list(principal_api.list()) if principal_api else None
    except Exception:
        # Account auth is commonly absent in workspace jobs. Workspace SCIM remains
        # SDK-only and gives the principals visible/assignable in this workspace.
        listed_principals = None
    if listed_principals is None:
        principal_api = workspace_client.service_principals
        listed_principals = list(principal_api.list())

    rows = []
    for listed in listed_principals:
        raw = _as_dict(listed)
        principal_id = raw.get("id")
        # Get commonly supplies active/applicationId when list is projection-limited.
        try:
            raw = _as_dict(principal_api.get(str(principal_id)))
        except Exception:
            pass
        owners, status, error = resolve_direct_owners(account_client, principal_id)
        rows.append({
            "service_principal_id": str(principal_id),
            "application_id": raw.get("application_id") or raw.get("applicationId"),
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
