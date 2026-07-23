"""Pure Slack Block Kit builder shared by per-silo routers and tests.

No Slack client, no network — just constructs the Block Kit payload from
plain values. Tests render with this exact function to verify the on-the-wire
shape produced by 03 is what they expect.
"""

import re

_USER_ID_RE = re.compile(r"^U[A-Z0-9]{6,}$")


def is_user_id(s):
    return bool(s) and bool(_USER_ID_RE.match(s))


def format_dollars(x):
    return f"${x:,.0f}" if x is not None else "?"


def build_cost_alert_blocks(
    recipient,
    cluster_name,
    cluster_id,
    formatted_dollars,
    formatted_percent,
    severity,
    creator,
    workspace_host,
    creator_user_id=None,
):
    """Block Kit blocks for a single-cluster cost alert.

    `recipient` is a Slack user ID (DM mode) or channel ID/name (channel mode).
    `creator_user_id` is the owner's Slack user ID for @-mentioning them in
    channel mode. Falls back to the email in inline code when None.
    """
    severity_emoji = ":rotating_light:" if severity == "CRITICAL" else ":warning:"

    if is_user_id(recipient):
        greeting = f"Hi <@{recipient}>, your"
    else:
        if creator_user_id:
            owner_tag = f"<@{creator_user_id}>"
        elif creator:
            owner_tag = f"`{creator}`"
        else:
            owner_tag = "_unknown owner_"
        greeting = f"Hi team — cluster owned by {owner_tag}: their"

    return [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"{severity_emoji} Cluster Cost Alert ({severity})"},
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{greeting} all-purpose compute cluster *{cluster_name}* "
                    f"is currently projected to cost *{formatted_dollars}/day*, "
                    f"which is *{formatted_percent}* greater than the workspace average."
                ),
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "This is well outside the norm and may indicate an oversized "
                    "or long-running cluster. Please review the cluster "
                    "configuration and right-size or terminate if appropriate. "
                    "If you think this is a validly sized workload, reach out to "
                    "your workspace admins to get this cluster exempted from notifications."
                ),
            },
        },
        {
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": "Review cluster in Databricks"},
                "url": f"https://{workspace_host}/compute/clusters/{cluster_id}",
                "style": "primary",
            }],
        },
        {
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": (
                    "Sent by `databricks-finops-governance` — reach out to your Databricks "
                    "account team or workspace admins if you need help right-sizing."
                ),
            }],
        },
    ]
