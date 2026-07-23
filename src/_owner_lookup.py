"""Resolve owner emails to Slack user IDs.

Shared across detectors so each silo's router gets identical routing semantics:
- valid email + match in Slack workspace → Slack user ID (suitable for DM)
- valid email + no Slack match → None (caller falls back to a channel)
- empty/None/garbage input → None
"""

from _slack import post_form, SlackError


def lookup_user_id(token, email):
    """Resolve an email to a Slack user ID.

    Returns ``None`` if the email is missing, malformed, or doesn't match
    a user in the Slack workspace. Re-raises any other SlackError.
    """
    if not email or "@" not in email:
        return None
    try:
        return post_form(token, "users.lookupByEmail", {"email": email})["user"]["id"]
    except SlackError as e:
        if e.body.get("error") == "users_not_found":
            return None
        raise
