# Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""SAS Identities (users & groups) tools for the SAS Viya MCP server.

These tools wrap the SAS Viya **Identities** REST API (``/identities``) — the
same service the Environment Manager "Users" and "Groups" views drive (captured
in ``ID_har.har``). They let the assistant answer *who am I*, look up and search
users and groups, inspect a user's group memberships and a group's members, and
— for administrators — create/delete custom groups and manage their membership.

Auth model is identical to the other tool modules: every tool resolves the
caller's Viya access token via ``get_token(ctx)`` and calls ``/identities`` **as
that user**. Read operations work for any authenticated user. The write
operations (create/delete group, add/remove member) require membership in
``SASAdministrators``; the service returns HTTP 403 otherwise, which these tools
surface as a clear "administrator privileges required" message rather than a raw
error.

Response shapes (confirmed against a live Viya environment):
  * user    → ``{id, name, providerId, type, state, ...}``
  * group   → ``{id, name, providerId, type, description, state, creation/modifiedTimeStamp}``
  * The verbose HATEOAS ``links`` array on every object is stripped before
    returning, so the assistant gets a compact, readable payload.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastmcp import Context, FastMCP

from .config import VIYA_ENDPOINT
from .viya_client import get_json, get_paged_items, logger, make_client

# The /administrator sub-resource ONLY accepts this media type — a plain
# ``application/json`` Accept header returns HTTP 406.
_BOOL_ACCEPT = "application/vnd.sas.primitive.boolean+json"

# Scalar fields worth surfacing from a user / group object, in a friendly order.
_USER_FIELDS = ("id", "name", "title", "providerId", "type", "state")
_GROUP_FIELDS = (
    "id", "name", "description", "providerId", "type", "state",
    "creationTimeStamp", "modifiedTimeStamp",
)


def _trim(item: dict[str, Any]) -> dict[str, Any]:
    """Drop the noisy HATEOAS ``links`` array from an identity object."""
    return {k: v for k, v in item.items() if k != "links"}


def _q(value: str) -> str:
    """Escape a single quote for embedding a literal in a SAS filter expression."""
    return value.replace("'", "''")


def _admin_required(action: str, status: int) -> dict[str, Any]:
    """Standard message for a write blocked by Viya authorization."""
    return {
        "status": "forbidden",
        "httpStatusCode": status,
        "message": (
            f"Could not {action}: the SAS Identities service rejected the request "
            f"(HTTP {status}). Managing users and groups requires administrator "
            "privileges (membership in the SASAdministrators group). Check "
            "`sas_whoami` — if isAdministrator is false you cannot perform this "
            "operation."
        ),
    }


def register_id_tools(
    mcp: FastMCP, get_token: Callable[[Context], Awaitable[str]]
) -> None:
    """Register every SAS Identities (user/group) tool on *mcp*.

    *get_token* resolves the caller's Viya access token — the bearer-header swap
    in HTTP mode, or the cached CLI token in stdio mode — exactly as the core and
    VA tool modules use it.
    """

    @asynccontextmanager
    async def session(name: str, ctx: Context) -> AsyncIterator[httpx.AsyncClient]:
        logger.info("--- TOOL USED: %s ---", name)
        token = await get_token(ctx)
        async with make_client(token) as client:
            yield client

    # ── Identity / current user ───────────────────────────────────────────────

    @mcp.tool(
        name="sas_whoami",
        description=(
            "Return the SAS Viya identity of the CURRENT user (the caller): their "
            "user id, display name, identity provider, account state, whether they "
            "are a SAS administrator, and the groups they directly belong to. Use "
            "this to answer 'who am I', 'what's my SAS user id', 'am I an admin', or "
            "'which groups am I in'."
        ),
    )
    async def sas_whoami(ctx: Context) -> dict[str, Any]:
        async with session("sas_whoami", ctx) as client:
            me = await get_json("/identities/users/@currentUser", client)
            uid = me.get("id", "")
            is_admin = False
            try:
                r = await client.get(
                    f"{VIYA_ENDPOINT}/identities/users/@currentUser/administrator",
                    headers={"Accept": _BOOL_ACCEPT},
                    follow_redirects=True,
                )
                is_admin = r.status_code == 200 and r.text.strip().lower() == "true"
            except httpx.HTTPError:
                pass
            groups: list[dict[str, Any]] = []
            if uid:
                try:
                    items, _ = await get_paged_items(
                        f"/identities/users/{uid}/memberships", client, limit=200
                    )
                    groups = [
                        {"id": g.get("id"), "name": g.get("name")} for g in items
                    ]
                except httpx.HTTPError:
                    pass
            return {
                "id": uid,
                "name": me.get("name"),
                "providerId": me.get("providerId"),
                "state": me.get("state"),
                "isAdministrator": is_admin,
                "groups": groups,
            }

    # ── Users ─────────────────────────────────────────────────────────────────

    @mcp.tool(
        name="sas_list_users",
        description=(
            "List or search SAS Viya users. With no arguments, returns a page of all "
            "users. Pass `name_contains` to search by display name (substring, "
            "case-insensitive), or `user_id` to look up an exact user id. Each user "
            "has id, name, providerId, type and state. Use `limit`/`start` to page "
            "through large directories."
        ),
    )
    async def sas_list_users(
        ctx: Context,
        name_contains: str | None = None,
        user_id: str | None = None,
        limit: int = 50,
        start: int = 0,
    ) -> dict[str, Any]:
        if user_id:
            filters: str | None = f"eq(id,'{_q(user_id)}')"
        elif name_contains:
            filters = f"contains(name,'{_q(name_contains)}')"
        else:
            filters = None
        async with session("sas_list_users", ctx) as client:
            items, count = await get_paged_items(
                "/identities/users", client, limit=limit, start=start, filters=filters
            )
            return {
                "count": count,
                "start": start,
                "limit": limit,
                "users": [_trim(u) for u in items],
            }

    @mcp.tool(
        name="sas_get_user",
        description=(
            "Get the full details of one SAS Viya user by their user id (e.g. 'yash'). "
            "Returns id, name, identity provider and account state. Returns a "
            "not_found status if no such user exists."
        ),
    )
    async def sas_get_user(user_id: str, ctx: Context) -> dict[str, Any]:
        async with session("sas_get_user", ctx) as client:
            try:
                user = await get_json(f"/identities/users/{user_id}", client)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return {
                        "status": "not_found",
                        "user_id": user_id,
                        "message": f"No SAS user with id '{user_id}'.",
                    }
                raise
            return _trim(user)

    @mcp.tool(
        name="sas_get_user_memberships",
        description=(
            "List the groups a SAS Viya user belongs to, given their user id. By "
            "default returns the user's DIRECT group memberships; set `nested=true` to "
            "also include groups they belong to indirectly (through nested groups). "
            "Use this to answer 'which groups is <user> in' or to check a user's "
            "access via group membership."
        ),
    )
    async def sas_get_user_memberships(
        user_id: str,
        ctx: Context,
        nested: bool = False,
        limit: int = 100,
        start: int = 0,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if nested:
            params["flatten"] = "true"
        async with session("sas_get_user_memberships", ctx) as client:
            try:
                items, count = await get_paged_items(
                    f"/identities/users/{user_id}/memberships",
                    client,
                    limit=limit,
                    start=start,
                    extra_params=params or None,
                )
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return {
                        "status": "not_found",
                        "user_id": user_id,
                        "message": f"No SAS user with id '{user_id}'.",
                    }
                raise
            return {
                "user_id": user_id,
                "nested": nested,
                "count": count,
                "groups": [_trim(g) for g in items],
            }

    # ── Groups ────────────────────────────────────────────────────────────────

    @mcp.tool(
        name="sas_list_groups",
        description=(
            "List or search SAS Viya groups (both built-in groups like "
            "SASAdministrators and custom groups). With no arguments, returns a page "
            "of all groups. Pass `name_contains` to search by group name (substring, "
            "case-insensitive), or `group_id` to look up an exact group id. Each group "
            "has id, name, description, providerId and state."
        ),
    )
    async def sas_list_groups(
        ctx: Context,
        name_contains: str | None = None,
        group_id: str | None = None,
        limit: int = 50,
        start: int = 0,
    ) -> dict[str, Any]:
        if group_id:
            filters: str | None = f"eq(id,'{_q(group_id)}')"
        elif name_contains:
            filters = f"contains(name,'{_q(name_contains)}')"
        else:
            filters = None
        async with session("sas_list_groups", ctx) as client:
            items, count = await get_paged_items(
                "/identities/groups", client, limit=limit, start=start, filters=filters
            )
            return {
                "count": count,
                "start": start,
                "limit": limit,
                "groups": [_trim(g) for g in items],
            }

    @mcp.tool(
        name="sas_get_group",
        description=(
            "Get the full details of one SAS Viya group by its group id (e.g. "
            "'SASAdministrators'). Returns id, name, description, identity provider "
            "and state. Returns a not_found status if no such group exists."
        ),
    )
    async def sas_get_group(group_id: str, ctx: Context) -> dict[str, Any]:
        async with session("sas_get_group", ctx) as client:
            try:
                group = await get_json(f"/identities/groups/{group_id}", client)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return {
                        "status": "not_found",
                        "group_id": group_id,
                        "message": f"No SAS group with id '{group_id}'.",
                    }
                raise
            return _trim(group)

    @mcp.tool(
        name="sas_get_group_members",
        description=(
            "List the members of a SAS Viya group, given its group id. `member_type` "
            "selects what to return: 'all' (default — users AND sub-groups), 'users' "
            "(only user members), or 'groups' (only sub-group members). Set "
            "`nested=true` to include members inherited through nested sub-groups. "
            "Use this to answer 'who is in group <X>'."
        ),
    )
    async def sas_get_group_members(
        group_id: str,
        ctx: Context,
        member_type: str = "all",
        nested: bool = False,
        limit: int = 100,
        start: int = 0,
    ) -> dict[str, Any]:
        sub = {
            "all": "members",
            "users": "userMembers",
            "groups": "groupMembers",
        }.get(member_type.lower())
        if sub is None:
            return {
                "status": "invalid_member_type",
                "message": "member_type must be one of: 'all', 'users', 'groups'.",
            }
        params: dict[str, Any] = {}
        if nested:
            params["flatten"] = "true"
        async with session("sas_get_group_members", ctx) as client:
            try:
                items, count = await get_paged_items(
                    f"/identities/groups/{group_id}/{sub}",
                    client,
                    limit=limit,
                    start=start,
                    extra_params=params or None,
                )
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return {
                        "status": "not_found",
                        "group_id": group_id,
                        "message": f"No SAS group with id '{group_id}'.",
                    }
                raise
            return {
                "group_id": group_id,
                "member_type": member_type.lower(),
                "nested": nested,
                "count": count,
                "members": [_trim(m) for m in items],
            }

    # ── Group administration (requires SASAdministrators) ─────────────────────

    @mcp.tool(
        name="sas_create_group",
        description=(
            "Create a new CUSTOM SAS Viya group (admin only). Provide a `group_id` "
            "(unique identifier, no spaces) and a display `name`; `description` is "
            "optional. Requires administrator privileges — returns a 'forbidden' "
            "status if the caller is not a SAS administrator. Built-in/LDAP groups "
            "cannot be created this way; only local custom groups."
        ),
    )
    async def sas_create_group(
        group_id: str, name: str, ctx: Context, description: str = ""
    ) -> dict[str, Any]:
        body = {"id": group_id, "name": name, "description": description}
        async with session("sas_create_group", ctx) as client:
            resp = await client.post(
                f"{VIYA_ENDPOINT}/identities/groups",
                json=body,
                headers={
                    "Content-Type": "application/vnd.sas.identity.group+json",
                    "Accept": "application/vnd.sas.identity.group+json",
                },
            )
            if resp.status_code == 403:
                return _admin_required(f"create group '{group_id}'", 403)
            if resp.status_code == 409:
                return {
                    "status": "already_exists",
                    "group_id": group_id,
                    "message": f"A group with id '{group_id}' already exists.",
                }
            resp.raise_for_status()
            created = resp.json() if resp.content else body
            return {"status": "created", **_trim(created)}

    @mcp.tool(
        name="sas_delete_group",
        description=(
            "Delete a CUSTOM SAS Viya group by its group id (admin only). This is "
            "irreversible. Requires administrator privileges — returns a 'forbidden' "
            "status if the caller is not a SAS administrator, or 'not_found' if no "
            "such group exists."
        ),
    )
    async def sas_delete_group(group_id: str, ctx: Context) -> dict[str, Any]:
        async with session("sas_delete_group", ctx) as client:
            resp = await client.delete(
                f"{VIYA_ENDPOINT}/identities/groups/{group_id}"
            )
            if resp.status_code == 403:
                return _admin_required(f"delete group '{group_id}'", 403)
            if resp.status_code == 404:
                return {
                    "status": "not_found",
                    "group_id": group_id,
                    "message": f"No SAS group with id '{group_id}'.",
                }
            resp.raise_for_status()
            return {"status": "deleted", "group_id": group_id}

    @mcp.tool(
        name="sas_add_user_to_group",
        description=(
            "Add a user as a member of a SAS Viya group (admin only). Provide the "
            "`group_id` and the `user_id` to add. Requires administrator privileges — "
            "returns a 'forbidden' status if the caller is not a SAS administrator. "
            "Typically used on custom groups."
        ),
    )
    async def sas_add_user_to_group(
        group_id: str, user_id: str, ctx: Context
    ) -> dict[str, Any]:
        async with session("sas_add_user_to_group", ctx) as client:
            resp = await client.put(
                f"{VIYA_ENDPOINT}/identities/groups/{group_id}/userMembers/{user_id}",
                headers={"Accept": "application/vnd.sas.identity.group.member+json"},
            )
            if resp.status_code == 403:
                return _admin_required(
                    f"add user '{user_id}' to group '{group_id}'", 403
                )
            if resp.status_code == 404:
                return {
                    "status": "not_found",
                    "message": (
                        f"Group '{group_id}' or user '{user_id}' was not found."
                    ),
                }
            resp.raise_for_status()
            return {
                "status": "added",
                "group_id": group_id,
                "user_id": user_id,
            }

    @mcp.tool(
        name="sas_remove_user_from_group",
        description=(
            "Remove a user from a SAS Viya group (admin only). Provide the `group_id` "
            "and the `user_id` to remove. Requires administrator privileges — returns "
            "a 'forbidden' status if the caller is not a SAS administrator, or "
            "'not_found' if the membership does not exist."
        ),
    )
    async def sas_remove_user_from_group(
        group_id: str, user_id: str, ctx: Context
    ) -> dict[str, Any]:
        async with session("sas_remove_user_from_group", ctx) as client:
            resp = await client.delete(
                f"{VIYA_ENDPOINT}/identities/groups/{group_id}/userMembers/{user_id}"
            )
            if resp.status_code == 403:
                return _admin_required(
                    f"remove user '{user_id}' from group '{group_id}'", 403
                )
            if resp.status_code == 404:
                return {
                    "status": "not_found",
                    "message": (
                        f"User '{user_id}' is not a member of group '{group_id}' "
                        "(or the group/user does not exist)."
                    ),
                }
            resp.raise_for_status()
            return {
                "status": "removed",
                "group_id": group_id,
                "user_id": user_id,
            }
