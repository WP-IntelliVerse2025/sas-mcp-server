# Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""SAS Model Manager (modelRepository) tools for the SAS Viya MCP server.

These tools were originally part of a separate low-level MCP server
(``VA_SAS_viy_mcp``). They are ported here so a single FastMCP server exposes the
SAS Model Manager model/variable/project tools alongside the core SAS Viya
execution, CAS, catalog, ML, VA and SAS Studio tools.

Auth model: every tool resolves the caller's Viya access token via
``get_token(ctx)`` — the same token source the core and VA/Studio tools use (the
bearer header swapped by ``AuthMiddleware`` in HTTP mode, or the cached CLI token
in stdio mode). With that token it constructs the vendored synchronous
``SASModelManagerClient`` and runs its blocking work in a worker thread
(``asyncio.to_thread``) so the async event loop is never blocked.

Model Manager is plain Viya REST (no compute session), so — like the VA client —
each tool just forwards the per-user Bearer token + base URL.

``sas_model_import`` consumes an ATTACHED model package (.zip). Its bytes arrive
via FastMCP context state (``upload_file_b64`` / ``upload_filename``), routed
there by ``InjectedArgsMiddleware`` (see ``mcp_server.py``) — the same path
``upload_data`` uses — so the model never needs to paste file contents.
"""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Awaitable, Callable
from typing import Any

from fastmcp import Context, FastMCP

from .config import VIYA_ENDPOINT
from .va.model_manager_client import SASModelManagerClient

# ── Client construction ─────────────────────────────────────────────────────────

def _mm(token: str) -> SASModelManagerClient:
    """A request-scoped Model Manager client that runs as the caller (their token)."""
    return SASModelManagerClient(base_url=VIYA_ENDPOINT, access_token=token)


# ─────────────────────────────────────────────────────────────────────────────
# Synchronous workers. Each builds its own client from the token and performs the
# original Model Manager server logic 1:1. They run in a thread (asyncio.to_thread)
# so the FastMCP event loop never blocks on SAS REST calls.
# ─────────────────────────────────────────────────────────────────────────────

def _w_list_repositories(token, limit):
    return _mm(token).list_repositories(limit)


def _w_list_models(token, limit, start, name_filter):
    return _mm(token).list_models(limit=limit, start=start, name_filter=name_filter)


def _w_models_summary(token, group_by):
    return _mm(token).get_models_summary(group_by)


def _w_get_model(token, model_id):
    return _mm(token).get_model(model_id)


def _w_get_model_content(token, model_id, content_id, max_chars):
    return _mm(token).get_model_content(model_id, content_id, max_chars=max_chars)


def _w_list_model_variables(token, model_id):
    return _mm(token).list_model_variables(model_id)


def _w_add_model_variables(token, model_id, variables):
    if not variables:
        return "ERROR: 'variables' must be a non-empty list."
    return _mm(token).add_model_variables(model_id, variables)


def _w_add_variables_from_cas_table(token, a):
    return _mm(token).add_variables_from_cas_table(
        model_id=a["model_id"], caslib=a["cas_library"], table=a["cas_table"],
        server=a.get("cas_server", "cas-shared-default"),
        output_columns=a.get("output_columns"),
        numeric_length=a.get("numeric_length", 12),
    )


def _w_import_model(token, a, file_b64, filename):
    if not file_b64:
        return ("No file was attached. Ask the user to attach a model package "
                "(.zip), then call sas_model_import again.")
    try:
        raw = base64.b64decode(file_b64)
    except Exception as dec_err:
        return f"Could not decode attached file: {dec_err}"
    return _mm(token).import_model(
        file_bytes=raw, name=a["name"], folder_id=a["folder_id"],
        model_type=a.get("model_type", "GENERIC"), filename=filename,
    )


def _w_list_projects(token, limit, start, name_filter):
    return _mm(token).list_projects(limit=limit, start=start, name_filter=name_filter)


def _w_get_project(token, project_id):
    return _mm(token).get_project(project_id)


def _w_projects_summary(token, group_by):
    return _mm(token).get_projects_summary(group_by)


def _w_create_project(token, a):
    return _mm(token).create_project(
        name=a["name"], function=a.get("function", "prediction"),
        description=a.get("description", ""), train_table=a.get("train_table"),
        target_variable=a.get("target_variable", ""),
        repository_id=a.get("repository_id"), folder_id=a.get("folder_id"),
    )


def _w_list_project_models(token, project_id, project_version_id):
    return _mm(token).list_project_models(project_id, project_version_id=project_version_id)


def _w_copy_model_to_project(token, a):
    return _mm(token).copy_model_to_project(
        source_model_id=a["source_model_id"], project_id=a["project_id"],
        project_version_id=a.get("project_version_id"),
        op_code=a.get("op_code", "copy"),
    )


def _w_create_project_with_model(token, a):
    return _mm(token).create_project_with_model(
        name=a["name"], source_model_id=a["source_model_id"],
        function=a.get("function", "prediction"), train_table=a.get("train_table"),
        target_variable=a.get("target_variable", ""), op_code=a.get("op_code", "copy"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Registration
# ─────────────────────────────────────────────────────────────────────────────

def register_model_tools(
    mcp: FastMCP, get_token: Callable[[Context], Awaitable[str]]
) -> None:
    """Register every SAS Model Manager tool on *mcp*.

    *get_token* resolves the caller's Viya access token (HTTP: bearer-header
    swap; stdio: cached CLI token) — identical to the core and VA/Studio tools'
    token source.
    """

    def _err(exc: Exception) -> str:
        return f"ERROR: {type(exc).__name__}: {exc}"

    # ── Models ────────────────────────────────────────────────────────────────

    @mcp.tool(
        name="sas_model_list_repositories",
        description=(
            "List SAS Model Manager model repositories. Returns each repository's "
            "id, name, description, folderId, and whether it is the default "
            "repository. The default repository's folderId is what "
            "sas_model_import needs as folder_id."
        ),
    )
    async def sas_model_list_repositories(ctx: Context, limit: int = 1000) -> Any:
        try:
            return await asyncio.to_thread(_w_list_repositories, await get_token(ctx), limit)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_model_list",
        description=(
            "List models in the SAS Model Manager repository. Returns a summary "
            "(id, name, score-code type, function, repository, modified time) per "
            "model plus the total count. Use name_filter to narrow by name."
        ),
    )
    async def sas_model_list(
        ctx: Context, limit: int = 100, start: int = 0, name_filter: str | None = None
    ) -> Any:
        try:
            return await asyncio.to_thread(
                _w_list_models, await get_token(ctx), limit, start, name_filter)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_model_summary",
        description=(
            "Get aggregate model counts grouped by a field (default "
            "'scoreCodeType', e.g. Python / dataStep / ds2MultiType). Returns rows "
            "like [{\"category\": \"Python\", \"count\": 7}, ...]."
        ),
    )
    async def sas_model_summary(ctx: Context, group_by: str = "scoreCodeType") -> Any:
        try:
            return await asyncio.to_thread(_w_models_summary, await get_token(ctx), group_by)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_model_get",
        description=(
            "Get full metadata for one model by id, including its content files "
            "(each file's id is the content_id for sas_model_get_content), its "
            "input/output variable arrays, and score-code URI."
        ),
    )
    async def sas_model_get(model_id: str, ctx: Context) -> Any:
        try:
            return await asyncio.to_thread(_w_get_model, await get_token(ctx), model_id)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_model_get_content",
        description=(
            "Stream the text of one of a model's content files (e.g. package.json "
            "or score code). Get the content_id from sas_model_get's 'files' list."
        ),
    )
    async def sas_model_get_content(
        model_id: str, content_id: str, ctx: Context, max_chars: int = 2_000_000
    ) -> Any:
        try:
            return await asyncio.to_thread(
                _w_get_model_content, await get_token(ctx), model_id, content_id, max_chars)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_model_list_variables",
        description=(
            "List a model's input/output variables (name, type, role, length, "
            "level, format)."
        ),
    )
    async def sas_model_list_variables(model_id: str, ctx: Context) -> Any:
        try:
            return await asyncio.to_thread(_w_list_model_variables, await get_token(ctx), model_id)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_model_add_variables",
        description=(
            "Add input/output variables to a model. Provide a list of variables; "
            "each has name, type ('string' or 'decimal'), length (integer), and "
            "role ('input' or 'output'). Use this when you already know the exact "
            "variable definitions; to derive them from a CAS table instead, use "
            "sas_model_add_variables_from_cas_table."
        ),
    )
    async def sas_model_add_variables(
        model_id: str, variables: list[dict], ctx: Context
    ) -> Any:
        try:
            return await asyncio.to_thread(
                _w_add_model_variables, await get_token(ctx), model_id, variables)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_model_add_variables_from_cas_table",
        description=(
            "Derive a model's variables from a CAS table's columns and add them in "
            "one step — the end-to-end 'import model data variables' flow. Reads "
            "the table's columns, maps CAS varchar->string (length = column "
            "length) and CAS numeric->decimal, and adds them as input variables. "
            "Pass output_columns to mark target column(s) as role 'output'."
        ),
    )
    async def sas_model_add_variables_from_cas_table(
        model_id: str, cas_library: str, cas_table: str, ctx: Context,
        cas_server: str = "cas-shared-default",
        output_columns: list[str] | None = None, numeric_length: int = 12,
    ) -> Any:
        try:
            return await asyncio.to_thread(_w_add_variables_from_cas_table, await get_token(ctx), {
                "model_id": model_id, "cas_library": cas_library, "cas_table": cas_table,
                "cas_server": cas_server, "output_columns": output_columns,
                "numeric_length": numeric_length})
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_model_import",
        description=(
            "Import a model from an attached model package archive (.zip) into a "
            "model repository folder. The file bytes are supplied automatically by "
            "the host — do NOT paste file contents. Get folder_id from "
            "sas_model_list_repositories (the target repository's folderId)."
        ),
    )
    async def sas_model_import(
        name: str, folder_id: str, ctx: Context, model_type: str = "GENERIC"
    ) -> Any:
        try:
            file_b64 = await ctx.get_state("upload_file_b64")
            filename = await ctx.get_state("upload_filename")
            return await asyncio.to_thread(
                _w_import_model, await get_token(ctx),
                {"name": name, "folder_id": folder_id, "model_type": model_type},
                file_b64, filename)
        except Exception as e:
            return _err(e)

    # ── Projects ──────────────────────────────────────────────────────────────
    # A model is only visible in the Model Manager "Projects" screen when it
    # belongs to a project version. sas_model_import alone leaves a model loose in
    # the repository (invisible in Projects); use sas_model_create_project +
    # sas_model_copy_model_to_project (or the one-shot
    # sas_model_create_project_with_model) to make it show up.

    @mcp.tool(
        name="sas_model_list_projects",
        description=(
            "List SAS Model Manager projects (the items shown on the Projects "
            "screen). Returns id, name, function, status, train table and "
            "version per project. Use name_filter to narrow by name."
        ),
    )
    async def sas_model_list_projects(
        ctx: Context, limit: int = 100, start: int = 0, name_filter: str | None = None
    ) -> Any:
        try:
            return await asyncio.to_thread(
                _w_list_projects, await get_token(ctx), limit, start, name_filter)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_model_get_project",
        description=(
            "Get one project's details plus its versions (each version's id is "
            "needed to add a model to that version)."
        ),
    )
    async def sas_model_get_project(project_id: str, ctx: Context) -> Any:
        try:
            return await asyncio.to_thread(_w_get_project, await get_token(ctx), project_id)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_model_projects_summary",
        description=(
            "Aggregate project counts grouped by a field — 'status' (prototype / "
            "deployed) or 'function' (classification / clustering / ...)."
        ),
    )
    async def sas_model_projects_summary(ctx: Context, group_by: str = "status") -> Any:
        try:
            return await asyncio.to_thread(_w_projects_summary, await get_token(ctx), group_by)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_model_create_project",
        description=(
            "Create a SAS Model Manager project — the container that appears on "
            "the Projects screen. Defaults to the default repository. After "
            "creating it, add models with sas_model_copy_model_to_project (or use "
            "sas_model_create_project_with_model to do both at once). The result "
            "includes a `url` field — ALWAYS share that exact url with the user; "
            "never invent or guess a link host."
        ),
    )
    async def sas_model_create_project(
        name: str, ctx: Context, function: str = "prediction", description: str = "",
        train_table: str | None = None, target_variable: str = "",
        repository_id: str | None = None, folder_id: str | None = None,
    ) -> Any:
        try:
            res = await asyncio.to_thread(_w_create_project, await get_token(ctx), {
                "name": name, "function": function, "description": description,
                "train_table": train_table, "target_variable": target_variable,
                "repository_id": repository_id, "folder_id": folder_id})
            # Return a real Model Manager URL so the model never invents a
            # placeholder host (e.g. "your-sas-server-link"). The project is
            # the newest in the Projects list (project routing is a client-side
            # hash route, so we link to the app, not a deep path).
            if isinstance(res, dict) and res.get("id"):
                res["url"] = f"{VIYA_ENDPOINT}/SASModelManager/"
            return res
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_model_list_project_models",
        description=(
            "List the models inside a project (its latest version unless "
            "project_version_id is given). Use this to VERIFY a model landed in "
            "the project and is visible under Projects."
        ),
    )
    async def sas_model_list_project_models(
        project_id: str, ctx: Context, project_version_id: str | None = None
    ) -> Any:
        try:
            return await asyncio.to_thread(
                _w_list_project_models, await get_token(ctx), project_id, project_version_id)
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_model_copy_model_to_project",
        description=(
            "Copy an existing repository model INTO a project so it shows up "
            "under Projects. source_model_id comes from sas_model_list; "
            "project_id from sas_model_list_projects. Defaults to the project's "
            "latest version. opCode 'copy' keeps the original; 'move' relocates it."
        ),
    )
    async def sas_model_copy_model_to_project(
        source_model_id: str, project_id: str, ctx: Context,
        project_version_id: str | None = None, op_code: str = "copy",
    ) -> Any:
        try:
            return await asyncio.to_thread(_w_copy_model_to_project, await get_token(ctx), {
                "source_model_id": source_model_id, "project_id": project_id,
                "project_version_id": project_version_id, "op_code": op_code})
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_model_create_project_with_model",
        description=(
            "One-shot: create a new project AND copy a model into it, so the "
            "model is immediately visible under SAS Model Manager → Projects. "
            "This is the fix for 'the model was created/stored but not visible in "
            "Projects'. Provide a project name and the source model's id."
        ),
    )
    async def sas_model_create_project_with_model(
        name: str, source_model_id: str, ctx: Context, function: str = "prediction",
        train_table: str | None = None, target_variable: str = "", op_code: str = "copy",
    ) -> Any:
        try:
            return await asyncio.to_thread(_w_create_project_with_model, await get_token(ctx), {
                "name": name, "source_model_id": source_model_id, "function": function,
                "train_table": train_table, "target_variable": target_variable,
                "op_code": op_code})
        except Exception as e:
            return _err(e)
