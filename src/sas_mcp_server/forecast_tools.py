# Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""SAS Viya **forecasting / analytics-project** tools for the MCP server.

These tools wrap the SAS **Analytics Gateway** REST API (``/analyticsGateway``) —
the service behind SAS Model Studio. They let the assistant create a Forecasting
(or other) analytics project on a CAS table, and list / inspect / delete those
projects. Captured in ``forecasting.har`` against a live Viya.

Create shape (confirmed against a live environment), media type
``application/vnd.sas.analytics.project+json``::

    POST /analyticsGateway/projects?parentFolderUri=<folder>
    {
      "name": "demo_fc",
      "projectType": "forecasting",
      "dataMediaType": "application/vnd.sas.data.table",
      "dataUri": "/dataTables/dataSources/cas~fs~cas-shared-default~fs~<caslib>/tables/<table>",
      "containerUri": "<folder>",
      "links": [{"rel": "initialPipelineTemplate",
                 "href": "/analyticsGateway/pipelineTemplates/forecasting-auto-3"}],
      "providerSpecificProperties": {"computeContextUri": "/compute/contexts/<id>"}
    }

**Important environment note.** Creating a project succeeds, but *opening* a Model
Studio project (to assign roles, build/run a pipeline, view champion/forecast
results) requires the Model Studio analytics *provider* to spin up. On some
deployments that provider fails for every project with ``creatingProviderError``
even though CAS and the compute contexts are healthy — a server-side issue an
admin must resolve. So only project create/list/get/delete are exposed here;
the pipeline/run/results tools require that provider to be working.

Auth model is identical to the other tool modules: each tool resolves the
caller's Viya token via ``get_token(ctx)`` and calls the API as that user.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastmcp import Context, FastMCP

from .config import VIYA_ENDPOINT
from .viya_client import get_paged_items, logger, make_client

_PROJECT_MEDIA = "application/vnd.sas.analytics.project+json"
_CAS_SERVER = "cas-shared-default"
# The auto-forecasting starter template Model Studio uses for a Forecasting
# project; overridable for other project types / templates.
_DEFAULT_FC_TEMPLATE = "/analyticsGateway/pipelineTemplates/forecasting-auto-3"
_VF_CONTEXT_NAME = "SAS Visual Forecasting compute context"

_PROJECT_FIELDS = (
    "id", "name", "projectType", "projectStatus", "description",
    "dataUri", "dataServerName", "dataSourceName",
    "createdBy", "creationTimeStamp", "modifiedTimeStamp",
)


def _trim(p: dict[str, Any]) -> dict[str, Any]:
    return {k: p.get(k) for k in _PROJECT_FIELDS if k in p}


def _data_uri(caslib: str, table: str, server: str = _CAS_SERVER) -> str:
    """Build the ``/dataTables`` URI for an in-memory CAS table."""
    return f"/dataTables/dataSources/cas~fs~{server}~fs~{caslib}/tables/{table}"


def register_forecast_tools(
    mcp: FastMCP, get_token: Callable[[Context], Awaitable[str]]
) -> None:
    """Register the SAS forecasting / analytics-project tools on *mcp*."""

    @asynccontextmanager
    async def session(name: str, ctx: Context) -> AsyncIterator[httpx.AsyncClient]:
        logger.info("--- TOOL USED: %s ---", name)
        token = await get_token(ctx)
        async with make_client(token) as client:
            yield client

    async def _compute_context_uri(client: httpx.AsyncClient, name: str) -> str | None:
        """Look up a compute context id by name → ``/compute/contexts/<id>``."""
        try:
            resp = await client.get(
                f"{VIYA_ENDPOINT}/compute/contexts",
                headers={"Accept": "application/vnd.sas.collection+json"},
                params={"filter": f"eq(name,'{name}')", "limit": 1},
            )
            resp.raise_for_status()
            items = resp.json().get("items", [])
            if items:
                return f"/compute/contexts/{items[0]['id']}"
        except (httpx.HTTPError, KeyError, ValueError):
            pass
        return None

    async def _my_folder_uri(client: httpx.AsyncClient) -> str | None:
        try:
            resp = await client.get(
                f"{VIYA_ENDPOINT}/folders/folders/@myFolder",
                headers={"Accept": "application/vnd.sas.content.folder+json"},
            )
            resp.raise_for_status()
            fid = resp.json().get("id")
            return f"/folders/folders/{fid}" if fid else None
        except (httpx.HTTPError, ValueError):
            return None

    # ── Create ────────────────────────────────────────────────────────────────

    @mcp.tool(
        name="sas_create_forecast_project",
        description=(
            "Create a SAS Model Studio analytics project of a given type on a CAS "
            "table. Defaults to a Forecasting project. Provide `name` and the CAS "
            "`caslib`/`table` holding the (time-series) data; optionally override "
            "`project_type` (forecasting | dataMining | textAnalytics), the pipeline "
            "`template`, the destination `folder_uri`, and the `compute_context` "
            "name. Returns the new project's id and details. NOTE: this only creates "
            "the project; opening it to build/run a pipeline needs the Model Studio "
            "provider service to be healthy on the server."
        ),
    )
    async def create_forecast_project(
        ctx: Context,
        name: str,
        caslib: str,
        table: str,
        project_type: str = "forecasting",
        description: str = "",
        template: str | None = None,
        folder_uri: str | None = None,
        compute_context: str = _VF_CONTEXT_NAME,
        cas_server: str = _CAS_SERVER,
    ) -> dict[str, Any]:
        async with session("sas_create_forecast_project", ctx) as client:
            container = folder_uri or await _my_folder_uri(client)
            if not container:
                return {
                    "status": "invalid_request",
                    "message": (
                        "Could not resolve a destination folder. Pass folder_uri "
                        "(e.g. '/folders/folders/<id>')."
                    ),
                }
            body: dict[str, Any] = {
                "name": name,
                "description": description,
                "projectType": project_type,
                "dataMediaType": "application/vnd.sas.data.table",
                "dataUri": _data_uri(caslib, table, cas_server),
                "containerUri": container,
            }
            tmpl = template or (_DEFAULT_FC_TEMPLATE if project_type == "forecasting" else None)
            if tmpl:
                body["links"] = [
                    {
                        "method": "GET",
                        "rel": "initialPipelineTemplate",
                        "href": tmpl,
                        "uri": tmpl,
                        "type": "application/vnd.sas.analytics.pipeline.template",
                    }
                ]
            ctx_uri = await _compute_context_uri(client, compute_context)
            if ctx_uri:
                body["providerSpecificProperties"] = {"computeContextUri": ctx_uri}
            try:
                resp = await client.post(
                    f"{VIYA_ENDPOINT}/analyticsGateway/projects",
                    headers={"Content-Type": _PROJECT_MEDIA, "Accept": _PROJECT_MEDIA},
                    params={"parentFolderUri": container},
                    json=body,
                )
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                return {
                    "status": "error",
                    "httpStatusCode": e.response.status_code,
                    "message": e.response.text[:400],
                }
            return {"status": "created", **_trim(resp.json())}

    # ── Read / delete ───────────────────────────────────────────────────────────

    @mcp.tool(
        name="sas_list_forecast_projects",
        description=(
            "List SAS Model Studio analytics projects (forecasting and others). Pass "
            "`name_contains` to search by name, or `project_type` (e.g. 'forecasting') "
            "to filter by type. Use `limit`/`start` to page. Each entry has id, name, "
            "type, status, the data table it was built on, and timestamps."
        ),
    )
    async def list_forecast_projects(
        ctx: Context,
        name_contains: str | None = None,
        project_type: str | None = None,
        limit: int = 50,
        start: int = 0,
    ) -> dict[str, Any]:
        clauses = []
        if name_contains:
            clauses.append(f"contains(name,'{name_contains}')")
        if project_type:
            clauses.append(f"eq(projectType,'{project_type}')")
        filters = clauses[0] if len(clauses) == 1 else (
            "and(" + ",".join(clauses) + ")" if clauses else None
        )
        async with session("sas_list_forecast_projects", ctx) as client:
            items, count = await get_paged_items(
                "/analyticsGateway/projects", client, limit=limit, start=start, filters=filters
            )
            return {
                "count": count,
                "start": start,
                "limit": limit,
                "projects": [_trim(p) for p in items],
            }

    @mcp.tool(
        name="sas_get_forecast_project",
        description=(
            "Get the details of one Model Studio analytics project by its id: name, "
            "type, status, the CAS data table it uses, and timestamps. Returns a "
            "not_found status if no such project exists."
        ),
    )
    async def get_forecast_project(project_id: str, ctx: Context) -> dict[str, Any]:
        async with session("sas_get_forecast_project", ctx) as client:
            try:
                resp = await client.get(
                    f"{VIYA_ENDPOINT}/analyticsGateway/projects/{project_id}",
                    headers={"Accept": _PROJECT_MEDIA},
                )
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return {"status": "not_found", "projectId": project_id}
                raise
            return _trim(resp.json())

    @mcp.tool(
        name="sas_delete_forecast_project",
        description=(
            "Delete a Model Studio analytics project by its id. Returns a not_found "
            "status if no such project exists."
        ),
    )
    async def delete_forecast_project(project_id: str, ctx: Context) -> dict[str, Any]:
        async with session("sas_delete_forecast_project", ctx) as client:
            try:
                resp = await client.delete(
                    f"{VIYA_ENDPOINT}/analyticsGateway/projects/{project_id}"
                )
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return {"status": "not_found", "projectId": project_id}
                raise
            return {"status": "deleted", "projectId": project_id}
