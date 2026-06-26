# Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""SAS Viya **model-monitoring** tools for the MCP server.

These tools wrap the SAS **Model Management** REST API (``/modelManagement``) —
the service behind SAS Model Manager's *Performance* and *Compare* views. They
let the assistant define and run a model performance-monitoring task (which also
produces characteristic / data-drift output), list and read those tasks, and
compare models. Captured in ``monitoring.har`` against a live Viya, end-to-end:
the task scored a champion model across three time-period tables and produced
KS / lift / ROC / fit-statistic and PSI / variable-deviation (drift) results.

Performance monitoring expects the input data as a set of **period tables** in a
CAS library, named ``<prefix>_1``, ``<prefix>_2``, … (the sequence number is the
time period; an optional ``_<timeLabel>`` suffix names it). Each table holds the
model's input variables plus the actual target. The task scores the model on
each period and tracks metrics — and variable drift (PSI) — over time.

Create shape (media type ``application/vnd.sas.models.performance.task+json``)::

    POST /modelManagement/performanceTasks
    {
      "name", "projectId", "function": "classification",
      "dataLibrary": "Public", "dataPrefix": "<prefix>", "casServerId": "cas-shared-default",
      "resultLibrary": "ModelPerformanceData",
      "relatedProperties": ["<modelId>_-_<name>_-_champion", "tarVar_-_<target>",
                            "tarEveVar_-_<event>", "tarLev_-_binary",
                            "eveProVar_-_P_<target><event>"],
      "inputVariables": [...], "outputVariables": ["EM_PROBABILITY", ...],
      "championMonitored": true, "scoreExecutionRequired": true,
      "performanceResultSaved": true, "maxBins": 10, "liftBins": 20
    }

Running it is a bodyless ``POST /modelManagement/performanceTasks/<id>``.

Comparing models (media ``application/vnd.sas.models.report.comparison.request+json``)::

    POST /modelManagement/reports  {"name", "modelUris": ["/modelRepository/models/<id>", ...]}

Auth model matches the other tool modules: each tool resolves the caller's Viya
token via ``get_token(ctx)`` and calls the API as that user.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastmcp import Context, FastMCP

from .config import VIYA_ENDPOINT
from .viya_client import get_json, get_paged_items, logger, make_client

_PERF_TASK_MEDIA = "application/vnd.sas.models.performance.task+json"
_REPORT_MEDIA = "application/vnd.sas.models.report.comparison.request+json"
_CAS_SERVER = "cas-shared-default"

# The standard CAS result tables a performance run writes into the result
# library (prefixed by the project id). Surfaced so callers know where the
# metric/drift detail lives (read them with the CAS data tools if needed).
_RESULT_TABLES = {
    "fitStatistics": "MM_FITSTAT",
    "ks": "MM_KS",
    "lift": "MM_LIFT",
    "roc": "MM_ROC",
    "stabilityKpi": "MM_STD_KPI",
    "variableDrift": "MM_VAR_DEVIATION",
    "variableSummary": "MM_VAR_SUMMARY",
    "jobHistory": "MM_JOB_HISTORY",
}

_TASK_FIELDS = (
    "id", "name", "projectId", "function", "dataLibrary", "dataPrefix",
    "resultLibrary", "championMonitored", "challengerMonitored",
    "scoreExecutionRequired", "createdBy", "creationTimeStamp", "modifiedTimeStamp",
)


def _trim_task(t: dict[str, Any]) -> dict[str, Any]:
    return {k: t.get(k) for k in _TASK_FIELDS if k in t}


def register_monitoring_tools(
    mcp: FastMCP, get_token: Callable[[Context], Awaitable[str]]
) -> None:
    """Register the SAS model-monitoring tools on *mcp*."""

    @asynccontextmanager
    async def session(name: str, ctx: Context) -> AsyncIterator[httpx.AsyncClient]:
        logger.info("--- TOOL USED: %s ---", name)
        token = await get_token(ctx)
        async with make_client(token) as client:
            yield client

    async def _project(client: httpx.AsyncClient, pid: str) -> dict[str, Any]:
        return await get_json(f"/modelRepository/projects/{pid}", client)

    async def _champion(client: httpx.AsyncClient, pid: str) -> dict[str, Any] | None:
        """Return the champion model object for a project (or the first model)."""
        items, _ = await get_paged_items(
            f"/modelRepository/projects/{pid}/models", client, limit=50
        )
        for m in items:
            if str(m.get("role", "")).lower() == "champion":
                return m
        return items[0] if items else None

    async def _input_vars(client: httpx.AsyncClient, pid: str) -> list[str]:
        items, _ = await get_paged_items(
            f"/modelRepository/projects/{pid}/variables", client, limit=500
        )
        return [v["name"] for v in items if str(v.get("role", "")).lower() == "input"]

    # ── Run monitoring ──────────────────────────────────────────────────────────

    @mcp.tool(
        name="sas_run_model_monitoring",
        description=(
            "Define and run a model performance-monitoring task for a Model Manager "
            "project, then return the task. It scores the project's champion model on "
            "the period tables `<data_prefix>_1`, `<data_prefix>_2`, … in CAS "
            "`data_library` (each table = one time period, holding the model inputs + "
            "actual target) and tracks fit statistics, lift/KS/ROC, and variable "
            "drift (PSI) over time. Most settings are derived from the project "
            "(function, target, event level, champion, input variables); you mainly "
            "supply `project_id`, `data_library`, and `data_prefix`. Set `run=false` "
            "to only create the definition without executing it. The drift output "
            "(characteristic / stability / PSI) is part of this same task — that is "
            "the 'detect data drift' capability."
        ),
    )
    async def run_model_monitoring(
        ctx: Context,
        project_id: str,
        data_library: str,
        data_prefix: str,
        name: str | None = None,
        run: bool = True,
        result_library: str = "ModelPerformanceData",
        cas_server: str = _CAS_SERVER,
        max_bins: int = 10,
        lift_bins: int = 20,
    ) -> dict[str, Any]:
        async with session("sas_run_model_monitoring", ctx) as client:
            try:
                proj = await _project(client, project_id)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return {"status": "not_found", "projectId": project_id}
                raise
            champ = await _champion(client, project_id)
            if not champ:
                return {
                    "status": "invalid_request",
                    "message": f"Project '{project_id}' has no registered model to monitor.",
                }
            target = proj.get("targetVariable") or ""
            event = str(proj.get("targetEventValue") or "1")
            function = proj.get("function") or "classification"
            not_event = "0" if event != "0" else "1"
            related = [
                f"{champ.get('id')}_-_{champ.get('name')}_-_champion",
                f"tarVar_-_{target}",
                f"tarEveVar_-_{event}",
                "tarLev_-_binary",
                f"eveProVar_-_P_{target}{event}",
            ]
            output_vars = [
                "EM_PROBABILITY",
                "EM_EVENTPROBABILITY",
                f"P_{target}{not_event}",
                f"P_{target}{event}",
            ]
            body: dict[str, Any] = {
                "name": name or f"{proj.get('name', project_id)}_Performance",
                "description": "",
                "projectId": project_id,
                "modelIds": [],
                "relatedProperties": related,
                "outputVariables": output_vars,
                "inputVariables": await _input_vars(client, project_id),
                "scoreExecutionRequired": True,
                "performanceResultSaved": True,
                "maxBins": max_bins,
                "resultLibrary": result_library,
                "casServerId": cas_server,
                "dataLibrary": data_library,
                "dataPrefix": data_prefix,
                "dataTable": "",
                "championMonitored": True,
                "challengerMonitored": False,
                "function": function,
                "binMethod": "ADAPTIVE",
                "traceOn": False,
                "biasVariables": [],
                "liftBins": lift_bins,
                "rocCutOffNumber": 100,
                "percentileMaxIter": 200,
            }
            try:
                resp = await client.post(
                    f"{VIYA_ENDPOINT}/modelManagement/performanceTasks",
                    headers={"Content-Type": _PERF_TASK_MEDIA, "Accept": "application/json"},
                    json=body,
                )
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                return {
                    "status": "error",
                    "httpStatusCode": e.response.status_code,
                    "message": e.response.text[:400],
                }
            task = resp.json()
            task_id = task.get("id")
            ran = False
            if run and task_id:
                try:
                    r = await client.post(
                        f"{VIYA_ENDPOINT}/modelManagement/performanceTasks/{task_id}",
                        headers={"Accept": "application/json"},
                    )
                    r.raise_for_status()
                    ran = True
                except httpx.HTTPStatusError:
                    ran = False
            return {
                "status": "running" if ran else "created",
                "taskId": task_id,
                "ran": ran,
                **_trim_task(task),
            }

    # ── Read performance ───────────────────────────────────────────────────────

    @mcp.tool(
        name="sas_list_performance_tasks",
        description=(
            "List model performance-monitoring tasks. Pass `project_id` to list a "
            "single project's tasks. Each entry has the task id, name, the project "
            "and model it monitors, and the data prefix/library it runs on."
        ),
    )
    async def list_performance_tasks(
        ctx: Context,
        project_id: str | None = None,
        limit: int = 50,
        start: int = 0,
    ) -> dict[str, Any]:
        filters = f"eq(projectId,'{project_id}')" if project_id else None
        async with session("sas_list_performance_tasks", ctx) as client:
            items, count = await get_paged_items(
                "/modelManagement/performanceTasks", client,
                limit=limit, start=start, filters=filters,
            )
            return {
                "count": count,
                "start": start,
                "limit": limit,
                "tasks": [_trim_task(t) for t in items],
            }

    @mcp.tool(
        name="sas_get_model_performance",
        description=(
            "Get a model performance-monitoring task's definition and where its "
            "results live. Returns the task settings plus the names of the CAS result "
            "tables (fit statistics, KS, lift, ROC, stability KPIs, and variable-drift "
            "/ PSI) written to the result library — each prefixed by the project id, "
            "e.g. '<PROJECTID>.MM_FITSTAT'. Read those tables with the CAS data tools "
            "for the metric/drift values over time. Returns not_found if no such task."
        ),
    )
    async def get_model_performance(task_id: str, ctx: Context) -> dict[str, Any]:
        async with session("sas_get_model_performance", ctx) as client:
            try:
                task = await get_json(
                    f"/modelManagement/performanceTasks/{task_id}", client
                )
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return {"status": "not_found", "taskId": task_id}
                raise
            pid = (task.get("projectId") or "").upper()
            result_tables = {
                label: f"{pid}.{tbl}" for label, tbl in _RESULT_TABLES.items()
            }
            return {
                **_trim_task(task),
                "resultTables": result_tables,
                "resultCasServer": task.get("casServerId", _CAS_SERVER),
            }

    # ── Compare ────────────────────────────────────────────────────────────────

    @mcp.tool(
        name="sas_compare_models",
        description=(
            "Compare two or more models and return the comparison report (model "
            "properties, input/output variables, target, fit statistics and plots "
            "side by side). Pass the model ids as `model_ids`; they are typically the "
            "models within one Model Manager project. Optionally name the report."
        ),
    )
    async def compare_models(
        ctx: Context,
        model_ids: list[str],
        name: str = "compareModels",
    ) -> dict[str, Any]:
        if not model_ids or len(model_ids) < 1:
            return {
                "status": "invalid_request",
                "message": "Pass at least one model id in model_ids (two+ to compare).",
            }
        body = {
            "name": name,
            "description": "",
            "modelUris": [f"/modelRepository/models/{mid}" for mid in model_ids],
        }
        async with session("sas_compare_models", ctx) as client:
            try:
                resp = await client.post(
                    f"{VIYA_ENDPOINT}/modelManagement/reports",
                    headers={"Content-Type": _REPORT_MEDIA, "Accept": "application/json"},
                    json=body,
                )
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                return {
                    "status": "error",
                    "httpStatusCode": e.response.status_code,
                    "message": e.response.text[:400],
                }
            report = resp.json()
            return {
                "status": "created",
                "reportId": report.get("id"),
                "name": report.get("name", name),
                "modelCount": len(model_ids),
            }
