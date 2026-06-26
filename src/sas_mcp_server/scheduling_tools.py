# Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""SAS Viya **scheduling** tools for the MCP server.

These tools wrap the SAS Viya **Scheduler** REST API (``/scheduler/jobs``) — the
same service that SAS Studio's "Schedule as a job" action and Environment
Manager → *Jobs and Flows → Scheduling* drive (captured in ``scheduling.har``).
They let the assistant list scheduled jobs, inspect one job and its upcoming
fire times, change a job's trigger (e.g. move it from 02:00 to 03:00), pause or
resume it, create a new schedule for an existing job request, and delete
(unschedule) a job.

A scheduler *job* is a thin wrapper around something runnable plus one or more
*triggers*. Its shape (confirmed against a live Viya environment) is::

    {
      "id", "name", "description", "runAs", "jobTimeOut",
      "request": {"uri": "/jobExecution/jobRequests/<id>/jobs",
                  "method": "POST", "headers": {...}, "body": null},
      "triggers": [{
        "type": "TIMEEVENT", "active": true,
        "recurrence": {"type": "daily", "startDate": "2026-06-25",
                       "skipCount": 1, "daysOfWeek": [], "dayOfMonth": 1,
                       "dates": []},
        "hours": "3", "minutes": "0", "timezone": "Asia/Calcutta",
        "duration": 1, "maxOccurrence": -1
      }]
    }

The service uses the ``application/vnd.sas.schedule.job+json`` media type, and an
update is an optimistic-concurrency ``PUT`` that must echo the resource's current
``ETag`` in an ``If-Match`` header — so :func:`update_schedule`, :func:`pause` and
:func:`resume` re-fetch the job to obtain that tag before writing.

Auth model is identical to the other tool modules: every tool resolves the
caller's Viya access token via ``get_token(ctx)`` and calls ``/scheduler`` **as
that user**. Listing/reading works for any authenticated user; writes require the
caller to own the job or hold the relevant scheduling authorization, and the
service returns HTTP 403 otherwise, which these tools surface as a clear message.

**Flow dependencies.** "Run job B after job A finishes" is *not* a scheduler-job
trigger (a Job Request only supports time-event triggers) — it is a property of a
SAS **job flow** (``/jobFlowScheduling``). So that lives in its own set of tools
here (:func:`create_flow_dependency`, list/get/delete flows). A flow holds
``jobs`` (each a ``/jobFlowScheduling/jobs`` wrapper around a job request) and
``dependencies`` like
``{"target": "B", "event": {"type": "jobevent", "expression": "completed('A')"}}``
(media type ``application/vnd.sas.schedule.flow+json``), captured against a live
Viya.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import date
from typing import Any

import httpx
from fastmcp import Context, FastMCP

from .config import VIYA_ENDPOINT
from .viya_client import get_paged_items, logger, make_client

# The Scheduler service speaks this media type for a single job; a plain
# ``application/json`` Accept still works for reads but the vendor type is what
# the UI sends and what ``PUT`` requires on the way back in.
_JOB_MEDIA = "application/vnd.sas.schedule.job+json"
# Job-flow scheduling (dependencies between jobs) uses its own service + types.
_FLOW_MEDIA = "application/vnd.sas.schedule.flow+json"
_FLOW_JOB_MEDIA = "application/vnd.sas.schedule.job+json"

# Recurrence skeleton shared by every trigger type; per-type fields are filled in
# by ``_build_recurrence``.
_RECURRENCE_TYPES = ("once", "hourly", "daily", "weekly", "monthly", "yearly")


def _trim(job: dict[str, Any]) -> dict[str, Any]:
    """Drop the noisy HATEOAS ``links`` array from a scheduler job (and triggers)."""
    out = {k: v for k, v in job.items() if k != "links"}
    triggers = out.get("triggers")
    if isinstance(triggers, list):
        out["triggers"] = [
            {k: v for k, v in t.items() if k != "links"} if isinstance(t, dict) else t
            for t in triggers
        ]
    return out


def _forbidden(action: str, status: int) -> dict[str, Any]:
    """Standard message for a write blocked by Viya authorization."""
    return {
        "status": "forbidden",
        "httpStatusCode": status,
        "message": (
            f"Could not {action}: the SAS Scheduler rejected the request "
            f"(HTTP {status}). Managing a scheduled job requires that you own it "
            "or hold scheduling administrator privileges."
        ),
    }


def _not_found(job_id: str) -> dict[str, Any]:
    return {
        "status": "not_found",
        "jobId": job_id,
        "message": f"No scheduled job with id '{job_id}'.",
    }


def _build_recurrence(
    recurrence_type: str,
    start_date: str,
    interval: int,
    days_of_week: list[int] | None,
    day_of_month: int,
) -> dict[str, Any]:
    """Construct the trigger ``recurrence`` object for a given frequency."""
    rt = recurrence_type.lower()
    if rt not in _RECURRENCE_TYPES:
        raise ValueError(
            f"recurrence_type must be one of {_RECURRENCE_TYPES}, got '{recurrence_type}'."
        )
    return {
        "type": rt,
        "startDate": start_date,
        "skipCount": max(int(interval), 1),
        "daysOfWeek": list(days_of_week or []) if rt == "weekly" else [],
        "dayOfMonth": int(day_of_month) if rt in ("monthly", "yearly") else 1,
        "dates": [],
    }


def register_scheduling_tools(
    mcp: FastMCP, get_token: Callable[[Context], Awaitable[str]]
) -> None:
    """Register every SAS Viya scheduling tool on *mcp*.

    *get_token* resolves the caller's Viya access token — the bearer-header swap
    in HTTP mode, or the cached CLI token in stdio mode — exactly as the other
    tool modules use it.
    """

    @asynccontextmanager
    async def session(name: str, ctx: Context) -> AsyncIterator[httpx.AsyncClient]:
        logger.info("--- TOOL USED: %s ---", name)
        token = await get_token(ctx)
        async with make_client(token) as client:
            yield client

    async def _fetch_job(
        client: httpx.AsyncClient, job_id: str
    ) -> tuple[dict[str, Any], str | None]:
        """GET one scheduler job, returning its body and current ETag (for If-Match)."""
        resp = await client.get(
            f"{VIYA_ENDPOINT}/scheduler/jobs/{job_id}",
            headers={"Accept": _JOB_MEDIA},
        )
        resp.raise_for_status()
        return resp.json(), resp.headers.get("ETag")

    # ── Read ──────────────────────────────────────────────────────────────────

    @mcp.tool(
        name="sas_list_scheduled_jobs",
        description=(
            "List the scheduled jobs registered with the SAS Viya scheduler (the "
            "'Scheduling' view in Environment Manager → Jobs and Flows). Each entry "
            "has id, name, the trigger(s) with their recurrence and time of day, who "
            "scheduled it, and timestamps. Pass `name_contains` to search by name "
            "(substring, case-insensitive), or `created_by` to filter by owner. Use "
            "`limit`/`start` to page. Answers 'what jobs are scheduled', 'is <job> "
            "scheduled', 'what runs at night'."
        ),
    )
    async def list_scheduled_jobs(
        ctx: Context,
        name_contains: str | None = None,
        created_by: str | None = None,
        limit: int = 50,
        start: int = 0,
    ) -> dict[str, Any]:
        clauses = []
        if name_contains:
            clauses.append(f"contains(name,'{name_contains}')")
        if created_by:
            clauses.append(f"eq(createdBy,'{created_by}')")
        if len(clauses) == 1:
            filters: str | None = clauses[0]
        elif clauses:
            filters = "and(" + ",".join(clauses) + ")"
        else:
            filters = None
        async with session("sas_list_scheduled_jobs", ctx) as client:
            items, count = await get_paged_items(
                "/scheduler/jobs", client, limit=limit, start=start, filters=filters
            )
            return {
                "count": count,
                "start": start,
                "limit": limit,
                "jobs": [_trim(j) for j in items],
            }

    @mcp.tool(
        name="sas_get_scheduled_job",
        description=(
            "Get the full details of one scheduled job by its id: name, the runnable "
            "request it fires, and every trigger (recurrence type, time of day, "
            "timezone, whether it is active). Returns a not_found status if no such "
            "job exists."
        ),
    )
    async def get_scheduled_job(job_id: str, ctx: Context) -> dict[str, Any]:
        async with session("sas_get_scheduled_job", ctx) as client:
            try:
                job, _ = await _fetch_job(client, job_id)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return _not_found(job_id)
                raise
            return _trim(job)

    @mcp.tool(
        name="sas_get_schedule_fire_times",
        description=(
            "Return the next upcoming run ('fire') times for a scheduled job — useful "
            "to answer 'when does <job> run next' or to confirm a schedule change took "
            "effect. Returns a not_found status if no such job exists."
        ),
    )
    async def get_schedule_fire_times(
        job_id: str, ctx: Context, count: int = 5
    ) -> dict[str, Any]:
        async with session("sas_get_schedule_fire_times", ctx) as client:
            try:
                resp = await client.get(
                    f"{VIYA_ENDPOINT}/scheduler/jobs/{job_id}/fireTimes",
                    headers={"Accept": "application/vnd.sas.collection+json"},
                    params={"limit": count},
                )
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return _not_found(job_id)
                raise
            data = resp.json()
            return {
                "jobId": job_id,
                "fireTimes": [
                    i.get("fireDateTime", i) for i in data.get("items", [])
                ],
            }

    # ── Create ────────────────────────────────────────────────────────────────

    @mcp.tool(
        name="sas_create_scheduled_job",
        description=(
            "Schedule an existing runnable to recur on a time trigger. `request_uri` "
            "must point at something the scheduler can fire — typically a Job "
            "Execution job request, e.g. '/jobExecution/jobRequests/<id>/jobs'. Set "
            "the cadence with `recurrence_type` (once|hourly|daily|weekly|monthly|"
            "yearly), `hours`/`minutes` for the time of day, and optionally `interval` "
            "(every N periods), `days_of_week` (weekly), `day_of_month` (monthly), "
            "`timezone`, and `start_date` (YYYY-MM-DD, defaults to today). Returns the "
            "created job with its new id. NOTE: it does not create the job request "
            "itself — point it at one that already exists."
        ),
    )
    async def create_scheduled_job(
        ctx: Context,
        name: str,
        request_uri: str,
        recurrence_type: str = "daily",
        hours: int = 0,
        minutes: int = 0,
        interval: int = 1,
        days_of_week: list[int] | None = None,
        day_of_month: int = 1,
        timezone: str = "UTC",
        start_date: str | None = None,
        description: str = "",
        run_as: str | None = None,
        job_timeout: int = 30,
    ) -> dict[str, Any]:
        try:
            recurrence = _build_recurrence(
                recurrence_type,
                start_date or date.today().isoformat(),
                interval,
                days_of_week,
                day_of_month,
            )
        except ValueError as e:
            return {"status": "invalid_request", "message": str(e)}

        body: dict[str, Any] = {
            "name": name,
            "description": description,
            "request": {
                "uri": request_uri,
                "method": "POST",
                "headers": {"Content-Type": ["application/json"]},
                "body": None,
            },
            "triggers": [
                {
                    "name": f"Trigger for {name}",
                    "type": "TIMEEVENT",
                    "active": True,
                    "recurrence": recurrence,
                    "hours": str(hours),
                    "minutes": str(minutes),
                    "duration": 1,
                    "timezone": timezone,
                    "maxOccurrence": -1,
                }
            ],
            "jobTimeOut": job_timeout,
        }
        if run_as:
            body["runAs"] = run_as
        async with session("sas_create_scheduled_job", ctx) as client:
            try:
                resp = await client.post(
                    f"{VIYA_ENDPOINT}/scheduler/jobs",
                    headers={"Content-Type": _JOB_MEDIA, "Accept": _JOB_MEDIA},
                    json=body,
                )
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 403:
                    return _forbidden("create the scheduled job", 403)
                return {
                    "status": "error",
                    "httpStatusCode": e.response.status_code,
                    "message": e.response.text[:400],
                }
            return _trim(resp.json())

    # ── Update / pause / resume ────────────────────────────────────────────────

    async def _put_job(
        client: httpx.AsyncClient, job_id: str, action: str, mutate: Any
    ) -> dict[str, Any]:
        """Re-fetch a job for its ETag, apply *mutate(job)*, then PUT with If-Match."""
        try:
            job, etag = await _fetch_job(client, job_id)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return _not_found(job_id)
            raise
        mutate(job)
        headers = {"Content-Type": _JOB_MEDIA, "Accept": _JOB_MEDIA}
        if etag:
            headers["If-Match"] = etag
        try:
            resp = await client.put(
                f"{VIYA_ENDPOINT}/scheduler/jobs/{job_id}", headers=headers, json=job
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 403:
                return _forbidden(action, 403)
            return {
                "status": "error",
                "httpStatusCode": e.response.status_code,
                "message": e.response.text[:400],
            }
        return _trim(resp.json())

    @mcp.tool(
        name="sas_update_schedule",
        description=(
            "Change the trigger of an existing scheduled job — its time of day "
            "(`hours`/`minutes`), cadence (`recurrence_type`), or whether it is active "
            "(`active`=false to disable without deleting). Only the fields you pass are "
            "changed; the rest are preserved. Edits the job's first trigger. Returns "
            "the updated job, or a not_found status if no such job exists."
        ),
    )
    async def update_schedule(
        ctx: Context,
        job_id: str,
        hours: int | None = None,
        minutes: int | None = None,
        recurrence_type: str | None = None,
        active: bool | None = None,
    ) -> dict[str, Any]:
        if recurrence_type is not None and recurrence_type.lower() not in _RECURRENCE_TYPES:
            return {
                "status": "invalid_request",
                "message": f"recurrence_type must be one of {_RECURRENCE_TYPES}.",
            }

        def mutate(job: dict[str, Any]) -> None:
            triggers = job.get("triggers") or []
            if not triggers:
                return
            trig = triggers[0]
            if hours is not None:
                trig["hours"] = str(hours)
            if minutes is not None:
                trig["minutes"] = str(minutes)
            if recurrence_type is not None:
                trig.setdefault("recurrence", {})["type"] = recurrence_type.lower()
            if active is not None:
                trig["active"] = active

        async with session("sas_update_schedule", ctx) as client:
            result = await _put_job(client, job_id, "update the schedule", mutate)
            if isinstance(result, dict) and result.get("triggers") == []:
                return {
                    "status": "no_triggers",
                    "jobId": job_id,
                    "message": "This scheduled job has no triggers to update.",
                }
            return result

    @mcp.tool(
        name="sas_pause_scheduled_job",
        description=(
            "Pause (disable) a scheduled job so it stops firing, without deleting it. "
            "The schedule is retained and can be resumed later with "
            "sas_resume_scheduled_job. Returns a not_found status if no such job exists."
        ),
    )
    async def pause_scheduled_job(job_id: str, ctx: Context) -> dict[str, Any]:
        async with session("sas_pause_scheduled_job", ctx) as client:
            try:
                job, etag = await _fetch_job(client, job_id)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return _not_found(job_id)
                raise
            headers = {"Accept": _JOB_MEDIA}
            if etag:
                headers["If-Match"] = etag
            try:
                resp = await client.put(
                    f"{VIYA_ENDPOINT}/scheduler/jobs/{job_id}/state",
                    headers=headers,
                    params={"value": "paused"},
                )
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 403:
                    return _forbidden("pause the scheduled job", 403)
                raise
            return {"status": "paused", "jobId": job_id, "name": job.get("name")}

    @mcp.tool(
        name="sas_resume_scheduled_job",
        description=(
            "Resume a previously paused scheduled job so it fires on its trigger again. "
            "Returns a not_found status if no such job exists."
        ),
    )
    async def resume_scheduled_job(job_id: str, ctx: Context) -> dict[str, Any]:
        async with session("sas_resume_scheduled_job", ctx) as client:
            try:
                job, etag = await _fetch_job(client, job_id)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return _not_found(job_id)
                raise
            headers = {"Accept": _JOB_MEDIA}
            if etag:
                headers["If-Match"] = etag
            try:
                resp = await client.put(
                    f"{VIYA_ENDPOINT}/scheduler/jobs/{job_id}/state",
                    headers=headers,
                    params={"value": "resumed"},
                )
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 403:
                    return _forbidden("resume the scheduled job", 403)
                raise
            return {"status": "resumed", "jobId": job_id, "name": job.get("name")}

    # ── Delete ────────────────────────────────────────────────────────────────

    @mcp.tool(
        name="sas_delete_schedule",
        description=(
            "Delete (unschedule) a scheduled job by its id. This removes the job and "
            "all of its triggers so it will no longer run; the underlying program/job "
            "request is not deleted. To keep the schedule but stop it from running, "
            "use sas_pause_scheduled_job instead. Returns a not_found status if no such "
            "job exists."
        ),
    )
    async def delete_schedule(job_id: str, ctx: Context) -> dict[str, Any]:
        async with session("sas_delete_schedule", ctx) as client:
            try:
                resp = await client.delete(f"{VIYA_ENDPOINT}/scheduler/jobs/{job_id}")
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return _not_found(job_id)
                if e.response.status_code == 403:
                    return _forbidden("delete the scheduled job", 403)
                raise
            return {
                "status": "deleted",
                "jobId": job_id,
                "message": f"Scheduled job '{job_id}' was unscheduled.",
            }

    # ── Job flows & dependencies ───────────────────────────────────────────────

    async def _default_scheduler_id(client: httpx.AsyncClient) -> str | None:
        try:
            data = await client.get(
                f"{VIYA_ENDPOINT}/jobFlowScheduling/schedulers",
                headers={"Accept": "application/vnd.sas.collection+json"},
                params={"limit": 5},
            )
            data.raise_for_status()
            items = data.json().get("items", [])
            return items[0]["id"] if items else None
        except (httpx.HTTPError, KeyError, ValueError):
            return None

    @mcp.tool(
        name="sas_create_flow_dependency",
        description=(
            "Create a SAS job flow that runs jobs with dependencies — e.g. 'run job B "
            "only after job A completes'. Pass `name` for the flow, `jobs` as a list "
            "of {name, request_uri} (each request_uri is an existing Job Execution job "
            "request, e.g. '/jobExecution/jobRequests/<id>'), and `dependencies` as a "
            "list of {job, runs_after, when} where `job` waits for `runs_after`; "
            "`when` defaults to 'completed' (other options: 'succeeded', 'failed'). "
            "Returns the new flow id and its dependency edges. This wraps each request "
            "as a flow job and posts the flow; it does not create the job requests "
            "themselves."
        ),
    )
    async def create_flow_dependency(
        ctx: Context,
        name: str,
        jobs: list[dict[str, str]],
        dependencies: list[dict[str, str]],
        scheduler_id: str | None = None,
    ) -> dict[str, Any]:
        if not jobs or len(jobs) < 2:
            return {
                "status": "invalid_request",
                "message": "Provide at least two jobs (each {name, request_uri}).",
            }
        async with session("sas_create_flow_dependency", ctx) as client:
            sched = scheduler_id or await _default_scheduler_id(client)
            if not sched:
                return {
                    "status": "invalid_request",
                    "message": "Could not resolve a job-flow scheduler; pass scheduler_id.",
                }
            # 1) Wrap each job request as a job-flow job.
            job_uris: list[str] = []
            for spec in jobs:
                jname, req = spec.get("name"), spec.get("request_uri")
                if not jname or not req:
                    return {
                        "status": "invalid_request",
                        "message": "Each job needs both 'name' and 'request_uri'.",
                    }
                try:
                    r = await client.post(
                        f"{VIYA_ENDPOINT}/jobFlowScheduling/jobs",
                        headers={"Content-Type": _FLOW_JOB_MEDIA, "Accept": _FLOW_JOB_MEDIA},
                        json={"name": jname, "jobRequestUri": req, "priority": "0"},
                    )
                    r.raise_for_status()
                except httpx.HTTPStatusError as e:
                    return {
                        "status": "error",
                        "stage": "create flow job",
                        "httpStatusCode": e.response.status_code,
                        "message": e.response.text[:300],
                    }
                job_uris.append(f"/jobFlowScheduling/jobs/{r.json()['id']}")
            # 2) Build the dependency edges (target runs after the named job).
            deps = []
            for d in dependencies:
                tgt, after = d.get("job"), d.get("runs_after")
                when = d.get("when", "completed")
                if not tgt or not after:
                    return {
                        "status": "invalid_request",
                        "message": "Each dependency needs 'job' and 'runs_after'.",
                    }
                deps.append({
                    "target": tgt,
                    "event": {"type": "jobevent", "expression": f"{when}('{after}')"},
                })
            # 3) Create the flow.
            try:
                resp = await client.post(
                    f"{VIYA_ENDPOINT}/jobFlowScheduling/flows",
                    headers={"Content-Type": _FLOW_MEDIA, "Accept": _FLOW_MEDIA},
                    json={
                        "name": name,
                        "schedulerId": sched,
                        "triggerType": "event",
                        "triggerCondition": "any",
                        "jobs": job_uris,
                        "dependencies": deps,
                    },
                )
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                return {
                    "status": "error",
                    "stage": "create flow",
                    "httpStatusCode": e.response.status_code,
                    "message": e.response.text[:300],
                }
            flow = resp.json()
            return {
                "status": "created",
                "flowId": flow.get("id"),
                "name": flow.get("name"),
                "dependencies": flow.get("dependencies"),
            }

    @mcp.tool(
        name="sas_list_job_flows",
        description=(
            "List SAS job flows (multi-job flows with dependencies). Pass "
            "`name_contains` to search by name. Each entry has the flow id, name, and "
            "trigger type."
        ),
    )
    async def list_job_flows(
        ctx: Context,
        name_contains: str | None = None,
        limit: int = 50,
        start: int = 0,
    ) -> dict[str, Any]:
        filters = f"contains(name,'{name_contains}')" if name_contains else None
        async with session("sas_list_job_flows", ctx) as client:
            items, count = await get_paged_items(
                "/jobFlowScheduling/flows", client, limit=limit, start=start, filters=filters
            )
            return {
                "count": count,
                "start": start,
                "limit": limit,
                "flows": [
                    {k: f.get(k) for k in ("id", "name", "triggerType", "createdBy")}
                    for f in items
                ],
            }

    @mcp.tool(
        name="sas_get_job_flow",
        description=(
            "Get one SAS job flow by id: its jobs and the dependency edges between "
            "them (which job runs after which). Returns not_found if no such flow."
        ),
    )
    async def get_job_flow(flow_id: str, ctx: Context) -> dict[str, Any]:
        async with session("sas_get_job_flow", ctx) as client:
            try:
                flow = await client.get(
                    f"{VIYA_ENDPOINT}/jobFlowScheduling/flows/{flow_id}",
                    headers={"Accept": _FLOW_MEDIA},
                )
                flow.raise_for_status()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return {"status": "not_found", "flowId": flow_id}
                raise
            f = flow.json()
            return {
                "id": f.get("id"),
                "name": f.get("name"),
                "jobs": f.get("jobs"),
                "dependencies": f.get("dependencies"),
                "triggerType": f.get("triggerType"),
            }

    @mcp.tool(
        name="sas_delete_job_flow",
        description="Delete a SAS job flow by id. Returns not_found if no such flow.",
    )
    async def delete_job_flow(flow_id: str, ctx: Context) -> dict[str, Any]:
        async with session("sas_delete_job_flow", ctx) as client:
            try:
                resp = await client.delete(
                    f"{VIYA_ENDPOINT}/jobFlowScheduling/flows/{flow_id}"
                )
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return {"status": "not_found", "flowId": flow_id}
                raise
            return {"status": "deleted", "flowId": flow_id}
