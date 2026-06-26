# Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""SAS Intelligent Decisioning tools for the SAS Viya MCP server.

These tools wrap the SAS Viya **Intelligent Decisioning** REST APIs — the
services behind *Build Decisions* / *Manage Business Rules* (captured in
``ID_har.har``, whose dominant activity was building a decision flow with a
rule set and Python/SQL code nodes):

  * **Decision flows**  ``/decisions/flows``                — the decision assets
  * **Rule sets + rules** ``/businessRules/ruleSets`` ``…/rules`` — business rules
  * **Code files**      ``/decisions/codeFiles`` (+ ``/files/files``) — SQL/Python nodes

They cover the create / read / delete lifecycle for each asset type plus rule
authoring (including editing a rule in place), assembling nodes into a runnable
flow (rule-set, code-file, **model**, **sub-decision** and if/then/else branch
nodes), publishing, and **testing/scoring** a decision against a CAS table. They
also manage the reference data decisions rely on — **global variables** and
**lookup tables** (reference-data domains) — and can list the registered custom
REST node types (Segmentation Tree, Decision Rest Node, Language Model Query).

The flow-node endpoints, test/score flow, global-variable and lookup shapes were
reverse-engineered from ``Decision.har`` (the Build Decisions UI building a flow
with a rule-set and a model node, editing a rule, and running the Test feature).

Auth model is identical to the other tool modules: every tool resolves the
caller's Viya access token via ``get_token(ctx)`` and calls the services **as
that user**, honoring their folder permissions.
"""

from __future__ import annotations

import asyncio
import re
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastmcp import Context, FastMCP

from .config import VIYA_ENDPOINT
from .viya_client import get_json, get_paged_items, logger, make_client

# Intelligent Decisioning vendor media types (these endpoints reject plain
# ``application/json`` on the typed resources — they require the exact type).
_DEC = "application/vnd.sas.decision+json"
_RS = "application/vnd.sas.business.rule.set+json"
_RS_INTEGRAL = "application/vnd.sas.business.rule.set.integral+json"
_CF = "application/vnd.sas.decision.code.file+json"
_COLLECTION = "application/vnd.sas.collection+json"
_VALIDATION_REQ = "application/vnd.sas.decision.expression.validation.request+json"
_VALIDATION_RESP = "application/vnd.sas.validation+json"
_PUBLISH_REQ = "application/vnd.sas.models.publishing.request.asynchronous+json"
_DS2 = "text/vnd.sas.source.ds2"
_MODEL = "application/vnd.sas.models.model+json"
_GV = "application/vnd.sas.data.reference.global.variable+json"
_DOMAIN = "application/vnd.sas.data.reference.domain+json"
_DOMAIN_CONTENT = "application/vnd.sas.data.reference.domain.content+json"
_NODE_TYPE = "application/vnd.sas.decision.node.type+json"

_VALID_DATATYPES = ("string", "integer", "decimal", "double", "date", "datetime", "boolean", "dataGrid")
_VALID_DIRECTIONS = ("input", "output", "inOut")
_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def _trim(item: dict[str, Any]) -> dict[str, Any]:
    """Drop the noisy HATEOAS ``links`` array from a returned object."""
    return {k: v for k, v in item.items() if k != "links"}


def _sig_vars(sig: list[dict] | None) -> list[dict[str, Any]]:
    """Compact view of a signature (the decision/rule-set variables)."""
    out = []
    for t in sig or []:
        out.append({k: t[k] for k in ("name", "dataType", "direction", "length") if k in t})
    return out


def _q(value: str) -> str:
    return value.replace("'", "''")


def _build_node_mappings(
    node_sig: list[dict], existing_names: set[str]
) -> tuple[list[dict], list[dict]]:
    """Build a node's flow-step ``mappings`` and any new decision-signature variables.

    Each node variable becomes a mapping (``stepTermName`` = the variable name, which
    SAS wires to a same-named decision variable). Scalar outputs also carry
    ``targetDecisionTermName`` so they write back to the decision; dataGrid outputs
    are step-local. ``existing_names`` is mutated with the names added.
    """
    mappings: list[dict] = []
    new_vars: list[dict] = []
    for v in node_sig:
        name = v.get("name")
        if not name:
            continue
        dt = v.get("dataType", "string")
        direction = v.get("direction", "inOut")
        length = v.get("length")
        is_grid = dt == "dataGrid"
        mid = str(uuid.uuid4())
        m: dict[str, Any] = {"direction": direction, "dataType": dt, "stepTermName": name,
                             "id": mid, "originalId": mid, "description": ""}
        if length:
            m["length"] = length
        if direction == "output" and not is_grid:
            m["targetDecisionTermName"] = name
        mappings.append(m)
        # mirror the variable into the decision signature (skip step-local dataGrids)
        if not is_grid and name not in existing_names:
            dv: dict[str, Any] = {"name": name, "dataType": dt, "direction": direction,
                                  "id": str(uuid.uuid4())}
            if length:
                dv["length"] = length
            new_vars.append(dv)
            existing_names.add(name)
    return mappings, new_vars


def _build_model_mappings(
    model_vars: list[dict], existing_names: set[str]
) -> tuple[list[dict], list[dict]]:
    """Build a model node's flow-step ``mappings`` and any new decision variables.

    Model variables come from ``/modelRepository/models/{id}/variables`` (each has
    ``name``, ``role`` input/output, ``type`` and ``length``). Unlike rule-set/code
    nodes, a model node writes ``targetDecisionTermName`` on **every** mapping — both
    its inputs (read from the decision) and outputs (written back) — so each model
    variable is wired to a same-named decision variable (created if missing). This
    mirrors the ``step.model`` payload SAS Decision Manager sends (Decision.har).
    """
    mappings: list[dict] = []
    new_vars: list[dict] = []
    for v in model_vars or []:
        name = v.get("name")
        if not name:
            continue
        dt = v.get("type") or v.get("dataType") or "string"
        direction = "output" if str(v.get("role", "input")).lower() == "output" else "input"
        length = v.get("length")
        mid = str(uuid.uuid4())
        m: dict[str, Any] = {"direction": direction, "dataType": dt, "stepTermName": name,
                             "id": mid, "originalId": mid, "description": "",
                             "targetDecisionTermName": name}
        if length:
            m["length"] = length
        mappings.append(m)
        if name not in existing_names:
            dv: dict[str, Any] = {"name": name, "dataType": dt, "direction": direction,
                                  "id": str(uuid.uuid4())}
            if length:
                dv["length"] = length
            new_vars.append(dv)
            existing_names.add(name)
    return mappings, new_vars


def _sanitize_ident(name: str) -> str:
    """A DS2/MAS-safe model name: letters/digits/underscore, not starting with a digit."""
    s = re.sub(r"[^A-Za-z0-9_]", "_", name or "") or "model"
    if s[0].isdigit():
        s = "m_" + s
    return s


def _dm_url(kind: str, asset_id: str) -> str:
    """Deep-link that opens an asset in SAS Decision Manager.

    ``kind`` is the Decision Manager route: 'decisions', 'rules', or 'codeFiles'
    (captured from the UI Referer headers in ID_har.har).
    """
    return f"{VIYA_ENDPOINT}/SASDecisionManager/{kind}/{asset_id}"


def _flow_outline(steps: list[dict] | None, indent: int = 0) -> list[str]:
    """A readable, nested text outline of a decision flow's steps (a chat preview).

    Decisioning has no image-render service (unlike VA), so the flow is shown as
    an indented outline — code nodes, rule-set nodes, and if/then/else branches.
    """
    out: list[str] = []
    pad = "  " * indent
    for s in steps or []:
        t = s.get("type", "")
        if "custom.object" in t:
            co = s.get("customObject", {}) or {}
            out.append(f"{pad}• Code node: {co.get('name')} ({co.get('type')})")
        elif "ruleset" in t:
            rs = s.get("ruleset", {}) or {}
            out.append(f"{pad}• Rule set: {rs.get('name')}")
        elif "condition" in t:
            out.append(f"{pad}• If {_cond_label(s)}:")
            on_true = (s.get("onTrue") or {}).get("steps")
            on_false = (s.get("onFalse") or {}).get("steps")
            if on_true:
                out.append(f"{pad}    then:")
                out += _flow_outline(on_true, indent + 3)
            if on_false:
                out.append(f"{pad}    else:")
                out += _flow_outline(on_false, indent + 3)
        else:
            out.append(f"{pad}• {(t.split('.')[-1] or 'step')}")
    return out


def _cond_label(step: dict) -> str:
    """Human label for a condition step, across the shapes SAS uses for it."""
    cond = step.get("condition", {}) or {}
    lhs = (cond.get("lhsTerm") or {}).get("name")
    op = cond.get("operator")
    rhs = cond.get("rhsConstant")
    if rhs is None and cond.get("rhsTerm"):
        rhs = (cond.get("rhsTerm") or {}).get("name")
    if lhs and op is not None:
        return f"{lhs} {op} {rhs}"
    term = (cond.get("term") or {}).get("name")
    expr = cond.get("expression")
    if term and expr:
        return f"{term} {expr}"
    if expr:
        return str(expr)
    return step.get("name") or "condition"


def _mm_label(text: Any) -> str:
    """Sanitise a label for a Mermaid node (drop chars that break the parser)."""
    s = re.sub(r'["\[\]{}|<>()]', "", str(text)).strip()
    return (s[:38] + "…") if len(s) > 38 else (s or "step")


def _flow_mermaid(steps: list[dict] | None) -> str | None:
    """Best-effort Mermaid flowchart of a decision flow's steps (``None`` if empty).

    Code nodes render as rectangles, rule-set nodes as stadiums, and condition
    steps as diamonds with then/else branches — so a chat UI that supports Mermaid
    can draw the decision flow as a diagram.
    """
    if not steps:
        return None
    lines = ["flowchart TD", "  start([Start])"]
    ctr = {"n": 0}

    def nid() -> str:
        ctr["n"] += 1
        return f"n{ctr['n']}"

    def walk(steps: list[dict] | None, prev: str, first_label: str | None = None) -> str:
        label = first_label
        for s in steps or []:
            t = s.get("type", "")
            i = nid()
            edge = f"  {prev} -->|{label}| {i}" if label else f"  {prev} --> {i}"
            label = None
            if "custom.object" in t:
                lines.append(f'  {i}["{_mm_label((s.get("customObject") or {}).get("name", "code"))}"]')
                lines.append(edge)
                prev = i
            elif "ruleset" in t:
                lines.append(f'  {i}(["{_mm_label((s.get("ruleset") or {}).get("name", "ruleset"))}"])')
                lines.append(edge)
                prev = i
            elif "condition" in t:
                lines.append(f'  {i}{{"{_mm_label(_cond_label(s))}"}}')
                lines.append(edge)
                walk((s.get("onTrue") or {}).get("steps"), i, "then")
                walk((s.get("onFalse") or {}).get("steps"), i, "else")
                prev = i
            else:
                lines.append(f'  {i}["{_mm_label(t.split(".")[-1] or "step")}"]')
                lines.append(edge)
                prev = i
        return prev

    last = walk(steps, "start")
    lines.append(f"  {last} --> done([End])")
    return "\n".join(lines)


def register_decision_tools(
    mcp: FastMCP, get_token: Callable[[Context], Awaitable[str]]
) -> None:
    """Register every SAS Intelligent Decisioning tool on *mcp*."""

    @asynccontextmanager
    async def session(name: str, ctx: Context) -> AsyncIterator[httpx.AsyncClient]:
        logger.info("--- TOOL USED: %s ---", name)
        token = await get_token(ctx)
        async with make_client(token) as client:
            yield client

    def _err(exc: Exception) -> dict[str, Any]:
        if isinstance(exc, httpx.HTTPStatusError):
            code = exc.response.status_code
            body = (exc.response.text or "")[:300]
            if code == 403:
                return {"status": "forbidden", "httpStatusCode": 403,
                        "message": "The Intelligent Decisioning service denied this "
                                   "request (HTTP 403). You may lack write access to "
                                   "the target folder."}
            return {"status": "error", "httpStatusCode": code, "message": body}
        return {"status": "error", "message": f"{type(exc).__name__}: {exc}"}

    # ── Decision flows ─────────────────────────────────────────────────────────

    @mcp.tool(
        name="sas_list_decisions",
        description=(
            "List SAS Intelligent Decisioning decision flows (the decisions shown in "
            "Build Decisions). Optionally filter by `name_contains`. Returns each "
            "decision's id, name, description, owner, last-modified time, and a `url` "
            "that opens it in SAS Decision Manager."
        ),
    )
    async def sas_list_decisions(
        ctx: Context, name_contains: str | None = None, limit: int = 50, start: int = 0
    ) -> Any:
        filters = f"contains(name,'{_q(name_contains)}')" if name_contains else None
        try:
            async with session("sas_list_decisions", ctx) as client:
                items, count = await get_paged_items(
                    "/decisions/flows", client, limit=limit, start=start, filters=filters,
                    extra_params={"sortBy": "modifiedTimeStamp:descending"})
                return {"count": count, "decisions": [
                    {**{k: it.get(k) for k in ("id", "name", "description", "createdBy",
                                               "modifiedTimeStamp", "majorRevision")},
                     "url": _dm_url("decisions", it.get("id", ""))}
                    for it in items]}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_get_decision",
        description=(
            "Get one SAS decision flow by id: its name, description, signature "
            "(the decision's input/output variables), a `flow` outline of its "
            "steps/nodes (a readable text preview of the flow), a `url` that opens it "
            "in SAS Decision Manager, and `flow_diagram` (a Mermaid flowchart).\n"
            "Lead with the `flow` outline and the `url`. `flow_diagram` renders as an "
            "actual diagram only in Mermaid-capable clients; include it (in a "
            "```mermaid block) only if the user explicitly asks for a diagram."
        ),
    )
    async def sas_get_decision(decision_id: str, ctx: Context) -> Any:
        try:
            async with session("sas_get_decision", ctx) as client:
                try:
                    d = await get_json(f"/decisions/flows/{decision_id}", client, accept=_DEC)
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        return {"status": "not_found", "decision_id": decision_id}
                    raise
                steps = (d.get("flow") or {}).get("steps") or []
                outline = _flow_outline(steps)
                result = {"id": d.get("id"), "name": d.get("name"),
                          "description": d.get("description"),
                          "signature": _sig_vars(d.get("signature")),
                          "stepCount": len(steps),
                          "flow": outline or ["(empty — no steps yet)"],
                          "createdBy": d.get("createdBy"),
                          "modifiedTimeStamp": d.get("modifiedTimeStamp"),
                          "url": _dm_url("decisions", d.get("id", decision_id))}
                diagram = _flow_mermaid(steps)
                if diagram:
                    result["flow_diagram"] = diagram
                return result
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_create_decision",
        description=(
            "Create a new (empty) SAS Intelligent Decisioning decision flow with the "
            "given name. Returns its id and a `url` that opens it in SAS Decision "
            "Manager (Build Decisions) — ALWAYS share that url with the user so they "
            "can open the flow. Use sas_create_rule_set / sas_create_code_file to "
            "build the nodes that go into it."
        ),
    )
    async def sas_create_decision(
        name: str, ctx: Context, description: str = ""
    ) -> Any:
        try:
            async with session("sas_create_decision", ctx) as client:
                r = await client.post(
                    f"{VIYA_ENDPOINT}/decisions/flows",
                    json={"assetType": "decision", "name": name, "description": description},
                    headers={"Content-Type": _DEC, "Accept": _DEC})
                r.raise_for_status()
                d = r.json()
                return {"status": "created", "id": d.get("id"), "name": d.get("name"),
                        "description": d.get("description"),
                        "url": _dm_url("decisions", d.get("id", ""))}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_delete_decision",
        description="Delete a SAS decision flow by its id. This is irreversible.",
    )
    async def sas_delete_decision(decision_id: str, ctx: Context) -> Any:
        try:
            async with session("sas_delete_decision", ctx) as client:
                r = await client.delete(f"{VIYA_ENDPOINT}/decisions/flows/{decision_id}")
                if r.status_code == 404:
                    return {"status": "not_found", "decision_id": decision_id}
                r.raise_for_status()
                return {"status": "deleted", "decision_id": decision_id}
        except Exception as e:
            return _err(e)

    # ── Rule sets + rules ──────────────────────────────────────────────────────

    @mcp.tool(
        name="sas_list_rule_sets",
        description=(
            "List SAS business rule sets (Manage Business Rules). Optionally filter by "
            "`name_contains`. Returns each rule set's id, name, ruleSetType and owner."
        ),
    )
    async def sas_list_rule_sets(
        ctx: Context, name_contains: str | None = None, limit: int = 50, start: int = 0
    ) -> Any:
        filters = f"contains(name,'{_q(name_contains)}')" if name_contains else None
        try:
            async with session("sas_list_rule_sets", ctx) as client:
                items, count = await get_paged_items(
                    "/businessRules/ruleSets", client, limit=limit, start=start, filters=filters,
                    extra_params={"sortBy": "modifiedTimeStamp:descending"})
                return {"count": count, "rule_sets": [
                    {**{k: it.get(k) for k in ("id", "name", "ruleSetType", "description",
                                               "createdBy", "modifiedTimeStamp")},
                     "url": _dm_url("rules", it.get("id", ""))}
                    for it in items]}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_get_rule_set",
        description=(
            "Get one SAS business rule set by id: its name, type, signature variables, "
            "and the list of rules it contains (each rule's conditions and actions)."
        ),
    )
    async def sas_get_rule_set(rule_set_id: str, ctx: Context) -> Any:
        try:
            async with session("sas_get_rule_set", ctx) as client:
                try:
                    rs = await get_json(f"/businessRules/ruleSets/{rule_set_id}", client,
                                        accept=_RS_INTEGRAL)
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        return {"status": "not_found", "rule_set_id": rule_set_id}
                    raise
                rules_summary = []
                try:
                    items, _ = await get_paged_items(
                        f"/businessRules/ruleSets/{rule_set_id}/rules", client, limit=200)
                    for r in items:
                        rules_summary.append({
                            "id": r.get("id"),
                            "name": r.get("name"),
                            "conditional": r.get("conditional"),
                            "conditions": [f"{(c.get('term') or {}).get('name')} {c.get('expression')}"
                                           for c in (r.get("conditions") or [])],
                            "actions": [f"{(a.get('term') or {}).get('name')} = {a.get('expression')}"
                                        for a in (r.get("actions") or [])]})
                except httpx.HTTPError:
                    pass
                return {"id": rs.get("id"), "name": rs.get("name"),
                        "ruleSetType": rs.get("ruleSetType"),
                        "signature": _sig_vars(rs.get("signature")),
                        "ruleCount": len(rules_summary), "rules": rules_summary,
                        "url": _dm_url("rules", rs.get("id", rule_set_id))}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_create_rule_set",
        description=(
            "Create a new SAS business rule set. `ruleSetType` is usually 'assignment'. "
            "`variables` defines the rule set's signature — a list of "
            "{name, dataType, direction} where dataType is string/integer/decimal/"
            "boolean/date and direction is input/output/inOut (default inOut). These "
            "variables are what rules then reference. Returns the rule set id; use "
            "sas_add_rule to add rules to it."
        ),
    )
    async def sas_create_rule_set(
        name: str, ctx: Context, description: str = "",
        rule_set_type: str = "assignment", variables: list[dict] | None = None,
        folder_path: str = "/My Folder",
    ) -> Any:
        signature = []
        for v in variables or []:
            dt = str(v.get("dataType", "string"))
            direction = str(v.get("direction", "inOut"))
            if dt not in _VALID_DATATYPES:
                return {"status": "invalid", "message": f"dataType '{dt}' must be one of {_VALID_DATATYPES}."}
            if direction not in _VALID_DIRECTIONS:
                return {"status": "invalid", "message": f"direction '{direction}' must be one of {_VALID_DIRECTIONS}."}
            term = {"name": v["name"], "dataType": dt, "direction": direction}
            if v.get("length"):
                term["length"] = v["length"]
            signature.append(term)
        body = {"name": name, "description": description, "ruleSetType": rule_set_type,
                "folderPath": folder_path, "signature": signature,
                "majorRevision": 0, "minorRevision": 0}
        try:
            async with session("sas_create_rule_set", ctx) as client:
                r = await client.post(f"{VIYA_ENDPOINT}/businessRules/ruleSets",
                                      json=body, headers={"Content-Type": _RS, "Accept": _RS})
                r.raise_for_status()
                rs = r.json()
                return {"status": "created", "id": rs.get("id"), "name": rs.get("name"),
                        "ruleSetType": rs.get("ruleSetType"),
                        "variables": [v["name"] for v in signature],
                        "url": _dm_url("rules", rs.get("id", ""))}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_add_rule",
        description=(
            "Add a rule to a SAS business rule set. The rule's conditions and actions "
            "reference the rule set's signature variables (see sas_get_rule_set / "
            "sas_create_rule_set).\n"
            "  conditions: list of {variable, expression} — e.g. {'variable':'Age', "
            "'expression':'>= 21'} (an `if`/`elseif` test). Use [] for an `else` rule.\n"
            "  actions: list of {variable, expression} — e.g. {'variable':'Eligible', "
            "'expression':\"'Yes'\"} (assigns the expression to the variable).\n"
            "  conditional: 'if' (default), 'elseif', or 'else'.\n"
            "String literals in expressions must be single-quoted (e.g. \"'Yes'\")."
        ),
    )
    async def sas_add_rule(
        rule_set_id: str, ctx: Context,
        actions: list[dict], conditions: list[dict] | None = None,
        conditional: str = "if", name: str = "rule",
    ) -> Any:
        conditions = conditions or []
        try:
            async with session("sas_add_rule", ctx) as client:
                try:
                    rs = await get_json(f"/businessRules/ruleSets/{rule_set_id}", client,
                                        accept=_RS_INTEGRAL)
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        return {"status": "not_found", "rule_set_id": rule_set_id}
                    raise
                sig = {t.get("name"): t for t in rs.get("signature", [])}

                def term_for(var: str) -> dict | None:
                    t = sig.get(var)
                    if not t:
                        return None
                    return {k: t[k] for k in ("id", "name", "dataType", "direction", "length") if k in t}

                bad = [c.get("variable") for c in conditions if c.get("variable") not in sig] + \
                      [a.get("variable") for a in actions if a.get("variable") not in sig]
                if bad:
                    return {"status": "unknown_variable",
                            "message": f"Variable(s) {bad} are not in the rule set signature. "
                                       f"Available: {list(sig)}. Add them via sas_create_rule_set "
                                       "or use existing variable names."}
                rule = {
                    "name": name, "status": "valid", "version": 0, "conditional": conditional,
                    "conditions": [{"type": "decisionTable", "term": term_for(c["variable"]),
                                    "expression": c["expression"], "status": "valid"}
                                   for c in conditions],
                    "actions": [{"type": "assignment", "term": term_for(a["variable"]),
                                 "expression": a["expression"], "status": "valid"}
                                for a in actions],
                    "ruleFiredTrackingEnabled": True,
                }
                r = await client.post(f"{VIYA_ENDPOINT}/businessRules/ruleSets/{rule_set_id}/rules",
                                      json=rule, headers={"Content-Type": "application/json",
                                                          "Accept": "application/json"})
                r.raise_for_status()
                res = r.json()
                return {"status": "added", "rule_id": res.get("id"), "name": res.get("name"),
                        "rule_set_id": rule_set_id,
                        "url": _dm_url("rules", rule_set_id)}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_edit_rule",
        description=(
            "Edit an existing rule in place (change a condition/action expression, the "
            "conditional, or the rule name) and save — NOT delete-and-re-add, so the "
            "rule keeps its id and history. Identify the rule with `rule_set_id` + "
            "`rule_id` (see sas_get_rule_set for the rules in a set; the rule_id is each "
            "rule's id).\n"
            "  set_conditions: list of {variable, expression} — replaces the expression "
            "of the existing condition on that variable, e.g. {'variable':'Dealer_Type', "
            "'expression':'= \"D01\"'}.\n"
            "  set_actions: list of {variable, expression} — replaces the existing action "
            "on that variable, e.g. {'variable':'Discount', 'expression':'Discount + 5'}.\n"
            "  conditional / name: optionally change the rule's conditional ('if'/'elseif'/"
            "'else') or name.\n"
            "Only variables already used by the rule can be edited this way. String "
            "literals in expressions must be quoted."
        ),
    )
    async def sas_edit_rule(
        rule_set_id: str, rule_id: str, ctx: Context,
        set_conditions: list[dict] | None = None, set_actions: list[dict] | None = None,
        conditional: str | None = None, name: str | None = None,
    ) -> Any:
        set_conditions = set_conditions or []
        set_actions = set_actions or []
        if not (set_conditions or set_actions or conditional or name):
            return {"status": "invalid",
                    "message": "Nothing to change — pass set_conditions, set_actions, "
                               "conditional or name."}
        try:
            async with session("sas_edit_rule", ctx) as client:
                # fetch the rule and its ETag (the PUT requires an If-Match precondition)
                g = await client.get(
                    f"{VIYA_ENDPOINT}/businessRules/ruleSets/{rule_set_id}/rules/{rule_id}",
                    headers={"Accept": "application/json"})
                if g.status_code == 404:
                    return {"status": "not_found",
                            "rule_set_id": rule_set_id, "rule_id": rule_id}
                g.raise_for_status()
                etag = g.headers.get("ETag")
                rule = _trim(g.json())

                def _apply(items: list[dict], changes: list[dict], kind: str) -> str | None:
                    for ch in changes:
                        var = ch.get("variable")
                        expr = ch.get("expression")
                        target = next((it for it in items
                                       if (it.get("term") or {}).get("name") == var), None)
                        if target is None:
                            avail = sorted({(it.get("term") or {}).get("name")
                                            for it in items if it.get("term")})
                            return (f"No {kind} on variable '{var}' in this rule. "
                                    f"Editable {kind} variables: {avail or '(none)'}.")
                        target["expression"] = expr
                        target["status"] = "valid"
                    return None

                err = _apply(rule.setdefault("conditions", []), set_conditions, "condition")
                if err:
                    return {"status": "unknown_variable", "message": err}
                err = _apply(rule.setdefault("actions", []), set_actions, "action")
                if err:
                    return {"status": "unknown_variable", "message": err}
                if conditional:
                    rule["conditional"] = conditional
                if name:
                    rule["name"] = name
                put_headers = {"Content-Type": "application/json", "Accept": "application/json"}
                if etag:
                    put_headers["If-Match"] = etag
                r = await client.put(
                    f"{VIYA_ENDPOINT}/businessRules/ruleSets/{rule_set_id}/rules/{rule_id}",
                    json=rule, headers=put_headers)
                if r.status_code == 404:
                    return {"status": "not_found", "rule_set_id": rule_set_id, "rule_id": rule_id}
                r.raise_for_status()
                res = r.json()
                return {"status": "updated", "rule_id": res.get("id"), "name": res.get("name"),
                        "conditional": res.get("conditional"),
                        "conditions": [f"{(c.get('term') or {}).get('name')} {c.get('expression')}"
                                       for c in (res.get("conditions") or [])],
                        "actions": [f"{(a.get('term') or {}).get('name')} = {a.get('expression')}"
                                    for a in (res.get("actions") or [])],
                        "rule_set_id": rule_set_id, "url": _dm_url("rules", rule_set_id)}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_delete_rule_set",
        description="Delete a SAS business rule set by its id. This is irreversible.",
    )
    async def sas_delete_rule_set(rule_set_id: str, ctx: Context) -> Any:
        try:
            async with session("sas_delete_rule_set", ctx) as client:
                r = await client.delete(f"{VIYA_ENDPOINT}/businessRules/ruleSets/{rule_set_id}")
                if r.status_code == 404:
                    return {"status": "not_found", "rule_set_id": rule_set_id}
                r.raise_for_status()
                return {"status": "deleted", "rule_set_id": rule_set_id}
        except Exception as e:
            return _err(e)

    # ── Code files (SQL / Python decision nodes) ───────────────────────────────

    @mcp.tool(
        name="sas_list_code_files",
        description=(
            "List SAS Intelligent Decisioning code files (the SQL / Python nodes used "
            "in decisions). Optionally filter by `name_contains`."
        ),
    )
    async def sas_list_code_files(
        ctx: Context, name_contains: str | None = None, limit: int = 50, start: int = 0
    ) -> Any:
        filters = f"contains(name,'{_q(name_contains)}')" if name_contains else None
        try:
            async with session("sas_list_code_files", ctx) as client:
                items, count = await get_paged_items(
                    "/decisions/codeFiles", client, limit=limit, start=start, filters=filters,
                    extra_params={"sortBy": "modifiedTimeStamp:descending"})
                return {"count": count, "code_files": [
                    {**{k: it.get(k) for k in ("id", "name", "type", "description",
                                               "createdBy", "modifiedTimeStamp")},
                     "url": _dm_url("codeFiles", it.get("id", ""))}
                    for it in items]}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_get_code_file",
        description=(
            "Get one SAS decision code file by id: its name, language/type, and "
            "signature, plus the source code content."
        ),
    )
    async def sas_get_code_file(code_file_id: str, ctx: Context) -> Any:
        try:
            async with session("sas_get_code_file", ctx) as client:
                try:
                    cf = await get_json(f"/decisions/codeFiles/{code_file_id}", client, accept=_CF)
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        return {"status": "not_found", "code_file_id": code_file_id}
                    raise
                content = None
                for link in cf.get("links", []):
                    if link.get("rel") == "content":
                        try:
                            cr = await client.get(f"{VIYA_ENDPOINT}{link['href']}")
                            if cr.is_success:
                                content = cr.text
                        except httpx.HTTPError:
                            pass
                        break
                return {"id": cf.get("id"), "name": cf.get("name"), "type": cf.get("type"),
                        "signature": _sig_vars(cf.get("signature")), "code": content,
                        "url": _dm_url("codeFiles", cf.get("id", code_file_id))}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_create_code_file",
        description=(
            "Create a SAS Intelligent Decisioning code file (a SQL or Python node) "
            "from source code. `language` is 'sql' or 'python'. The code is uploaded "
            "to the Files service and registered as a decision code file; returns its "
            "id so it can be used as a node in a decision. (For Python, the first line "
            "may be a SAS signature-extension comment defining the node's inputs/"
            "outputs.)"
        ),
    )
    async def sas_create_code_file(
        name: str, code: str, ctx: Context, language: str = "sql", description: str = ""
    ) -> Any:
        language = language.lower().strip()
        if language not in ("sql", "python"):
            return {"status": "invalid", "message": "language must be 'sql' or 'python'."}
        ext = "sql" if language == "sql" else "py"
        cf_type = "decisionSQLCodeFile" if language == "sql" else "decisionPythonFile"
        fname = f"{name}.{ext}"
        try:
            async with session("sas_create_code_file", ctx) as client:
                up = await client.post(
                    f"{VIYA_ENDPOINT}/files/files",
                    content=code.encode("utf-8"),
                    headers={"Content-Type": "text/plain",
                             "Content-Disposition": f'attachment; filename="{fname}"',
                             "Accept": "application/json"})
                up.raise_for_status()
                file_id = up.json()["id"]
                reg = await client.post(
                    f"{VIYA_ENDPOINT}/decisions/codeFiles",
                    json={"assetType": "codeFile", "name": name, "description": description,
                          "fileUri": f"/files/files/{file_id}", "type": cf_type,
                          "outputType": "dataGrid", "testCustomContextUri": "", "checkout": False},
                    headers={"Content-Type": _CF, "Accept": _CF})
                reg.raise_for_status()
                cf = reg.json()
                return {"status": "created", "id": cf.get("id"), "name": cf.get("name"),
                        "type": cf.get("type"), "language": language,
                        "file_uri": f"/files/files/{file_id}",
                        "url": _dm_url("codeFiles", cf.get("id", ""))}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_delete_code_file",
        description="Delete a SAS decision code file by its id. This is irreversible.",
    )
    async def sas_delete_code_file(code_file_id: str, ctx: Context) -> Any:
        try:
            async with session("sas_delete_code_file", ctx) as client:
                r = await client.delete(f"{VIYA_ENDPOINT}/decisions/codeFiles/{code_file_id}")
                if r.status_code == 404:
                    return {"status": "not_found", "code_file_id": code_file_id}
                r.raise_for_status()
                return {"status": "deleted", "code_file_id": code_file_id}
        except Exception as e:
            return _err(e)

    # ── Validate / generate code / publish (deploy) ────────────────────────────

    @mcp.tool(
        name="sas_validate_expression",
        description=(
            "Validate a SAS Intelligent Decisioning expression (a rule condition/action "
            "or a branch condition) against a set of variables, BEFORE using it in a "
            "rule or decision. `expression` is the SAS expression (e.g. \"Age >= 21\" or "
            "\"Eligible = 'Yes'\"). `variables` is the list of variables it may reference "
            "— [{name, dataType}] with dataType string/integer/decimal/boolean/date. "
            "Returns valid=true, or valid=false with the error message. Every variable "
            "the expression uses must be listed or it is invalid."
        ),
    )
    async def sas_validate_expression(
        expression: str, ctx: Context, variables: list[dict] | None = None
    ) -> Any:
        sig = [{"name": v["name"], "dataType": str(v.get("dataType", "string"))}
               for v in (variables or [])]
        try:
            async with session("sas_validate_expression", ctx) as client:
                r = await client.post(
                    f"{VIYA_ENDPOINT}/decisions/validations/expressionValidations",
                    json={"expression": expression, "signature": sig},
                    headers={"Content-Type": _VALIDATION_REQ, "Accept": _VALIDATION_RESP})
                r.raise_for_status()
                d = r.json()
                valid = bool(d.get("valid"))
                out: dict[str, Any] = {"expression": expression, "valid": valid}
                if not valid:
                    msg = (d.get("error") or {}).get("message", "")
                    # surface the meaningful WARNING/ERROR line, not the whole DS2 log
                    key = next((ln.strip() for ln in str(msg).splitlines()
                                if "ERROR" in ln or "WARNING" in ln), "")
                    out["error"] = key or (str(msg)[:300] if msg else "invalid expression")
                return out
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_get_decision_code",
        description=(
            "Get the generated SAS DS2 code for a decision flow (what actually runs when "
            "the decision is published/scored). Returns the code as text. Useful to "
            "inspect or review the decision's logic."
        ),
    )
    async def sas_get_decision_code(decision_id: str, ctx: Context) -> Any:
        try:
            async with session("sas_get_decision_code", ctx) as client:
                r = await client.get(f"{VIYA_ENDPOINT}/decisions/flows/{decision_id}/code",
                                     headers={"Accept": _DS2})
                if r.status_code == 404:
                    return {"status": "not_found", "decision_id": decision_id}
                r.raise_for_status()
                return {"decision_id": decision_id, "language": "ds2", "code": r.text,
                        "url": _dm_url("decisions", decision_id)}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_list_publish_destinations",
        description=(
            "List the destinations a SAS decision can be published to (e.g. 'maslocal' — "
            "the Micro Analytic Score service, the default; or container/CAS destinations). "
            "Returns each destination's name, type and description. Use a name with "
            "sas_publish_decision."
        ),
    )
    async def sas_list_publish_destinations(ctx: Context) -> Any:
        try:
            async with session("sas_list_publish_destinations", ctx) as client:
                items, count = await get_paged_items(
                    "/modelPublish/destinations", client, limit=200)
                return {"count": count, "destinations": [
                    {k: it.get(k) for k in ("name", "destinationType", "description")}
                    for it in items]}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_publish_decision",
        description=(
            "Publish (deploy) a SAS Intelligent Decisioning decision so it can be executed "
            "for scoring. Publishes the decision's current revision to a destination "
            "(`destination_name` defaults to 'maslocal', the Micro Analytic Score service "
            "— see sas_list_publish_destinations). Returns the publish id and status; check "
            "details with sas_get_publish_log. This is the 'deploy' step after building a "
            "decision."
        ),
    )
    async def sas_publish_decision(
        decision_id: str, ctx: Context, destination_name: str = "maslocal"
    ) -> Any:
        try:
            async with session("sas_publish_decision", ctx) as client:
                try:
                    dec = await get_json(f"/decisions/flows/{decision_id}", client, accept=_DEC)
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        return {"status": "not_found", "decision_id": decision_id}
                    raise
                name = dec.get("name", "decision")
                major, minor = dec.get("majorRevision", 1), dec.get("minorRevision", 0)
                # resolve the current (latest) revision id
                revs, _ = await get_paged_items(
                    f"/decisions/flows/{decision_id}/revisions", client, limit=1,
                    extra_params={"sortBy": "modifiedTimeStamp:descending"})
                if not revs:
                    return {"status": "error", "message": "Could not resolve a revision to publish."}
                rev = revs[0].get("id")
                model_name = f"{_sanitize_ident(name)}{major}_{minor}"
                src = f"/decisions/flows/{decision_id}/revisions/{rev}"
                code_uri = (f"{src}/code?rootPackageName={model_name}&lookupMode=PACKAGE"
                            "&traversedPathFlag=false&isGeneratingRuleFiredColumn=false"
                            "&codeTarget=microAnalyticService")
                body = {
                    "name": f'Decision "{name} ({major}.{minor})" published via MCP',
                    "state": "active", "notes": "Published via SAS MCP",
                    "modelContents": [{
                        "overwrite": True, "modelName": model_name,
                        "codeMediaType": "text/vnd.sas.source.ds2.async", "codeType": "ds2",
                        "codeUri": code_uri, "publishLevel": "decision", "sourceUri": src,
                        "properties": {"open.api.version": "1.0"}}],
                    "reloadModelTable": True, "destinationName": destination_name,
                    "tags": [], "properties": {}}
                r = await client.post(
                    f"{VIYA_ENDPOINT}/modelPublish/models", json=body,
                    headers={"Content-Type": _PUBLISH_REQ, "Accept": "application/json"})
                if r.status_code == 404:
                    return {"status": "error",
                            "message": f"Destination '{destination_name}' not found. "
                                       "Use sas_list_publish_destinations."}
                r.raise_for_status()
                d = r.json()
                item = (d.get("items") or [d])[0]
                err = item.get("errorResponse") or {}
                return {"status": "error" if err else "published",
                        "publish_id": item.get("id"), "model_name": model_name,
                        "destination": destination_name, "decision": name,
                        "error": err.get("message") if err else None,
                        "message": "Published. Check sas_get_publish_log for details."}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_get_publish_log",
        description=(
            "Get the publishing log for a publish job (from sas_publish_decision), to "
            "confirm whether the decision deployed successfully and see any messages."
        ),
    )
    async def sas_get_publish_log(publish_id: str, ctx: Context) -> Any:
        try:
            async with session("sas_get_publish_log", ctx) as client:
                r = await client.get(f"{VIYA_ENDPOINT}/modelPublish/models/{publish_id}/log",
                                     headers={"Accept": "application/json"})
                if r.status_code == 404:
                    return {"status": "not_found", "publish_id": publish_id}
                r.raise_for_status()
                d = r.json()
                log = d.get("log", "")
                ok = "successfully" in str(log).lower() or "completed" in str(log).lower()
                return {"publish_id": publish_id,
                        "status": "success" if ok else "see_log",
                        "log": str(log)[:4000]}
        except Exception as e:
            return _err(e)

    # ── Flow assembly: add real nodes / branches INTO a decision ───────────────

    async def _resolve_node_step(client, node: dict, existing_names: set) -> tuple[dict, list]:
        """Build ``(flow_step, new_signature_vars)`` for a node spec
        ``{"type": "rule_set"|"code_file"|"model"|"decision", "id": "..."}``. Raises
        ValueError on a bad spec; lets httpx 404s propagate so the caller can report
        not-found."""
        ntype = str((node or {}).get("type", "")).lower().replace(" ", "_")
        nid = (node or {}).get("id")
        if not nid:
            raise ValueError("each node needs an 'id'")
        sid = str(uuid.uuid4())
        if ntype in ("model",):
            model = await get_json(f"/modelRepository/models/{nid}", client, accept=_MODEL)
            mvars, _ = await get_paged_items(
                f"/modelRepository/models/{nid}/variables", client, limit=500)
            mappings, new_vars = _build_model_mappings(mvars, existing_names)
            step = {"id": sid, "originalId": sid,
                    "type": "application/vnd.sas.decision.step.model",
                    "model": {"name": model.get("name"), "id": nid},
                    "mappings": mappings}
            return step, new_vars
        if ntype in ("decision", "sub_decision", "subdecision"):
            sub = await get_json(f"/decisions/flows/{nid}", client, accept=_DEC)
            revs, _ = await get_paged_items(
                f"/decisions/flows/{nid}/revisions", client, limit=1,
                extra_params={"sortBy": "modifiedTimeStamp:descending"})
            if not revs:
                raise ValueError(f"could not resolve a revision for decision {nid}")
            mappings, new_vars = _build_node_mappings(sub.get("signature", []), existing_names)
            step = {"id": sid, "originalId": sid,
                    "type": "application/vnd.sas.decision.step.decision",
                    "decision": {"name": sub.get("name"), "id": nid,
                                 "versionId": revs[0].get("id"),
                                 "versionName": f"{sub.get('majorRevision', 1)}.{sub.get('minorRevision', 0)}"},
                    "mappings": mappings}
            return step, new_vars
        if ntype in ("rule_set", "ruleset"):
            rs = await get_json(f"/businessRules/ruleSets/{nid}", client, accept=_RS_INTEGRAL)
            revs, _ = await get_paged_items(
                f"/businessRules/ruleSets/{nid}/revisions", client, limit=1,
                extra_params={"sortBy": "modifiedTimeStamp:descending"})
            if not revs:
                raise ValueError(f"could not resolve a revision for rule set {nid}")
            mappings, new_vars = _build_node_mappings(rs.get("signature", []), existing_names)
            step = {"id": sid, "originalId": sid,
                    "type": "application/vnd.sas.decision.step.ruleset",
                    "ruleSetType": rs.get("ruleSetType", "assignment"),
                    "ruleset": {"name": rs.get("name"), "id": nid,
                                "versionId": revs[0].get("id"),
                                "versionName": f"{rs.get('majorRevision', 1)}.{rs.get('minorRevision', 0)}"},
                    "mappings": mappings}
            return step, new_vars
        if ntype in ("code_file", "codefile"):
            cf = await get_json(f"/decisions/codeFiles/{nid}", client, accept=_CF)
            revs, _ = await get_paged_items(
                f"/decisions/codeFiles/{nid}/revisions", client, limit=1,
                extra_params={"sortBy": "modifiedTimeStamp:descending"})
            if not revs:
                raise ValueError(f"could not resolve a revision for code file {nid}")
            mappings, new_vars = _build_node_mappings(cf.get("signature", []), existing_names)
            step = {"id": sid, "originalId": sid,
                    "type": "application/vnd.sas.decision.step.custom.object",
                    "customObject": {"uri": f"/decisions/codeFiles/{nid}/revisions/{revs[0].get('id')}",
                                     "name": cf.get("name"), "type": cf.get("type"), "isRestDNT": False},
                    "mappings": mappings}
            return step, new_vars
        raise ValueError(f"unknown node type {ntype!r}; use 'rule_set', 'code_file', "
                         "'model' or 'decision'")

    async def _get_decision_etag(client, decision_id):
        g = await client.get(f"{VIYA_ENDPOINT}/decisions/flows/{decision_id}",
                             headers={"Accept": _DEC})
        g.raise_for_status()
        return g.json(), g.headers.get("ETag")

    async def _put_decision(client, decision_id, dec, etag):
        headers = {"Content-Type": _DEC, "Accept": _DEC}
        if etag:
            headers["If-Match"] = etag
        r = await client.put(f"{VIYA_ENDPOINT}/decisions/flows/{decision_id}",
                             json=dec, headers=headers)
        r.raise_for_status()
        updated = r.json()
        steps = (updated.get("flow") or {}).get("steps") or []
        return {"status": "added", "decision_id": decision_id,
                "decision": updated.get("name"), "stepCount": len(steps),
                "flow": _flow_outline(steps), "url": _dm_url("decisions", decision_id)}

    async def _append_single_node(client, decision_id, node):
        dec, etag = await _get_decision_etag(client, decision_id)
        existing = {v.get("name") for v in dec.get("signature", [])}
        step, new_vars = await _resolve_node_step(client, node, existing)
        dec.setdefault("signature", []).extend(new_vars)
        dec.setdefault("flow", {}).setdefault("steps", []).append(step)
        return await _put_decision(client, decision_id, dec, etag)

    @mcp.tool(
        name="sas_add_rule_set_to_decision",
        description=(
            "Add a rule set as a NODE inside a decision flow — this is how you build a "
            "real, non-empty decision. The rule set's variables are auto-wired to the "
            "decision's variables (created if needed). After this the decision actually "
            "contains the rule set (not just start/end). Returns the new step count and "
            "flow outline. Build a decision by: sas_create_decision, then call this (and "
            "sas_add_code_file_to_decision / sas_add_condition_branch) for each node."
        ),
    )
    async def sas_add_rule_set_to_decision(
        decision_id: str, rule_set_id: str, ctx: Context
    ) -> Any:
        try:
            async with session("sas_add_rule_set_to_decision", ctx) as client:
                return await _append_single_node(
                    client, decision_id, {"type": "rule_set", "id": rule_set_id})
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return {"status": "not_found",
                        "message": f"Decision or rule set not found ({decision_id} / {rule_set_id})."}
            return _err(e)
        except ValueError as e:
            return {"status": "error", "message": str(e)}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_add_code_file_to_decision",
        description=(
            "Add a code file (SQL/Python node) as a NODE inside a decision flow. The code "
            "file's input/output variables are auto-wired to the decision's variables. "
            "After this the decision actually contains the code node. Use together with "
            "sas_add_rule_set_to_decision / sas_add_condition_branch to assemble a flow."
        ),
    )
    async def sas_add_code_file_to_decision(
        decision_id: str, code_file_id: str, ctx: Context
    ) -> Any:
        try:
            async with session("sas_add_code_file_to_decision", ctx) as client:
                return await _append_single_node(
                    client, decision_id, {"type": "code_file", "id": code_file_id})
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return {"status": "not_found",
                        "message": f"Decision or code file not found ({decision_id} / {code_file_id})."}
            return _err(e)
        except ValueError as e:
            return {"status": "error", "message": str(e)}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_add_model_to_decision",
        description=(
            "Add a MODEL node (a model / analytic store from Model Manager) as a node "
            "inside a decision flow — the Build Decisions 'Model' node. `model_id` is a "
            "model in the SAS model repository (see sas_list_models if available, or the "
            "model's id in Model Manager). The model's input/output variables are read "
            "from the repository and auto-wired to the decision's variables (created if "
            "needed) — its inputs are read from the decision, its scored outputs "
            "(EM_*/P_*/I_* etc.) are written back. Returns the new step count and flow "
            "outline."
        ),
    )
    async def sas_add_model_to_decision(
        decision_id: str, model_id: str, ctx: Context
    ) -> Any:
        try:
            async with session("sas_add_model_to_decision", ctx) as client:
                return await _append_single_node(
                    client, decision_id, {"type": "model", "id": model_id})
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return {"status": "not_found",
                        "message": f"Decision or model not found ({decision_id} / {model_id})."}
            return _err(e)
        except ValueError as e:
            return {"status": "error", "message": str(e)}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_add_decision_to_decision",
        description=(
            "Add a SUB-DECISION node — another decision flow nested as a node inside this "
            "decision flow (the Build Decisions 'Decision' node, a decision inside a "
            "decision). `sub_decision_id` is the decision to embed. Its signature "
            "variables are auto-wired to this decision's variables (created if needed). "
            "Returns the new step count and flow outline."
        ),
    )
    async def sas_add_decision_to_decision(
        decision_id: str, sub_decision_id: str, ctx: Context
    ) -> Any:
        if sub_decision_id == decision_id:
            return {"status": "invalid", "message": "A decision cannot contain itself."}
        try:
            async with session("sas_add_decision_to_decision", ctx) as client:
                return await _append_single_node(
                    client, decision_id, {"type": "decision", "id": sub_decision_id})
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return {"status": "not_found",
                        "message": f"Decision not found ({decision_id} / {sub_decision_id})."}
            return _err(e)
        except ValueError as e:
            return {"status": "error", "message": str(e)}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_add_condition_branch",
        description=(
            "Add an IF / THEN / ELSE branch (a condition node) to a decision flow — this is "
            "how you build ADVANCED flows that route to different nodes based on data.\n"
            "`condition_variable` must be a variable already in the decision (e.g. one a "
            "prior node outputs, or a rule-set variable) — call sas_get_decision to see the "
            "decision's variables. `operator` is one of =, <>, >, <, >=, <=. `value` is the "
            "constant compared against.\n"
            "`then_nodes` / `else_nodes` are the nodes to run in each branch — each a list of "
            "{type:'rule_set'|'code_file', id:'<asset id>'}. Either branch may be empty.\n"
            "Example: route on Age — then_nodes=[{type:'code_file', id:'...approve...'}], "
            "else_nodes=[{type:'code_file', id:'...decline...'}], condition_variable='Age', "
            "operator='>=', value='21'."
        ),
    )
    async def sas_add_condition_branch(
        decision_id: str, condition_variable: str, operator: str, value: str, ctx: Context,
        then_nodes: list[dict] | None = None, else_nodes: list[dict] | None = None,
    ) -> Any:
        ops = {"=", "<>", "!=", ">", "<", ">=", "<="}
        if operator not in ops:
            return {"status": "invalid", "message": f"operator must be one of {sorted(ops)}."}
        then_nodes = then_nodes or []
        else_nodes = else_nodes or []
        try:
            async with session("sas_add_condition_branch", ctx) as client:
                dec, etag = await _get_decision_etag(client, decision_id)
                sig = dec.get("signature", [])
                existing = {v.get("name") for v in sig}
                lhs = next((v for v in sig if v.get("name") == condition_variable), None)
                if not lhs:
                    return {"status": "unknown_variable",
                            "message": (f"'{condition_variable}' is not a variable in this decision "
                                        f"yet. Add a node that produces it first. Available: "
                                        f"{sorted(existing) or '(none — the decision is empty)'}.")}
                then_steps, else_steps, all_new = [], [], []
                for node in then_nodes:
                    st, nv = await _resolve_node_step(client, node, existing)
                    then_steps.append(st); all_new += nv
                for node in else_nodes:
                    st, nv = await _resolve_node_step(client, node, existing)
                    else_steps.append(st); all_new += nv
                cid, ot, of = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
                cond_step = {
                    "id": cid, "originalId": cid,
                    "type": "application/vnd.sas.decision.step.condition", "version": 1,
                    "onTrue": {"id": ot, "steps": then_steps, "version": 1, "parentId": cid},
                    "onFalse": {"id": of, "steps": else_steps, "version": 1},
                    "name": "",
                    "condition": {
                        "lhsTerm": {"id": lhs.get("id"), "name": lhs.get("name"),
                                    "dataType": lhs.get("dataType"),
                                    "direction": lhs.get("direction", "inOut"), "description": ""},
                        "operator": operator, "rhsConstant": str(value)}}
                dec.setdefault("signature", []).extend(all_new)
                dec.setdefault("flow", {}).setdefault("steps", []).append(cond_step)
                return await _put_decision(client, decision_id, dec, etag)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return {"status": "not_found", "decision_id": decision_id}
            return _err(e)
        except ValueError as e:
            return {"status": "error", "message": str(e)}
        except Exception as e:
            return _err(e)

    # ── Test / run (score) a decision or rule set ──────────────────────────────

    async def _latest_revision_uri(client, base: str) -> str | None:
        revs, _ = await get_paged_items(
            f"{base}/revisions", client, limit=1,
            extra_params={"sortBy": "modifiedTimeStamp:descending"})
        return f"{base}/revisions/{revs[0].get('id')}" if revs else None

    @mcp.tool(
        name="sas_test_decision",
        description=(
            "Test / run (score) a decision or rule set against a CAS input table and "
            "return a sample of the scored output — the Build Decisions 'Test' feature. "
            "Creates a score definition, runs it, waits for it to finish, and returns the "
            "first rows of the output table.\n"
            "  decision_id: the decision or rule set to test — its NAME or its id. A name "
            "is resolved automatically (and whether it's a decision or a rule set is "
            "auto-detected), so you do not need to look up the id first.\n"
            "  object_type: optional hint, 'decision' (default) or 'rule_set'; ignored when "
            "a name is given (the type is detected).\n"
            "  input_table / input_library / server: the CAS table of input data to score "
            "(default library 'Public', server 'cas-shared-default'). The table is loaded "
            "into CAS automatically if it isn't already (scoring can't open an unloaded "
            "table).\n"
            "  max_rows: how many output rows to return (default 10).\n"
            "  retries: how many times to re-run the scoring on a transient failure "
            "(default 2) — a CAS/host hiccup mid-scoring is retried automatically.\n"
            "Returns the execution state, how many attempts it took, the output table "
            "name, and the sampled rows. If it is still running when the wait elapses, "
            "returns the execution id so you can re-check."
        ),
    )
    async def sas_test_decision(
        decision_id: str, input_table: str, ctx: Context,
        object_type: str = "decision", input_library: str = "Public",
        server: str = "cas-shared-default", max_rows: int = 10, wait_seconds: int = 90,
        retries: int = 2,
    ) -> Any:
        otype = object_type.lower().strip().replace(" ", "_")
        otype = "rule_set" if otype == "ruleset" else otype
        if otype not in ("decision", "rule_set"):
            return {"status": "invalid", "message": "object_type must be 'decision' or 'rule_set'."}
        _DECISION = ("/decisions/flows", "decision", _DEC)
        _RULESET = ("/businessRules/ruleSets", "ruleSet", _RS_INTEGRAL)
        try:
            async with session("sas_test_decision", ctx) as client:
                # Resolve the object to a (collection, descriptorType, accept) tuple.
                # The caller — often an LLM — may pass a NAME instead of the UUID, and
                # SAS returns 403 (not 404) for a non-id path, so look names up and
                # auto-detect whether the target is a decision or a rule set.
                obj = base = desc_type = accept = None
                if _UUID_RE.match(decision_id.strip()):
                    # try the hinted type first, then the other (handles a wrong hint)
                    order = [_RULESET, _DECISION] if otype == "rule_set" else [_DECISION, _RULESET]
                    for coll, dt, acc in order:
                        try:
                            obj = await get_json(f"{coll}/{decision_id}", client, accept=acc)
                            base, desc_type, accept = f"{coll}/{decision_id}", dt, acc
                            break
                        except httpx.HTTPStatusError as e:
                            if e.response.status_code in (403, 404):
                                continue
                            raise
                    if obj is None:
                        return {"status": "not_found", "decision_id": decision_id}
                else:
                    name = decision_id.strip()
                    flt = f"eq(name,'{_q(name)}')"
                    for coll, dt, acc in (_DECISION, _RULESET):
                        items, _ = await get_paged_items(coll, client, limit=1, filters=flt)
                        if items:
                            rid = items[0].get("id")
                            obj = await get_json(f"{coll}/{rid}", client, accept=acc)
                            base, desc_type, accept = f"{coll}/{rid}", dt, acc
                            break
                    if obj is None:
                        return {"status": "not_found",
                                "message": f"No decision or rule set named '{name}' was found. "
                                           "Pass its name or id (see sas_list_decisions / "
                                           "sas_list_rule_sets)."}
                rev_uri = await _latest_revision_uri(client, base)
                if not rev_uri:
                    return {"status": "error", "message": "Could not resolve a revision to test."}
                obj_name = obj.get("name", "object")
                # a unique suffix keeps the score definition / output table from colliding
                # with a prior test run (the service rejects a duplicate definition name)
                uniq = uuid.uuid4().hex[:6]
                test_name = f"{obj_name}_Test_{uniq}"
                table_base = f"{_sanitize_ident(obj_name)}_Test_{uniq}"
                # map each signature variable to a same-named column in the input table
                mappings = []
                for t in obj.get("signature", []):
                    nm = t.get("name")
                    if not nm:
                        continue
                    term = {"name": nm, "dataType": t.get("dataType", "string"),
                            "direction": t.get("direction", "inOut")}
                    if t.get("length"):
                        term["length"] = t["length"]
                    mappings.append({"variableName": nm, "mappingType": "datasource",
                                     "mappingValue": nm, "term": term})
                # Ensure the input CAS table is loaded — a scoring run cannot open an
                # unloaded table (it fails with "Table 'X' could not be loaded"). This
                # loads it if a source exists; it's a no-op if already loaded.
                try:
                    await client.put(
                        f"{VIYA_ENDPOINT}/casManagement/servers/{server}/caslibs/"
                        f"{input_library}/tables/{input_table}/state",
                        params={"value": "loaded"}, headers={"Accept": "application/json"})
                except httpx.HTTPError:
                    pass
                # resolve a parent folder for the score definition (the user's home folder)
                parent_uri = None
                try:
                    home = await get_json("/folders/folders/@myFolder", client)
                    if home.get("id"):
                        parent_uri = f"/folders/folders/{home['id']}"
                except httpx.HTTPError:
                    pass
                def_body = {
                    "name": test_name,
                    "inputData": {"type": "CASTable", "libraryName": input_library,
                                  "serverName": server, "tableName": input_table},
                    "objectDescriptor": {"type": desc_type, "name": obj_name, "uri": rev_uri},
                    "properties": {"version": "1.0", "test": "true",
                                   "outputServerName": server, "outputLibraryName": input_library,
                                   "tableBaseName": table_base},
                    "mappings": mappings}
                params = {"parentFolderUri": parent_uri} if parent_uri else None
                dr = await client.post(f"{VIYA_ENDPOINT}/scoreDefinitions/definitions",
                                       json=def_body, params=params,
                                       headers={"Content-Type": "application/json",
                                                "Accept": "application/json"})
                dr.raise_for_status()
                definition_id = dr.json().get("id")
                # Run it, retrying the execution on a transient failure (a CAS/host
                # hiccup mid-scoring surfaces as state="failed"). The definition is
                # reused; each attempt gets its own guid + timestamped output table.
                deadline = max(1, wait_seconds)
                attempts = max(1, retries + 1)
                execu: dict[str, Any] = {}
                execution_id = None
                state = None
                used = 0
                for attempt in range(attempts):
                    used = attempt + 1
                    exec_body = {
                        "scoreDefinitionId": definition_id, "type": "scoreDefinition",
                        "name": f"Execution for {test_name}",
                        "hints": {"objectURI": rev_uri, "inputTableName": input_table,
                                  "inputLibraryName": input_library,
                                  "useGlobalVariableCurrentValues": "true",
                                  "scoreRequestGuid": str(uuid.uuid4())}}
                    try:
                        er = await client.post(f"{VIYA_ENDPOINT}/scoreExecution/executions",
                                               json=exec_body,
                                               headers={"Content-Type": "application/json",
                                                        "Accept": "application/json"})
                        er.raise_for_status()
                        execu = er.json()
                        execution_id = execu.get("id")
                        state = execu.get("state")
                        waited = 0
                        while state in ("running", "pending", None) and waited < deadline:
                            await asyncio.sleep(min(3, deadline - waited))
                            waited += 3
                            pr = await client.get(
                                f"{VIYA_ENDPOINT}/scoreExecution/executions/{execution_id}",
                                headers={"Accept": "application/json"})
                            if pr.is_success:
                                execu = pr.json()
                                state = execu.get("state")
                    except httpx.HTTPError:
                        state = "error"
                    if state == "completed" or state not in ("failed", "canceled", "error"):
                        break  # success, or still-running (don't retry a hang)
                    if attempt < attempts - 1:
                        await asyncio.sleep(2)  # brief backoff before retrying
                result: dict[str, Any] = {
                    "status": "completed" if state == "completed" else state or "running",
                    "decision": obj_name, "definition_id": definition_id,
                    "execution_id": execution_id, "state": state, "attempts": used,
                    "url": _dm_url("decisions" if desc_type == "decision" else "rules", decision_id)}
                if state != "completed":
                    if state in ("failed", "canceled", "error"):
                        base_msg = (execu.get("error") or {}).get("message") or \
                            "Score execution did not complete successfully."
                        result["message"] = (f"{base_msg} (tried {used}x)" if used > 1 else base_msg)
                    else:
                        result["message"] = ("Still running — re-run sas_test_decision or check "
                                              f"score execution {execution_id} shortly.")
                    return result
                out = execu.get("outputTable") or {}
                table = out.get("tableName")
                lib = out.get("libraryName", input_library)
                srv = out.get("serverName", server)
                result["output_table"] = table
                if table:
                    try:
                        cols, _ = await get_paged_items(
                            f"/casManagement/servers/{srv}/caslibs/{lib}/tables/{table}/columns",
                            client, limit=500)
                        names = [c.get("name") for c in cols]
                        rr = await client.get(
                            f"{VIYA_ENDPOINT}/casRowSets/servers/{srv}/caslibs/{lib}/tables/{table}/rows",
                            params={"formatted": "true", "limit": max_rows},
                            headers={"Accept": "application/json"})
                        rows = []
                        if rr.is_success:
                            for item in (rr.json().get("items") or [])[:max_rows]:
                                cells = [str(c).strip() for c in (item.get("cells") or [])]
                                rows.append(dict(zip(names, cells, strict=False)) if names else cells)
                        result["row_count_returned"] = len(rows)
                        result["rows"] = rows
                    except httpx.HTTPError:
                        result["message"] = ("Scored, but the output rows could not be read. "
                                              f"See output table {table} in {lib}.")
                return result
        except Exception as e:
            return _err(e)

    # ── Global variables (reference data) ──────────────────────────────────────

    @mcp.tool(
        name="sas_list_global_variables",
        description=(
            "List SAS Intelligent Decisioning global variables (reference-data global "
            "variables that decisions and rules can read). Returns each variable's name, "
            "dataType and current value."
        ),
    )
    async def sas_list_global_variables(
        ctx: Context, limit: int = 100, start: int = 0
    ) -> Any:
        try:
            async with session("sas_list_global_variables", ctx) as client:
                items, count = await get_paged_items(
                    "/referenceData/globalVariables", client, limit=limit, start=start,
                    extra_params={"sortBy": "modifiedTimeStamp:descending"})
                return {"count": count, "global_variables": [
                    {k: it.get(k) for k in ("id", "name", "dataType", "length",
                                            "value", "defaultValue", "description")}
                    for it in items]}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_create_global_variable",
        description=(
            "Create a new SAS Intelligent Decisioning global variable (reference data) — "
            "a named value available to every decision and rule. `data_type` is one of "
            "string/integer/decimal/double/boolean/date/datetime. `value` is the variable's "
            "value (and default). For string variables pass `length` (default 32)."
        ),
    )
    async def sas_create_global_variable(
        name: str, ctx: Context, data_type: str = "string",
        value: Any = None, length: int | None = None, description: str = "",
    ) -> Any:
        dt = str(data_type).strip()
        if dt not in _VALID_DATATYPES:
            return {"status": "invalid",
                    "message": f"data_type '{dt}' must be one of {_VALID_DATATYPES}."}
        body: dict[str, Any] = {"name": name, "dataType": dt, "description": description}
        if dt == "string":
            body["length"] = length or 32
        if value is not None:
            body["value"] = value
            body["defaultValue"] = value
        try:
            async with session("sas_create_global_variable", ctx) as client:
                r = await client.post(f"{VIYA_ENDPOINT}/referenceData/globalVariables",
                                      json=body, headers={"Content-Type": _GV, "Accept": _GV})
                r.raise_for_status()
                gv = r.json()
                return {"status": "created", "id": gv.get("id"), "name": gv.get("name"),
                        "dataType": gv.get("dataType"), "value": gv.get("value")}
        except Exception as e:
            return _err(e)

    # ── Lookup tables (reference-data domains) ─────────────────────────────────

    @mcp.tool(
        name="sas_list_lookup_tables",
        description=(
            "List SAS Intelligent Decisioning lookup tables (reference-data domains — the "
            "key/value tables that decisions and rules look values up in). Returns each "
            "domain's id, name and description."
        ),
    )
    async def sas_list_lookup_tables(
        ctx: Context, name_contains: str | None = None, limit: int = 50, start: int = 0
    ) -> Any:
        filters = f"contains(name,'{_q(name_contains)}')" if name_contains else None
        try:
            async with session("sas_list_lookup_tables", ctx) as client:
                items, count = await get_paged_items(
                    "/referenceData/domains", client, limit=limit, start=start, filters=filters,
                    extra_params={"sortBy": "modifiedTimeStamp:descending"})
                return {"count": count, "lookup_tables": [
                    {k: it.get(k) for k in ("id", "name", "label", "description",
                                            "createdBy", "modifiedTimeStamp")}
                    for it in items]}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_get_lookup_table",
        description=(
            "Get one SAS lookup table (reference-data domain) by id: its name plus the "
            "key/value entries of its active content. Use sas_list_lookup_tables to find "
            "the id."
        ),
    )
    async def sas_get_lookup_table(domain_id: str, ctx: Context, limit: int = 200) -> Any:
        try:
            async with session("sas_get_lookup_table", ctx) as client:
                try:
                    dom = await get_json(f"/referenceData/domains/{domain_id}", client,
                                         accept=_DOMAIN)
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        return {"status": "not_found", "domain_id": domain_id}
                    raise
                contents, _ = await get_paged_items(
                    f"/referenceData/domains/{domain_id}/contents", client, limit=50)
                # prefer the active content
                active = next((c for c in contents if c.get("activationStatus") == "active"),
                              contents[0] if contents else None)
                entries = []
                if active:
                    items, _ = await get_paged_items(
                        f"/referenceData/domains/{domain_id}/contents/{active['id']}/entries",
                        client, limit=limit)
                    entries = [{"key": it.get("key"), "value": it.get("value")} for it in items]
                return {"id": dom.get("id"), "name": dom.get("name"),
                        "label": dom.get("label"), "description": dom.get("description"),
                        "contentId": active.get("id") if active else None,
                        "entryCount": len(entries), "entries": entries}
        except Exception as e:
            return _err(e)

    @mcp.tool(
        name="sas_create_lookup_table",
        description=(
            "Create a new SAS Intelligent Decisioning lookup table (reference-data domain) "
            "with a set of key/value entries, and activate it so decisions/rules can use "
            "it. `entries` is a list of {key, value} pairs (strings). Builds the domain, "
            "adds an active content version, and loads the entries. Returns the new "
            "domain id."
        ),
    )
    async def sas_create_lookup_table(
        name: str, entries: list[dict], ctx: Context,
        description: str = "", label: str | None = None,
    ) -> Any:
        if not entries:
            return {"status": "invalid", "message": "Provide at least one {key, value} entry."}
        rows = [{"key": str(e["key"]), "value": str(e.get("value", ""))}
                for e in entries if e.get("key") is not None]
        if not rows:
            return {"status": "invalid", "message": "Each entry needs a 'key'."}
        try:
            async with session("sas_create_lookup_table", ctx) as client:
                # 1. create the lookup domain (domainType is required)
                dr = await client.post(
                    f"{VIYA_ENDPOINT}/referenceData/domains",
                    json={"name": name, "domainType": "lookup", "description": description},
                    headers={"Content-Type": _DOMAIN, "Accept": _DOMAIN})
                dr.raise_for_status()
                domain_id = dr.json().get("id")
                # 2. create a content version in an editable ('developing') status
                cr = await client.post(
                    f"{VIYA_ENDPOINT}/referenceData/domains/{domain_id}/contents",
                    json={"label": label or f"{name}1.0", "status": "developing"},
                    headers={"Content-Type": _DOMAIN_CONTENT, "Accept": _DOMAIN_CONTENT})
                cr.raise_for_status()
                content_id = cr.json().get("id")
                # 3. load the key/value entries while the content is editable
                ear = await client.post(
                    f"{VIYA_ENDPOINT}/referenceData/domains/{domain_id}/contents/{content_id}/entries",
                    json=rows, headers={"Content-Type": _COLLECTION, "Accept": _COLLECTION})
                ear.raise_for_status()
                # 4. promote the content to production so the lookup is active/usable
                #    (status-only PUT with If-Match; activationStatus is set by SAS)
                activated = False
                try:
                    cg = await client.get(
                        f"{VIYA_ENDPOINT}/referenceData/domains/{domain_id}/contents/{content_id}",
                        headers={"Accept": _DOMAIN_CONTENT})
                    etag = cg.headers.get("ETag")
                    headers = {"Content-Type": _DOMAIN_CONTENT, "Accept": _DOMAIN_CONTENT}
                    if etag:
                        headers["If-Match"] = etag
                    pr = await client.put(
                        f"{VIYA_ENDPOINT}/referenceData/domains/{domain_id}/contents/{content_id}",
                        json={"status": "production"}, headers=headers)
                    activated = bool(pr.is_success)
                except httpx.HTTPError:
                    pass
                return {"status": "created", "id": domain_id, "name": name,
                        "contentId": content_id, "entryCount": len(rows),
                        "activated": activated,
                        "message": None if activated else
                        "Lookup created with entries but left in 'developing' status "
                        "(activation step did not complete); activate it in SAS if needed."}
        except Exception as e:
            return _err(e)

    # ── Custom REST node types (registered decision nodes) ─────────────────────

    @mcp.tool(
        name="sas_list_decision_node_types",
        description=(
            "List the registered custom decision node types — the custom REST nodes you "
            "can drag into a Build Decisions flow (e.g. 'Segmentation Tree', 'Decision "
            "Rest Node', 'Language Model Query'). Returns each node type's id, name, "
            "description and (for REST nodes) the backing REST object type/uri. Use this "
            "to discover the custom nodes available in this deployment."
        ),
    )
    async def sas_list_decision_node_types(ctx: Context) -> Any:
        try:
            async with session("sas_list_decision_node_types", ctx) as client:
                items, count = await get_paged_items(
                    "/decisions/decisionNodeTypes", client, limit=200)
                out = []
                for it in items:
                    entry = {k: it.get(k) for k in ("id", "name", "description", "type")}
                    # best-effort: include the REST backing info from the type's content
                    try:
                        content = await get_json(
                            f"/decisions/decisionNodeTypes/{it.get('id')}/content", client)
                        entry["restObjectTypeName"] = content.get("restObjectTypeName")
                        entry["restUri"] = content.get("restUri")
                    except httpx.HTTPError:
                        pass
                    out.append(entry)
                return {"count": count, "node_types": out}
        except Exception as e:
            return _err(e)
