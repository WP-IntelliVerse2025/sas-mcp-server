"""
SAS Viya Model Manager (modelRepository) HTTP client.

Thin REST client for the SAS Model Manager service, mirroring the structure and
auth model of ``sas_client.SASViyaClient`` (plain Viya REST, OAuth Bearer token,
no compute session) rather than ``studio_client.SASStudioClient`` (which needs a
stateful compute/Studio session). Model Manager is a sibling of the VA/report
endpoints — same host, same per-user Bearer token — so it follows the VA client
pattern, not the Studio one.

Every endpoint, query parameter, request body shape, and response field used
here was derived strictly from a captured browser session
(``model_importing_data_variables.har``); see the per-method comments citing the
relevant call. Nothing here is invented.

Auth note — CSRF: the browser session sent an ``X-CSRF-Token`` header on every
call, but that token is bound to SAS's *cookie* session. MCP runs as the
logged-in user via an OAuth **Bearer** token (no cookie), for which SAS Viya
does not require CSRF. The existing VA client (``sas_client.py``) POSTs/PUTs the
same way without a CSRF token, so we follow it.
"""
from __future__ import annotations

from typing import Optional

import httpx

# Media types SAS uses for the modelRepository collections / resources. Sending
# the right Accept/Content-Type is what makes POST .../variables accept the
# {"items":[...]} envelope (it 415s on a bare application/json body).
_COLLECTION = "application/vnd.sas.collection+json"
_JSON = "application/json"


class SASModelManagerClient:
    """Thin HTTP client for the SAS Viya Model Manager (modelRepository) API."""

    def __init__(self, base_url: str, access_token: str):
        self.base_url = base_url.rstrip("/")
        self._token = access_token
        # verify=False + follow_redirects=True mirror sas_client.SASViyaClient:
        # the content "stream" endpoint 302-redirects to /files/files, so we must
        # follow redirects to read file bytes.
        self._http = httpx.Client(timeout=120.0, follow_redirects=True, verify=False)

    # ── Auth ──────────────────────────────────────────────────────────────────

    def _hdrs(self, accept: str = _JSON, content_type: Optional[str] = None) -> dict:
        h = {
            "Authorization": f"Bearer {self._token}",
            "Accept": accept,
        }
        if content_type:
            h["Content-Type"] = content_type
        return h

    # ── Repositories ──────────────────────────────────────────────────────────

    def list_repositories(self, limit: int = 1000) -> list:
        """List model repositories.

        HAR: GET /modelRepository/repositories?start=0&limit=1000
        The default repository (``defaultRepository: true``) is where imports land
        unless a folderId is given; its ``folderId`` is the value ``import_model``
        needs as ``folder_id``.
        """
        resp = self._http.get(
            f"{self.base_url}/modelRepository/repositories",
            headers=self._hdrs(_COLLECTION),
            params={"start": 0, "limit": limit},
        )
        resp.raise_for_status()
        return [
            {
                "id": r.get("id"),
                "name": r.get("name"),
                "description": r.get("description", ""),
                "folderId": r.get("folderId"),
                "defaultRepository": r.get("defaultRepository", False),
            }
            for r in resp.json().get("items", [])
        ]

    # ── Models ────────────────────────────────────────────────────────────────

    def list_models(self, limit: int = 100, start: int = 0,
                    name_filter: Optional[str] = None) -> dict:
        """List models in the model repository.

        HAR: GET /modelRepository/models?start=0&limit=N
        (the browser used limit=0 to read just the total ``count``; we default to
        a real page so the items come back). ``name_filter`` maps to SAS's
        ``filter=contains(name,'...')`` query grammar.
        """
        params: dict = {"start": start, "limit": limit}
        if name_filter:
            params["filter"] = f"contains(name,'{name_filter}')"
        resp = self._http.get(
            f"{self.base_url}/modelRepository/models",
            headers=self._hdrs(_COLLECTION),
            params=params,
        )
        resp.raise_for_status()
        body = resp.json()
        return {
            "count": body.get("count", 0),
            "start": body.get("start", start),
            "limit": body.get("limit", limit),
            "items": [self._model_summary(m) for m in body.get("items", [])],
        }

    def get_models_summary(self, group_by: str = "scoreCodeType") -> list:
        """Aggregate model counts grouped by a field.

        HAR: GET /modelRepository/models/summary?groupBy=scoreCodeType
        Returns rows like [{"category": "Python", "count": 7}, ...].
        """
        resp = self._http.get(
            f"{self.base_url}/modelRepository/models/summary",
            headers=self._hdrs(_JSON),
            params={"groupBy": group_by},
        )
        resp.raise_for_status()
        return resp.json()

    def get_model(self, model_id: str) -> dict:
        """Fetch full metadata for one model, including its content ``files``.

        HAR: GET /modelRepository/models/{id}
        The returned ``files`` list gives each file's ``id`` — that id is the
        ``content_id`` ``get_model_content`` needs.
        """
        resp = self._http.get(
            f"{self.base_url}/modelRepository/models/{model_id}",
            headers=self._hdrs(_JSON),
        )
        resp.raise_for_status()
        m = resp.json()
        summary = self._model_summary(m)
        # Surface the content-file index + variable arrays so the LLM can chain
        # straight into get_model_content / list_model_variables.
        summary["files"] = [
            {"id": f.get("id"), "name": f.get("name"), "role": f.get("role", "")}
            for f in m.get("files", [])
        ]
        summary["inputVariables"] = m.get("inputVariables", [])
        summary["outputVariables"] = m.get("outputVariables", [])
        summary["scoreCodeUri"] = m.get("scoreCodeUri")
        summary["location"] = m.get("location")
        return summary

    def get_model_content(self, model_id: str, content_id: str,
                          max_chars: int = 2_000_000) -> dict:
        """Stream the text of one of a model's content files.

        HAR: GET /modelRepository/models/{id}/contents/{contentId}/stream
             ?charNumber=2000000
        The browser used this to preview files like ``package.json`` / score
        code. ``charNumber`` caps how many characters SAS streams back.
        """
        resp = self._http.get(
            f"{self.base_url}/modelRepository/models/{model_id}"
            f"/contents/{content_id}/stream",
            headers=self._hdrs("*/*"),
            params={"charNumber": max_chars},
        )
        resp.raise_for_status()
        return {
            "model_id": model_id,
            "content_id": content_id,
            "content_type": resp.headers.get("Content-Type", ""),
            "text": resp.text,
        }

    def import_model(self, file_bytes: bytes, name: str, folder_id: str,
                     model_type: str = "GENERIC",
                     filename: Optional[str] = None) -> dict:
        """Import a model from a packaged archive (.zip) into a repository folder.

        HAR: POST /modelRepository/models
             ?fileName={name}&folderId={folderId}&name={name}&type=GENERIC
             multipart body: _charset_=UTF-8, files-data=(empty),
             files=<name>.zip (Content-Type application/zip)
        ``folder_id`` is the target repository's ``folderId`` (see
        ``list_repositories``). Returns the created model summary (SAS wraps the
        new model in a 1-item collection).
        """
        fname = filename or f"{name}.zip"
        # Exact multipart field set the browser sent (entry 10). httpx encodes a
        # (None, value) tuple as a plain form field and (filename, bytes, type) as
        # a file part — reproducing the WebKitFormBoundary body byte-for-byte.
        fields = {
            "_charset_": (None, "UTF-8"),
            "files-data": (None, ""),
            "files": (fname, file_bytes, "application/zip"),
        }
        resp = self._http.post(
            f"{self.base_url}/modelRepository/models",
            headers=self._hdrs(_JSON),
            params={"fileName": name, "folderId": folder_id,
                    "name": name, "type": model_type},
            files=fields,
            timeout=600.0,
        )
        if not resp.is_success:
            body = resp.text[:600] if resp.content else "(empty body)"
            raise RuntimeError(f"Model import failed HTTP {resp.status_code}: {body}")
        data = resp.json()
        # POST .../models returns a collection {count:1, items:[{model}]}.
        items = data.get("items") or []
        return self._model_summary(items[0]) if items else data

    # ── Model variables ───────────────────────────────────────────────────────

    def list_model_variables(self, model_id: str, limit: int = 10000) -> list:
        """List a model's input/output variables.

        HAR: GET /modelRepository/models/{id}/variables?start=0&limit=10000
        """
        resp = self._http.get(
            f"{self.base_url}/modelRepository/models/{model_id}/variables",
            headers=self._hdrs(_COLLECTION),
            params={"start": 0, "limit": limit},
        )
        resp.raise_for_status()
        return [self._variable_summary(v) for v in resp.json().get("items", [])]

    def add_model_variables(self, model_id: str, variables: list) -> dict:
        """Add input/output variables to a model.

        HAR: POST /modelRepository/models/{id}/variables
             Content-Type: application/vnd.sas.collection+json
             body: {"items": [{"name","type","description","length","role"}, ...]}
        ``type`` is SAS's logical type — "string" or "decimal"; ``role`` is
        "input" or "output"; ``length`` is the variable length. Returns the
        created variable collection.
        """
        # Normalise each item to the exact field set the captured POST sent, so a
        # caller can pass a looser dict (e.g. omit description) and still match.
        items = []
        for v in variables:
            vtype = v.get("type", "string")
            items.append({
                "name": v["name"],
                "type": vtype,
                "description": v.get("description", vtype),
                "length": int(v.get("length", 8)),
                "role": v.get("role", "input"),
            })
        resp = self._http.post(
            f"{self.base_url}/modelRepository/models/{model_id}/variables",
            headers=self._hdrs(_JSON, content_type=_COLLECTION),
            json={"items": items},
        )
        if not resp.is_success:
            body = resp.text[:600] if resp.content else "(empty body)"
            raise RuntimeError(
                f"Add variables failed HTTP {resp.status_code}: {body}"
            )
        body = resp.json()
        return {
            "model_id": model_id,
            "added": body.get("count", len(items)),
            "items": [self._variable_summary(v) for v in body.get("items", [])],
        }

    def add_variables_from_cas_table(
        self, model_id: str, caslib: str, table: str,
        server: str = "cas-shared-default",
        output_columns: Optional[list] = None,
        numeric_length: int = 12,
    ) -> dict:
        """Convenience: derive model variables from a CAS table's columns and add
        them, reproducing the captured "import model data variables" flow.

        This is the end-to-end action the HAR captured: read S01OTH's 32 columns
        (entry 67) and POST 32 model variables (entry 74). The CAS-type → model
        -type mapping below was reverse-engineered from that pair of calls:

          * CAS ``varchar`` / ``char``  -> model ``string``, length = the column's
            CAS ``rawLength`` (e.g. Demand_Class varchar(25) -> string length 25).
          * CAS numeric (``double``/int) -> model ``decimal``. The captured UI used
            a FIXED length of 12 for every numeric variable — NOT the CAS storage
            length of 8 — so ``numeric_length`` defaults to 12 to match. (SAS
            numerics are 8 bytes on disk; the 12 is a Model Manager display
            default. Overridable.)
          * Every variable gets role "input" except names listed in
            ``output_columns`` (e.g. the target ``KPI_Billing``), which get role
            "output" — exactly as the capture marked its target variable.
        """
        out = {c.lower() for c in (output_columns or [])}
        cols = self.get_cas_table_columns(caslib, table, server)
        variables = []
        for col in cols:
            ctype = (col.get("type") or "").lower()
            if ctype in ("varchar", "char", "string", "nchar", "nvarchar"):
                vtype, length = "string", int(col.get("rawLength") or col.get("length") or 8)
            else:
                # double / int32 / int64 / decimal / date / datetime / time → decimal
                vtype, length = "decimal", numeric_length
            role = "output" if col["name"].lower() in out else "input"
            variables.append({
                "name": col["name"], "type": vtype, "description": vtype,
                "length": length, "role": role,
            })
        result = self.add_model_variables(model_id, variables)
        result["source_table"] = f"{caslib}.{table}"
        result["mapped_from_columns"] = len(variables)
        return result

    # ── Projects (the unit the SAS Model Manager "Projects" view shows) ────────
    #
    # WHY THIS EXISTS: a model created with import_model lands loose under the
    # repository folder and is therefore NOT visible in the Model Manager
    # "Projects" screen — that screen only lists models that belong to a project
    # *version*. To make a model appear there you (1) create a project, then
    # (2) copy/transfer the model into that project's version. Both steps below
    # are taken verbatim from new_model_creation.har (entries 113 + 239).

    def list_projects(self, limit: int = 100, start: int = 0,
                      name_filter: Optional[str] = None) -> dict:
        """List Model Manager projects.

        HAR: GET /modelRepository/projects?start=0&limit=N
        """
        params: dict = {"start": start, "limit": limit}
        if name_filter:
            params["filter"] = f"contains(name,'{name_filter}')"
        resp = self._http.get(
            f"{self.base_url}/modelRepository/projects",
            headers=self._hdrs(_COLLECTION),
            params=params,
        )
        resp.raise_for_status()
        body = resp.json()
        return {
            "count": body.get("count", 0),
            "items": [self._project_summary(p) for p in body.get("items", [])],
        }

    def get_projects_summary(self, group_by: str = "status") -> list:
        """Aggregate project counts grouped by a field.

        HAR: GET /modelRepository/projects/summary?groupBy=status   (or function)
        """
        resp = self._http.get(
            f"{self.base_url}/modelRepository/projects/summary",
            headers=self._hdrs(_JSON),
            params={"groupBy": group_by},
        )
        resp.raise_for_status()
        return resp.json()

    def get_project(self, project_id: str) -> dict:
        """Fetch one project plus its versions.

        HAR: GET /modelRepository/projects/{id}
             GET /modelRepository/projects/{id}/projectVersions
        """
        resp = self._http.get(
            f"{self.base_url}/modelRepository/projects/{project_id}",
            headers=self._hdrs(_JSON),
        )
        resp.raise_for_status()
        out = self._project_summary(resp.json())
        out["versions"] = self.list_project_versions(project_id)
        return out

    def list_project_versions(self, project_id: str) -> list:
        """List a project's versions (each has the projectVersionId a transfer
        needs).

        HAR: GET /modelRepository/projects/{id}/projectVersions
        """
        resp = self._http.get(
            f"{self.base_url}/modelRepository/projects/{project_id}/projectVersions",
            headers=self._hdrs(_COLLECTION),
            params={"start": 0, "limit": 1000,
                    "sortBy": "modifiedTimeStamp:ascending"},
        )
        resp.raise_for_status()
        return [
            {
                "id": v.get("id"),
                "name": v.get("name"),
                "versionNumber": v.get("versionNumber"),
                "projectId": v.get("projectId"),
            }
            for v in resp.json().get("items", [])
        ]

    def _latest_project_version_id(self, project_id: str) -> str:
        """Resolve a project's newest version id (the one a fresh transfer targets)."""
        versions = self.list_project_versions(project_id)
        if not versions:
            raise RuntimeError(f"Project {project_id} has no versions to transfer into.")

        def _num(v):
            try:
                return float(v.get("versionNumber") or 0)
            except (TypeError, ValueError):
                return 0.0
        # Highest versionNumber wins; the list is modified-ascending so the last
        # item is the tie-breaker fallback.
        return max(versions, key=_num).get("id") or versions[-1]["id"]

    def list_project_models(self, project_id: str,
                            project_version_id: Optional[str] = None) -> dict:
        """List the models inside a project (version) — i.e. exactly what the
        Projects view shows. Use this to VERIFY a model landed in the project.

        HAR: GET /modelRepository/projects/{id}/projectVersions/{vid}/models
        """
        if not project_version_id:
            project_version_id = self._latest_project_version_id(project_id)
        resp = self._http.get(
            f"{self.base_url}/modelRepository/projects/{project_id}"
            f"/projectVersions/{project_version_id}/models",
            headers=self._hdrs(_JSON),
            params={"start": 0, "limit": 1000},
        )
        resp.raise_for_status()
        body = resp.json()
        return {
            "project_id": project_id,
            "project_version_id": project_version_id,
            "count": body.get("count", 0),
            "items": [self._model_summary(m) for m in body.get("items", [])],
        }

    def _default_repository(self) -> dict:
        repos = self.list_repositories()
        if not repos:
            raise RuntimeError("No model repositories found.")
        for r in repos:
            if r.get("defaultRepository"):
                return r
        return repos[0]

    @staticmethod
    def _project_variables(variables: Optional[list]) -> list:
        """Normalise loose variable dicts to the project-create variable shape.

        The project endpoint (unlike the model-variable endpoint) wants an
        UPPERCASE logical type and a STRING length — e.g.
        {"name","type":"DECIMAL","length":"12","description","role"} — exactly as
        new_model_creation.har entry 113 sent. We accept the same loose dicts as
        add_model_variables and convert.
        """
        out = []
        for v in variables or []:
            t = (v.get("type") or "string").lower()
            ptype = "STRING" if t in ("string", "varchar", "char", "nchar", "nvarchar") else "DECIMAL"
            out.append({
                "name": v["name"],
                "type": ptype,
                "length": str(v.get("length", 12)),
                "description": v.get("description", ptype.lower()),
                "role": v.get("role", "input"),
            })
        return out

    def create_project(
        self, name: str, repository_id: Optional[str] = None,
        folder_id: Optional[str] = None, function: str = "prediction",
        description: str = "", status: str = "prototype",
        train_table: Optional[str] = None, variables: Optional[list] = None,
        target_variable: str = "", segmentation_variable: str = "",
        prediction_variable: str = "", event_probability_variable: str = "",
    ) -> dict:
        """Create a Model Manager project — the container the Projects view shows.

        HAR: POST /modelRepository/projects
             Content-Type: application/vnd.sas.models.project+json
        ``function`` is the project's model function (e.g. prediction,
        classification, clustering, "text analytics"). repository_id/folder_id
        default to the default repository. Creating the project auto-creates
        "Version 1". The full empty-string field set below mirrors the captured
        request (SAS defaults them, but sending them is proven to 201).
        """
        if not (repository_id and folder_id):
            repo = self._default_repository()
            repository_id = repository_id or repo["id"]
            folder_id = folder_id or repo["folderId"]
        body = {
            "name": name,
            "description": description or "",
            "folderId": folder_id,
            "repositoryId": repository_id,
            "status": status or "prototype",
            "targetEventValue": "",
            "selectionStatistic": "",
            "selectionOperator": "",
            "selectionThreshold": "",
            "eventProbabilityVariable": event_probability_variable or "",
            "variables": self._project_variables(variables),
            "predictionVariable": prediction_variable or "",
            "segmentationVariable": segmentation_variable or "",
            "textCategoriesVariable": "",
            "textConceptsVariable": "",
            "textSentimentVariable": "",
            "textTopicsVariable": "",
            "properties": [],
            "tags": [],
            "trainTable": train_table or "",
            "targetVariable": target_variable or "",
            "function": function or "prediction",
        }
        resp = self._http.post(
            f"{self.base_url}/modelRepository/projects",
            headers=self._hdrs(_JSON, content_type="application/vnd.sas.models.project+json"),
            json=body,
        )
        if not resp.is_success:
            err = resp.text[:600] if resp.content else "(empty body)"
            raise RuntimeError(f"Create project failed HTTP {resp.status_code}: {err}")
        return self._project_summary(resp.json())

    def copy_model_to_project(
        self, source_model_id: str, project_id: str,
        project_version_id: Optional[str] = None, op_code: str = "copy",
    ) -> dict:
        """Copy (or move) an existing repository model INTO a project version —
        the step that makes a model appear under Projects.

        HAR: POST /modelRepository/models/transfer
             body: {"destinationType":"project",
                    "destinationUri":"/modelRepository/projects/{pid}/projectVersions/{vid}",
                    "opCode":"copy", "sourceType":"model",
                    "sourceUri":"/modelRepository/models/{sourceId}"}
        The returned model carries projectId + projectVersionId. opCode "copy"
        leaves the source model in place; "move" relocates it.
        """
        if not project_version_id:
            project_version_id = self._latest_project_version_id(project_id)
        body = {
            "destinationType": "project",
            "destinationUri": (
                f"/modelRepository/projects/{project_id}"
                f"/projectVersions/{project_version_id}"
            ),
            "opCode": op_code,
            "sourceType": "model",
            "sourceUri": f"/modelRepository/models/{source_model_id}",
        }
        resp = self._http.post(
            f"{self.base_url}/modelRepository/models/transfer",
            headers=self._hdrs(_JSON, content_type=_JSON),
            json=body,
        )
        if not resp.is_success:
            err = resp.text[:600] if resp.content else "(empty body)"
            raise RuntimeError(f"Transfer model failed HTTP {resp.status_code}: {err}")
        m = resp.json()
        out = self._model_summary(m)
        out["projectId"] = m.get("projectId")
        out["projectVersionId"] = m.get("projectVersionId")
        out["copiedFrom"] = m.get("copiedFrom")
        return out

    @staticmethod
    def _parse_train_table(train_table: Optional[str]):
        """'cas-shared-default/Public/S01OTH' -> ('cas-shared-default','Public','S01OTH').

        Accepts 'server/caslib/table' or 'caslib/table' (server defaults). Returns
        (None, None, None) if it can't parse a caslib + table.
        """
        parts = [p for p in (train_table or "").split("/") if p]
        if len(parts) >= 3:
            return parts[0], parts[1], parts[2]
        if len(parts) == 2:
            return "cas-shared-default", parts[0], parts[1]
        return None, None, None

    def create_project_with_model(
        self, name: str, source_model_id: str, function: str = "prediction",
        repository_id: Optional[str] = None, folder_id: Optional[str] = None,
        op_code: str = "copy", train_table: Optional[str] = None,
        target_variable: str = "", populate_variables: bool = True,
        **project_kwargs,
    ) -> dict:
        """One-shot: create a project, copy a model into it, and (by default)
        populate that model's variables from the training table so its Variables
        tab isn't empty — leaving it immediately visible under Projects.

        Chains: create_project (HAR entry 113) → resolve the new project's version
        → copy_model_to_project (entry 239) → add the train table's columns as the
        copied model's variables (the model_importing_data_variables.har flow),
        with ``target_variable`` marked as the output. ``source_model_id`` is an
        existing repository model (from list_models).

        Variable population is SKIPPED when the copied model already has its own
        variables (so a real model's schema is never clobbered) or when no
        train_table is given. Failures here are non-fatal — the project + model
        are already created; we just attach a warning.
        """
        project = self.create_project(
            name, repository_id=repository_id, folder_id=folder_id,
            function=function, train_table=train_table,
            target_variable=target_variable, **project_kwargs,
        )
        version_id = self._latest_project_version_id(project["id"])
        model = self.copy_model_to_project(
            source_model_id, project["id"], version_id, op_code=op_code)
        result = {
            "status": "model saved to project",
            "project": project,
            "project_version_id": version_id,
            "model": model,
            "view_in": f"{self.base_url}/SASModelManager/",
        }

        if populate_variables and train_table:
            copied_id = model.get("id")
            server, caslib, table = self._parse_train_table(train_table)
            try:
                existing = self.list_model_variables(copied_id) if copied_id else []
                if copied_id and caslib and table and not existing:
                    out_cols = [target_variable] if target_variable else None
                    var_res = self.add_variables_from_cas_table(
                        copied_id, caslib, table,
                        server=server or "cas-shared-default",
                        output_columns=out_cols)
                    result["variables_added"] = var_res.get("added", 0)
                    result["model"]["variableCount"] = var_res.get("added", 0)
                elif existing:
                    result["variables_note"] = (
                        "model already had variables; left its schema untouched"
                    )
            except Exception as e:
                result["variables_warning"] = (
                    f"Project + model created, but could not populate model "
                    f"variables from {train_table}: {e}"
                )
        return result

    # ── CAS columns (read-only helper for the variable-mapping flow) ───────────

    def get_cas_table_columns(self, caslib: str, table: str,
                              server: str = "cas-shared-default") -> list:
        """Read a CAS table's columns *with* their raw lengths, loading the table
        into CAS memory first if it isn't already.

        HAR: GET /casManagement/servers/{server}/caslibs/{caslib}/tables/{table}
             /columns?start=0&limit=2147483647
        We read it here (rather than reuse ``sas_client.get_table_columns``)
        because that helper drops ``rawLength``, which we need to set
        string-variable lengths faithfully.

        IMPORTANT: casManagement's ``/columns`` endpoint returns **404 for an
        *unloaded* table** (one that exists on disk but is not in CAS memory).
        Most Model Manager training tables sit unloaded, so a plain read came
        back 404 and variable population silently produced nothing. We therefore
        promote the table with ``PUT .../state?value=loaded`` on a 404 and retry
        (verified: load → 200, columns then return the full schema).
        """
        base_tbl = (f"{self.base_url}/casManagement/servers/{server}"
                    f"/caslibs/{caslib}/tables/{table}")

        def _cols():
            return self._http.get(
                base_tbl + "/columns", headers=self._hdrs(_COLLECTION),
                params={"start": 0, "limit": 2147483647})

        resp = _cols()
        if resp.status_code == 404:
            # Load the table into CAS memory, then retry the columns read.
            self._http.put(base_tbl + "/state", headers=self._hdrs(_JSON),
                           params={"value": "loaded"})
            resp = _cols()
        resp.raise_for_status()
        return [
            {
                "name": col["name"],
                "type": col.get("type", ""),
                "rawLength": col.get("rawLength"),
                "index": col.get("index"),
            }
            for col in resp.json().get("items", [])
        ]

    # ── Shapers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _model_summary(m: dict) -> dict:
        """Trim a raw model object down to the fields worth showing an LLM."""
        return {
            "id": m.get("id"),
            "name": m.get("name"),
            "role": m.get("role"),
            "scoreCodeType": m.get("scoreCodeType"),
            "function": m.get("function"),
            "algorithm": m.get("algorithm"),
            "modeler": m.get("modeler"),
            "repositoryId": m.get("repositoryId"),
            "folderId": m.get("folderId"),
            "modelVersionName": m.get("modelVersionName"),
            "createdBy": m.get("createdBy"),
            "modifiedTimeStamp": m.get("modifiedTimeStamp"),
        }

    @staticmethod
    def _project_summary(p: dict) -> dict:
        """Trim a raw project object to the fields worth showing an LLM."""
        return {
            "id": p.get("id"),
            "name": p.get("name"),
            "function": p.get("function"),
            "status": p.get("status"),
            "latestVersion": p.get("latestVersion"),
            "trainTable": p.get("trainTable"),
            "repositoryId": p.get("repositoryId"),
            "folderId": p.get("folderId"),
            "location": p.get("location"),
            "createdBy": p.get("createdBy"),
            "modifiedTimeStamp": p.get("modifiedTimeStamp"),
        }

    @staticmethod
    def _variable_summary(v: dict) -> dict:
        return {
            "id": v.get("id"),
            "name": v.get("name"),
            "type": v.get("type"),
            "role": v.get("role"),
            "length": v.get("length"),
            "level": v.get("level", ""),
            "format": v.get("format", ""),
        }

    def close(self) -> None:
        self._http.close()
