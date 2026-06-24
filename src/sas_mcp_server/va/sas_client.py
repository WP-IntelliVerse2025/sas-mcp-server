import base64
import os
import tempfile
import time
from typing import Optional

import httpx


def _svg_to_png(svg_bytes: bytes, width: int = 900, height: int = 600) -> bytes:
    """Convert SVG bytes to PNG bytes using Edge headless via Selenium."""
    from selenium import webdriver
    from selenium.webdriver.edge.options import Options

    svg_text = svg_bytes.decode("utf-8", errors="replace")
    html = (
        "<!DOCTYPE html><html><body style='margin:0;padding:0;background:white;'>"
        + svg_text
        + "</body></html>"
    )
    with tempfile.NamedTemporaryFile(suffix=".html", delete=False, mode="w",
                                     encoding="utf-8") as tmp:
        tmp.write(html)
        tmp_path = tmp.name

    opts = Options()
    opts.add_argument("--headless")
    opts.add_argument("--disable-gpu")
    # Add extra height for browser chrome so the full SVG is captured
    opts.add_argument(f"--window-size={width},{height + 150}")
    driver = webdriver.Edge(options=opts)
    try:
        driver.get("file:///" + tmp_path.replace("\\", "/"))
        time.sleep(0.8)
        # Crop to exact SVG dimensions using Pillow
        import io
        from PIL import Image
        raw = driver.get_screenshot_as_png()
        img = Image.open(io.BytesIO(raw)).crop((0, 0, width, height))
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()
    finally:
        driver.quit()
        os.remove(tmp_path)


class SASViyaClient:
    def __init__(
        self,
        base_url: str,
        username: str = "",
        password: str = "",
        client_id: str = "sas.ec",
        client_secret: str = "",
        access_token: str = "",
    ):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.client_id = client_id
        self.client_secret = client_secret
        self._access_token = access_token
        self._http = httpx.Client(timeout=60.0, follow_redirects=True, verify=False)

    def _get_token(self) -> str:
        if self._access_token:
            return self._access_token
        cred = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
        resp = self._http.post(
            f"{self.base_url}/SASLogon/oauth/token",
            headers={
                "Authorization": f"Basic {cred}",
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
            data={
                "grant_type": "password",
                "username": self.username,
                "password": self.password,
            },
        )
        resp.raise_for_status()
        self._access_token = resp.json()["access_token"]
        return self._access_token

    def _hdrs(self, extra: Optional[dict] = None) -> dict:
        h = {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept": "application/json",
        }
        if extra:
            h.update(extra)
        return h

    # ── Auth ────────────────────────────────────────────────────────────────

    def authenticate(self) -> dict:
        token = self._get_token()
        return {"status": "authenticated", "token_preview": f"{token[:20]}..."}

    # ── CAS ─────────────────────────────────────────────────────────────────

    def list_cas_libraries(self, server: str = "cas-shared-default") -> list:
        resp = self._http.get(
            f"{self.base_url}/casManagement/servers/{server}/caslibs",
            headers=self._hdrs(),
            params={"limit": 1000},
        )
        resp.raise_for_status()
        return [
            {
                "name": lib["name"],
                "description": lib.get("description", ""),
                "path": lib.get("path", ""),
            }
            for lib in resp.json().get("items", [])
        ]

    def list_cas_tables(self, library: str, server: str = "cas-shared-default") -> list:
        resp = self._http.get(
            f"{self.base_url}/casManagement/servers/{server}/caslibs/{library}/tables",
            headers=self._hdrs(),
            params={"limit": 1000},
        )
        resp.raise_for_status()
        return [
            {
                "name": tbl["name"],
                "label": tbl.get("label", ""),
                "rowCount": tbl.get("rowCount", 0),
            }
            for tbl in resp.json().get("items", [])
        ]

    def get_table_columns(
        self, library: str, table: str, server: str = "cas-shared-default"
    ) -> list:
        resp = self._http.get(
            f"{self.base_url}/casManagement/servers/{server}/caslibs/{library}/tables/{table}/columns",
            headers=self._hdrs(),
            params={"limit": 2000},
        )
        resp.raise_for_status()
        raw_items = resp.json().get("items", [])
        columns = []
        for col in raw_items:
            name = col["name"]
            # Some CAS tables store all column names as one comma-separated value
            if "," in name and len(name) > 64:
                for col_name in name.split(","):
                    col_name = col_name.strip()
                    if col_name:
                        columns.append({"name": col_name, "label": col_name, "type": col.get("type", "varchar"), "format": ""})
            else:
                columns.append({
                    "name": name,
                    "label": col.get("label", name),
                    "type": col.get("type", ""),
                    "format": col.get("format", ""),
                })
        return columns

    # ── Data upload ──────────────────────────────────────────────────────────

    @staticmethod
    def _sanitize_table_name(name: str) -> str:
        """CAS table name: letters/digits/underscore only, must not start with a digit."""
        import re
        cleaned = re.sub(r"[^A-Za-z0-9_]", "_", (name or "").strip()) or "UPLOAD"
        if cleaned[0].isdigit():
            cleaned = "T_" + cleaned
        return cleaned[:60]

    def upload_table(
        self,
        file_bytes: bytes,
        table_name: str,
        caslib: str = "Public",
        server: str = "cas-shared-default",
        filename: Optional[str] = None,
        file_type: str = "DelimitedFile",
        save_type: str = "csv",
        delimiter: str = ",",
        encoding: str = "UTF-8",
        contains_header_row: bool = True,
        scope: str = "global",
        persist: bool = True,
    ) -> dict:
        """Upload a delimited/Excel file into a CAS library as a table.

        Uses the casManagement ``upload`` endpoint
        (``POST .../caslibs/{caslib}/tables``) which streams a multipart body —
        bearer-token auth, no CSRF, and handles large files (tens of MB) fine.
        Global-scope tables can't be replaced in place, so on a name collision
        (HTTP 409) we auto-uniquify the name (``foo`` → ``foo_2`` → …), mirroring
        ``create_report``. Returns the canonical table name + a column summary.
        """
        safe = self._sanitize_table_name(table_name)
        fname = filename or f"{safe}.csv"
        url = f"{self.base_url}/casManagement/servers/{server}/caslibs/{caslib}/tables"

        def _post(tn: str, enc: str):
            fields = {
                "tableName": (None, tn),
                "fileType": (None, file_type),
                "saveType": (None, save_type),
                "scope": (None, scope),
                "persist": (None, "true" if persist else "false"),
                "replace": (None, "false"),
                "encoding": (None, enc),
                "delimiter": (None, delimiter),
                "containsHeaderRow": (None, "true" if contains_header_row else "false"),
                "varchars": (None, "true"),
                "file": (fname, file_bytes, "application/octet-stream"),
            }
            return self._http.post(
                url,
                headers=self._hdrs({"Accept": "application/vnd.sas.cas.table+json"}),
                files=fields,
                timeout=600.0,
            )

        # `latin1` maps every byte 0-255, so it loads files SAS can't auto-detect
        # as UTF-8 — we fall back to it when the server reports an unknown encoding.
        resp = None
        try_name = safe
        enc = encoding
        tried_latin1 = False
        for attempt in range(8):
            try_name = safe if attempt == 0 else f"{safe}_{attempt + 1}"
            resp = _post(try_name, enc)
            if resp.status_code == 409:
                body = resp.text or ""
                if "ENCODING" in body.upper() and not tried_latin1:
                    enc = "latin1"
                    tried_latin1 = True
                    resp = _post(try_name, enc)        # same name, looser encoding
                    if resp.is_success:
                        break
                if resp.status_code == 409:
                    continue                            # name taken — next suffix
            break
        if resp is None or not resp.is_success:
            body = resp.text[:600] if resp is not None else "(no response)"
            code = getattr(resp, "status_code", "?")
            raise RuntimeError(f"CAS upload failed HTTP {code}: {body}")

        data = resp.json()
        final_name = data.get("name", try_name)
        # The create response carries columnCount but not the column list; fetch it
        # so the LLM can confirm the load and immediately reason about columns.
        try:
            cols = [col["name"] for col in
                    self.get_table_columns(caslib, final_name, server)]
        except Exception:
            cols = []
        return {
            "status": "loaded",
            "table": final_name,
            "caslib": data.get("caslibName", caslib),
            "server": server,
            "rowCount": data.get("rowCount"),
            "columnCount": data.get("columnCount", len(cols)),
            "columns": cols[:80],
            "url": f"{self.base_url}/SASDataExplorer/",
        }

    # ── Folders ──────────────────────────────────────────────────────────────

    def get_my_folder_uri(self) -> str:
        resp = self._http.get(
            f"{self.base_url}/folders/folders/@myFolder",
            headers=self._hdrs(),
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("uri", f"/folders/folders/{data['id']}")

    def list_folders(self, folder_uri: str = "@myFolder") -> list:
        path = (
            f"{self.base_url}/folders/folders/{folder_uri}/members"
            if not folder_uri.startswith("/")
            else f"{self.base_url}{folder_uri}/members"
        )
        resp = self._http.get(path, headers=self._hdrs(), params={"limit": 200})
        resp.raise_for_status()
        return [
            {
                "name": item["name"],
                "type": item.get("contentType", ""),
                "uri": item.get("uri", ""),
                "id": item.get("id", ""),
            }
            for item in resp.json().get("items", [])
        ]

    # ── Reports ──────────────────────────────────────────────────────────────

    def create_report(self, name: str, parent_folder_uri: str) -> dict:
        """Create a report, auto-uniquifying the name on conflict.

        SAS enforces unique report names within a folder, so a repeat name (or an
        orphan left by a half-failed earlier attempt) returns HTTP 409. Rather than
        fail the whole request, retry with ``name (2)``, ``name (3)``, … and finally
        a timestamped name. The returned dict carries the name actually used.
        """
        def _post(nm: str):
            return self._http.post(
                f"{self.base_url}/reports/reports",
                headers=self._hdrs({"Content-Type": "application/json"}),
                params={"parentFolderUri": parent_folder_uri},
                json={"name": nm},
            )

        for attempt in range(12):
            try_name = name if attempt == 0 else f"{name} ({attempt + 1})"
            resp = _post(try_name)
            if resp.status_code == 409:
                continue                      # name taken — try the next suffix
            resp.raise_for_status()
            return resp.json()
        # Every suffix collided — fall back to a guaranteed-unique timestamped name.
        resp = _post(f"{name} {int(time.time())}")
        resp.raise_for_status()
        return resp.json()

    def update_report_content(self, report_id: str, content: dict) -> dict:
        # Updating an EXISTING report's content requires an If-Match (ETag) — SAS
        # returns 428 without it. A freshly-created report has no content yet, so
        # the GET 404s and we PUT without If-Match (the create path).
        headers = self._hdrs({"Content-Type": "application/vnd.sas.report.content+json"})
        try:
            g = self._http.get(
                f"{self.base_url}/reports/reports/{report_id}/content",
                headers=self._hdrs({"Accept": "application/vnd.sas.report.content+json"}),
            )
            if g.is_success and g.headers.get("ETag"):
                headers["If-Match"] = g.headers["ETag"]
        except Exception:
            pass
        resp = self._http.put(
            f"{self.base_url}/reports/reports/{report_id}/content",
            headers=headers,
            json=content,
        )
        if not resp.is_success:
            body = resp.text[:2000] if resp.content else "(empty body)"
            raise RuntimeError(
                f"Update content failed HTTP {resp.status_code}: {body}"
            )
        # 201 returns JSON body; 204 returns empty body — both are success
        return resp.json() if resp.content else {"status": "updated", "report_id": report_id}

    def list_reports(self, limit: int = 50) -> list:
        resp = self._http.get(
            f"{self.base_url}/reports/reports",
            headers=self._hdrs(),
            params={"limit": limit, "sortBy": "modifiedTimeStamp:descending"},
        )
        resp.raise_for_status()
        return [
            {
                "id": r.get("id", ""),
                "name": r.get("name", ""),
                "createdBy": r.get("createdBy", ""),
                "modifiedTimeStamp": r.get("modifiedTimeStamp", ""),
            }
            for r in resp.json().get("items", [])
        ]

    def get_report(self, report_id: str) -> dict:
        resp = self._http.get(
            f"{self.base_url}/reports/reports/{report_id}",
            headers=self._hdrs(),
        )
        resp.raise_for_status()
        r = resp.json()
        return {
            "id": r.get("id"),
            "name": r.get("name"),
            "createdBy": r.get("createdBy"),
            "creationTimeStamp": r.get("creationTimeStamp"),
            "modifiedTimeStamp": r.get("modifiedTimeStamp"),
            "url": (
                f"{self.base_url}/SASVisualAnalytics/"
                f"?reportUri=/reports/reports/{r.get('id')}&page=vi1"
            ),
        }

    def get_report_content(self, report_id: str) -> dict:
        """Fetch the BIRD content JSON of a report (the visual model)."""
        resp = self._http.get(
            f"{self.base_url}/reports/reports/{report_id}/content",
            headers=self._hdrs({"Accept": "application/vnd.sas.report.content+json"}),
        )
        resp.raise_for_status()
        return resp.json()

    def rename_report(self, report_id: str, new_name: str) -> dict:
        """Rename a report (GET object + ETag, then PUT with If-Match)."""
        g = self._http.get(f"{self.base_url}/reports/reports/{report_id}",
                           headers=self._hdrs({"Accept": "application/vnd.sas.report+json"}))
        g.raise_for_status()
        obj = g.json()
        obj["name"] = new_name
        headers = self._hdrs({"Content-Type": "application/vnd.sas.report+json",
                              "Accept": "application/vnd.sas.report+json"})
        etag = g.headers.get("ETag")
        if etag:
            headers["If-Match"] = etag
        p = self._http.put(f"{self.base_url}/reports/reports/{report_id}",
                           headers=headers, json=obj)
        if not p.is_success:
            raise RuntimeError(f"Rename failed HTTP {p.status_code}: {p.text[:300]}")
        return p.json() if p.content else {"id": report_id, "name": new_name}

    def move_report(self, report_id: str, parent_folder_uri: str) -> dict:
        """Move a report into another folder by adding it as a member there."""
        report_uri = f"/reports/reports/{report_id}"
        # Resolve the folder id from a folder uri like /folders/folders/<id>.
        fid = parent_folder_uri.rstrip("/").split("/")[-1]
        m = self._http.post(
            f"{self.base_url}/folders/folders/{fid}/members",
            headers=self._hdrs({"Content-Type": "application/vnd.sas.content.folder.member+json"}),
            json={"uri": report_uri, "type": "CHILD",
                  "name": self.get_report(report_id)["name"], "contentType": "report"},
        )
        if not m.is_success:
            raise RuntimeError(f"Move failed HTTP {m.status_code}: {m.text[:300]}")
        return {"id": report_id, "moved_to": parent_folder_uri}

    def delete_report(self, report_id: str) -> bool:
        resp = self._http.delete(
            f"{self.base_url}/reports/reports/{report_id}",
            headers=self._hdrs(),
        )
        resp.raise_for_status()
        return True

    def get_report_image(
        self,
        report_id: str,
        width: int = 900,
        height: int = 600,
        max_wait_seconds: int = 40,
    ) -> bytes:
        """
        Render a VA report page via /reportImages/jobs, convert SVG→PNG, return PNG bytes.
        PNG is required because Claude Desktop cannot display SVG via ImageContent.
        """
        size_str = f"{width}x{height}"

        # Submit render job
        job_resp = self._http.post(
            f"{self.base_url}/reportImages/jobs",
            headers=self._hdrs({"Content-Type": "application/json"}),
            json={
                "reportUri": f"/reports/reports/{report_id}",
                "layoutType": "entireSection",
                "selectionFilters": [],
                "size": size_str,
            },
        )
        job_resp.raise_for_status()
        job_data = job_resp.json()
        job_id = job_data["id"]

        # Poll until completed or failed
        deadline = time.time() + max_wait_seconds
        while time.time() < deadline:
            time.sleep(2)
            poll = self._http.get(
                f"{self.base_url}/reportImages/jobs/{job_id}",
                headers=self._hdrs(),
            )
            poll.raise_for_status()
            job = poll.json()
            state = job.get("state")

            if state == "completed":
                images = job.get("images") or []
                if images:
                    img_href = next(
                        (lnk["href"] for lnk in images[0].get("links", [])
                         if lnk.get("rel") == "image"),
                        None,
                    )
                    if img_href:
                        img = self._http.get(
                            f"{self.base_url}{img_href}",
                            headers={**self._hdrs(), "Accept": "image/svg+xml"},
                        )
                        img.raise_for_status()
                        return _svg_to_png(img.content, width, height)
                raise RuntimeError("Render completed but no image link found.")

            if state in ("failed", "error"):
                err = job.get("error", {})
                raise RuntimeError(
                    f"Report image render failed: {err.get('message', 'unknown error')}"
                )

        raise TimeoutError(
            f"Report image render did not complete within {max_wait_seconds}s."
        )

    def close(self) -> None:
        self._http.close()
