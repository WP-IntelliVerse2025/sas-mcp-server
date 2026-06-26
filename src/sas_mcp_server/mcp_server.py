#!/usr/bin/env python3
# Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""
Starter MCP Server for SAS Viya, utilizing the SAS Viya OAuth flow for authentication.
Handles session management, job submission, and result retrieval using httpx.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from dotenv import load_dotenv
from fastmcp import Context, FastMCP
from fastmcp.server.dependencies import get_http_request
from fastmcp.server.middleware import Middleware, MiddlewareContext
from starlette.requests import Request
from starlette.responses import JSONResponse

from .cas_admin_tools import register_cas_admin_tools
from .config import VIYA_ENDPOINT, viya_auth
from .decision_tools import register_decision_tools
from .exceptions import AuthenticationError
from .forecast_tools import register_forecast_tools
from .id_tools import register_id_tools
from .model_tools import register_model_tools
from .monitoring_tools import register_monitoring_tools
from .prompts import register_prompts
from .scheduling_tools import register_scheduling_tools
from .tools import register_tools
from .va_tools import InjectedArgsMiddleware, register_va_tools
from .viya_client import logger
from .viya_utils import shutdown_session_cache

# Load environment variables before accessing them
load_dotenv()


class AuthMiddleware(Middleware):
    async def on_call_tool(self, ctx: MiddlewareContext, call_next: Any) -> Any:
        request = get_http_request()
        bearer_token = request.headers.get("Authorization")
        if not bearer_token:
            logger.error("No auth header found. Cannot proceed")
            raise AuthenticationError("No auth header found. Cannot proceed")

        parts = bearer_token.split()
        jwt = (
            parts[1]
            if len(parts) > 1 and parts[0].lower() == "bearer"
            else bearer_token
        )
        logger.info("Client auth header found, Swapping for upstream token")
        viya_access_info = await viya_auth.load_access_token(jwt)
        if viya_access_info:
            logger.info("Viya access info retrieved successfully!")
            fastmcp_ctx = ctx.fastmcp_context
            if fastmcp_ctx is not None:
                await fastmcp_ctx.set_state("access_token", viya_access_info.token)
        else:
            logger.error("Could not retrieve upstream access token!")
        return await call_next(ctx)


@asynccontextmanager
async def _lifespan(server: FastMCP) -> AsyncIterator[dict]:
    """Tear down warm compute sessions when the server stops."""
    try:
        yield {}
    finally:
        await shutdown_session_cache()


# Initialize the FastMCP server
logger.info("Connecting to SAS Viya at %s", VIYA_ENDPOINT)
mcp = FastMCP("SAS Viya Execution MCP Server", auth=viya_auth, lifespan=_lifespan)
mcp.add_middleware(AuthMiddleware())
# Strip WPIntelliChat's injected _-prefixed tool args (token already arrives in
# the bearer header) and route attached upload bytes into context state.
mcp.add_middleware(InjectedArgsMiddleware())


@mcp.custom_route("/health", methods=["GET"])
async def health_check(request: Request) -> JSONResponse:
    logger.info("Performing health check . . .")
    return JSONResponse({"status": "healthy", "service": "sas-viya-execution-mcp"})


# Token getter for HTTP mode: reads from context state set by AuthMiddleware
async def _http_get_token(ctx: Context) -> str:
    token = await ctx.get_state("access_token")
    if not token:
        raise AuthenticationError("No auth header found. Cannot authenticate to Viya")
    return token


# Register all tools and prompts
register_tools(mcp, _http_get_token)
register_va_tools(mcp, _http_get_token)
register_id_tools(mcp, _http_get_token)
register_decision_tools(mcp, _http_get_token)
register_model_tools(mcp, _http_get_token)
register_cas_admin_tools(mcp, _http_get_token)
register_scheduling_tools(mcp, _http_get_token)
register_forecast_tools(mcp, _http_get_token)
register_monitoring_tools(mcp, _http_get_token)
register_prompts(mcp)

# Primary ASGI app: streamable HTTP transport, served at /mcp.
app = mcp.http_app()

# Also expose the SSE transport (/sse stream + /messages) on the same app, for
# clients and registries that only speak SSE (e.g. ones that reject
# "streamable_http" as a transport type). Both transports share this single
# FastMCP instance: the streamable app's lifespan already starts the shared
# FastMCP lifespan that SSE relies on, and SSE needs no session manager of its
# own — so we graft only the SSE-unique routes onto the existing app rather
# than running a second server.
_sse_app = mcp.http_app(transport="sse")
_existing_paths = {getattr(r, "path", None) for r in app.routes}
for _route in _sse_app.routes:
    if getattr(_route, "path", None) not in _existing_paths:
        app.router.routes.append(_route)
