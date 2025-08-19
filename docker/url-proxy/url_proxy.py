#!/usr/bin/env python3
"""
ClearML URL Encoding Proxy Service

Fixes URL encoding issue where ClearML UI sends single-encoded URLs (%2F)
but the file server expects double-encoded URLs (%252F).

This proxy transparently fixes URLs and forwards requests to the real file server.
"""

import logging
import re
import urllib.parse
from typing import Optional

import httpx
import requests
import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Configuration
import os

FILESERVER_HOST = os.getenv("FILESERVER_HOST", "fileserver")  # Docker service name or IP
FILESERVER_PORT = int(os.getenv("FILESERVER_PORT", "8081"))  # Internal fileserver port
PROXY_PORT = int(os.getenv("PROXY_PORT", "8082"))  # This proxy port

app = FastAPI(
    title="ClearML URL Proxy", description="Fixes URL encoding issues for ClearML file server", version="1.0.0"
)

# HTTP client for forwarding requests
client = httpx.AsyncClient(timeout=30.0)


def fix_url_encoding(path: str) -> str:
    """
    Fix URL encoding issues in file paths.

    The problem: ClearML UI sends paths like '/3D Brainz/CT_BRAIN_...'
    but files on disk are stored as '/3D Brainz%252FCT_BRAIN_...'

    Args:
        path: Original URL path from request

    Returns:
        Fixed URL path with proper encoding
    """
    # Pattern to match problematic paths with forward slashes in directory names
    # Example: /3D Brainz/CT_BRAIN_multiclass_segmentation/...
    pattern = r"^/([^/]+)/([^/]+)_([^/]+)/(.*)$"

    match = re.match(pattern, path)
    if match:
        part1, part2, part3, rest = match.groups()

        # Check if this looks like our problematic pattern
        if "CT_BRAIN" in f"{part2}_{part3}" or "multiclass_segmentation" in f"{part2}_{part3}":
            # Fix encoding: replace the slash between part1 and part2_part3 with %252F
            # Files on disk are stored with double URL encoding
            fixed_path = f"/{part1}%252F{part2}_{part3}/{rest}"
            logger.info(f"URL fixed: {path} -> {fixed_path}")
            return fixed_path

    # No fix needed
    return path


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"])
async def proxy_request(request: Request, path: str):
    """
    Proxy all requests to the file server with URL encoding fixes.
    """
    # Fix URL encoding
    original_path = f"/{path}"
    fixed_path = fix_url_encoding(original_path)

    # Build target URL
    target_url = f"http://{FILESERVER_HOST}:{FILESERVER_PORT}{fixed_path}"

    # Preserve query parameters
    if request.url.query:
        target_url += f"?{request.url.query}"

    logger.info(f"{request.method} {original_path} -> {target_url}")

    try:
        # Forward the request using requests (sync) to avoid httpx URL encoding issues
        import asyncio

        # Get body first
        body = await request.body()

        def sync_request():
            return requests.request(
                method=request.method,
                url=target_url,  # requests preserves URL encoding better
                headers=dict(request.headers),
                data=body,
                allow_redirects=False,
                timeout=30,
            )

        # Run sync request in thread pool
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, sync_request)

        # Forward the response
        headers = dict(response.headers)

        # Remove hop-by-hop headers
        headers.pop("connection", None)
        headers.pop("transfer-encoding", None)

        # Add CORS headers
        headers["Access-Control-Allow-Origin"] = "*"
        headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS, DELETE, PUT"
        headers["Access-Control-Allow-Headers"] = (
            "DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range,Authorization"
        )

        return Response(content=response.content, status_code=response.status_code, headers=headers)

    except httpx.RequestError as e:
        logger.error(f"Request failed: {e}")
        return Response(content=f"Proxy error: {str(e)}", status_code=502, headers={"Content-Type": "text/plain"})


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "clearml-url-proxy"}


if __name__ == "__main__":
    logger.info(f"Starting ClearML URL Proxy on port {PROXY_PORT}")
    logger.info(f"Forwarding to {FILESERVER_HOST}:{FILESERVER_PORT}")

    uvicorn.run(app, host="0.0.0.0", port=PROXY_PORT, log_level="info")
