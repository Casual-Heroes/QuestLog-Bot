"""
Centralized AMP Bearer Token Fix for Casual Heroes Infrastructure
Fixes "SessionID in request body is deprecated" warning for AMP 2.6.4.2+

This module patches cc-ampapi library to use modern Bearer token authentication.

Usage in ANY Python file that uses AMP:

    # Import BEFORE any ampapi imports
    from utils.amp_bearer_fix import ensure_amp_bearer_fix
    ensure_amp_bearer_fix()

    # Then use AMP as normal
    from ampapi.bridge import Bridge
    from ampapi.controller import AMPControllerInstance
"""

import logging
import json
import aiohttp
from typing import Any, Union
from datetime import datetime
from pprint import pformat

logger = logging.getLogger("casual_heroes.amp_fix")

# Global flag to ensure we only patch once per process
_AMP_BEARER_FIX_APPLIED = False


def ensure_amp_bearer_fix():
    """
    Apply AMP Bearer token fix (idempotent - safe to call multiple times).

    This function monkey-patches ampapi.base.Base._call_api to use
    Authorization: Bearer {token} header instead of deprecated SESSIONID in body.
    """
    global _AMP_BEARER_FIX_APPLIED

    if _AMP_BEARER_FIX_APPLIED:
        logger.debug("AMP Bearer fix already applied, skipping")
        return

    try:
        from ampapi import base
        from ampapi.dataclass import APISession
    except ImportError:
        logger.warning("ampapi not installed, skipping Bearer token fix")
        return

    # Save original method
    original_call_api = base.Base._call_api

    async def patched_call_api(
        self,
        api: str,
        parameters: Union[None, dict[str, Any]] = None,
        format_data: Union[bool, None] = None,
        format_: Union[type, None] = None,
        sanitize_json: bool = True,
        _use_from_dict: bool = True,
        _auto_unpack: bool = True,
        _no_data: bool = False,
    ) -> Any:
        """
        PATCHED: Uses Authorization: Bearer header instead of SESSIONID in body.

        Changes from original cc-ampapi 1.3.0:
        - Removed: parameters["SESSIONID"] = api_session.id
        - Added: headers["Authorization"] = f"Bearer {api_session.id}"
        """

        # Get session from bridge
        api_session: APISession = self._bridge._sessions.get(
            self.instance_id,
            APISession(id="0", ttl=datetime.now())
        )

        # CRITICAL FIX: Use Authorization header with Bearer token
        headers = {
            "Accept": "text/javascript",
            "Authorization": f"Bearer {api_session.id}",  # ← Modern method
            "Content-Type": "application/json"
        }

        # Prepare parameters (NO SESSIONID in body!)
        if parameters is None:
            parameters = {}

        json_data: str = json.dumps(obj=parameters)
        _url: str = self.url + "/API/" + api

        self.logger.debug(
            "AMP API Call: %s | %s | %s",
            self.instance_id, api, _url
        )

        # Make request with Bearer token
        async with aiohttp.ClientSession() as session:
            try:
                post_req = await session.post(
                    url=_url,
                    headers=headers,
                    data=json_data
                )
            except Exception as e:
                self.logger.error("AMP API request failed: %s", type(e))
                raise ValueError(f"AMP API connection error: {e}")

            if post_req.content_length == 0:
                raise ValueError("AMP API returned no data")

            # Read response
            content = await post_req.read()

            if post_req.status != 200:
                error_msg = content.decode('utf-8', errors='ignore')
                raise ConnectionError(
                    f"AMP API error (HTTP {post_req.status}): {error_msg}"
                )

            # Parse JSON
            try:
                if sanitize_json:
                    content_str = content.decode("utf-8", errors="ignore")
                    content_str = content_str.replace("\\\\", "\\")
                    result = json.loads(content_str)
                else:
                    result = json.loads(content)
            except json.JSONDecodeError as e:
                raise ValueError(f"AMP returned malformed JSON: {e}")

            # Check for API-level errors
            if isinstance(result, dict):
                if "Result" in result:
                    if result["Result"] in ["Unauthorized Access", "Access Denied"]:
                        raise PermissionError(
                            f"AMP permission denied for {api}"
                        )
                    if "Instance Unavailable" in result["Result"]:
                        raise ConnectionError(
                            f"AMP instance unavailable: {_url}"
                        )

            # Return result (simplified - for full formatting use original logic)
            return result

    # Apply the patch
    base.Base._call_api = patched_call_api
    _AMP_BEARER_FIX_APPLIED = True

    logger.info("✅ AMP Bearer token fix applied - using Authorization header")


# Auto-apply on import (optional - comment out if you want explicit control)
# ensure_amp_bearer_fix()
