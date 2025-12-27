"""
AMP API Authentication with Bearer Token Support
Fixes deprecated SessionID in request body warning
"""

import logging
import aiohttp
from typing import Optional, Dict, Any
from datetime import datetime, timedelta

logger = logging.getLogger("warden.amp_auth")


class AMPSession:
    """
    Manages AMP API authentication with Bearer token (modern method).
    Replaces deprecated SessionID in request body.
    """

    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.session_id: Optional[str] = None
        self.token_expires_at: Optional[datetime] = None
        self._http_session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self._http_session = aiohttp.ClientSession()
        await self.login()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._http_session:
            await self._http_session.close()

    async def login(self) -> str:
        """
        Authenticate with AMP and get session token.
        Returns the session ID for use as Bearer token.
        """
        url = f"{self.base_url}/API/Core/Login"

        payload = {
            "username": self.username,
            "password": self.password,
            "token": "",
            "rememberMe": False
        }

        try:
            async with self._http_session.post(url, json=payload) as response:
                response.raise_for_status()
                data = await response.json()

                if not data.get("success"):
                    error_msg = data.get("Message", "Unknown error")
                    raise Exception(f"AMP login failed: {error_msg}")

                self.session_id = data["sessionID"]
                # AMP sessions typically last 20 minutes, refresh at 15 to be safe
                self.token_expires_at = datetime.now() + timedelta(minutes=15)

                logger.info(f"✅ AMP authenticated successfully (expires: {self.token_expires_at})")
                return self.session_id

        except Exception as e:
            logger.error(f"❌ AMP login failed: {e}")
            raise

    async def ensure_authenticated(self):
        """Re-authenticate if token is expired or missing."""
        if not self.session_id or not self.token_expires_at:
            await self.login()
        elif datetime.now() >= self.token_expires_at:
            logger.info("🔄 AMP session expired, re-authenticating...")
            await self.login()

    async def api_call(
        self,
        endpoint: str,
        method: str = "POST",
        data: Optional[Dict[str, Any]] = None,
        instance_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Make authenticated API call using Bearer token (modern method).

        Args:
            endpoint: API endpoint (e.g., "Core/GetStatus")
            method: HTTP method (GET, POST, etc.)
            data: Request payload
            instance_id: Optional instance ID for instance-specific calls

        Returns:
            JSON response data
        """
        await self.ensure_authenticated()

        # Build URL
        if instance_id:
            url = f"{self.base_url}/API/ADSModule/Servers/{instance_id}/API/{endpoint}"
        else:
            url = f"{self.base_url}/API/{endpoint}"

        # CRITICAL: Use Authorization header with Bearer token (modern method)
        # This fixes the "SessionID in request body deprecated" warning
        headers = {
            "Authorization": f"Bearer {self.session_id}",
            "Content-Type": "application/json"
        }

        try:
            async with self._http_session.request(
                method=method,
                url=url,
                headers=headers,
                json=data or {}
            ) as response:
                response.raise_for_status()
                result = await response.json()

                if isinstance(result, dict) and not result.get("success", True):
                    error = result.get("Message", "Unknown error")
                    logger.warning(f"AMP API error: {error}")

                return result

        except aiohttp.ClientResponseError as e:
            if e.status == 401:
                # Token expired, retry once
                logger.warning("🔄 401 Unauthorized - retrying with fresh token...")
                await self.login()
                return await self.api_call(endpoint, method, data, instance_id)
            else:
                logger.error(f"❌ AMP API call failed ({e.status}): {e}")
                raise
        except Exception as e:
            logger.error(f"❌ AMP API call exception: {e}")
            raise

    async def send_console_command(self, instance_id: str, command: str) -> bool:
        """
        Send console command to game server.

        Args:
            instance_id: AMP instance ID
            command: Console command (e.g., "say Hello")

        Returns:
            True if successful
        """
        try:
            await self.api_call(
                endpoint="Core/SendConsoleMessage",
                instance_id=instance_id,
                data={"message": command}
            )
            logger.debug(f"📤 Sent console command: {command}")
            return True
        except Exception as e:
            logger.error(f"Failed to send console command: {e}")
            return False

    async def get_instance_status(self, instance_id: str) -> Dict[str, Any]:
        """Get server status (running, CPU, RAM, players, etc.)"""
        return await self.api_call("Core/GetStatus", instance_id=instance_id)

    async def get_file_contents(self, instance_id: str, file_path: str) -> str:
        """
        Read file from game server filesystem.

        Args:
            instance_id: AMP instance ID
            file_path: Relative path from game directory (e.g., "saves/Navezgane/Player/76561198012345678.ttp")

        Returns:
            File contents as string
        """
        result = await self.api_call(
            endpoint="FileManagerPlugin/ReadFileChunk",
            instance_id=instance_id,
            data={
                "Filename": file_path,
                "Position": 0,
                "Length": 10485760  # 10MB max
            }
        )

        # Result format: {"Base64Data": "...", "EndOfFile": true}
        import base64
        if result.get("Base64Data"):
            return base64.b64decode(result["Base64Data"]).decode('utf-8', errors='ignore')
        return ""

    async def list_directory(self, instance_id: str, directory: str) -> list:
        """
        List files in directory.

        Returns:
            List of file/folder dicts with 'Filename', 'IsDirectory', 'SizeBytes', etc.
        """
        result = await self.api_call(
            endpoint="FileManagerPlugin/GetDirectoryListing",
            instance_id=instance_id,
            data={"Dir": directory}
        )
        return result.get("result", [])


# Global session pool (one per AMP instance)
_amp_sessions: Dict[str, AMPSession] = {}


async def get_amp_session(base_url: str, username: str, password: str) -> AMPSession:
    """
    Get or create AMP session (connection pooling).

    Usage:
        session = await get_amp_session("http://192.168.1.154:8080", "admin", "password")
        status = await session.get_instance_status("my-instance-id")
    """
    cache_key = f"{base_url}:{username}"

    if cache_key not in _amp_sessions:
        session = AMPSession(base_url, username, password)
        # Initialize HTTP session
        session._http_session = aiohttp.ClientSession()
        await session.login()
        _amp_sessions[cache_key] = session
    else:
        session = _amp_sessions[cache_key]
        await session.ensure_authenticated()

    return session
