"""
Discord OAuth2 Authentication Helpers
"""
import os
import requests
from urllib.parse import urlencode
from django.conf import settings
from dotenv import load_dotenv

load_dotenv()

# Discord OAuth2 Configuration
DISCORD_CLIENT_ID = os.getenv("DISCORD_CLIENT_ID")
DISCORD_CLIENT_SECRET = os.getenv("DISCORD_CLIENT_SECRET")
DISCORD_REDIRECT_URI = os.getenv("DISCORD_REDIRECT_URI", "http://localhost:8000/auth/discord/callback/")

DISCORD_API_ENDPOINT = "https://discord.com/api/v10"
DISCORD_AUTH_URL = "https://discord.com/api/oauth2/authorize"
DISCORD_TOKEN_URL = "https://discord.com/api/oauth2/token"

# Scopes we request from Discord
DISCORD_SCOPES = ["identify", "email", "guilds"]


def get_discord_login_url(state: str = None) -> str:
    """Generate Discord OAuth2 authorization URL"""
    params = {
        "client_id": DISCORD_CLIENT_ID,
        "redirect_uri": DISCORD_REDIRECT_URI,
        "response_type": "code",
        "scope": " ".join(DISCORD_SCOPES),
    }
    if state:
        params["state"] = state

    return f"{DISCORD_AUTH_URL}?{urlencode(params)}"


def exchange_code_for_token(code: str) -> dict:
    """Exchange authorization code for access token"""
    data = {
        "client_id": DISCORD_CLIENT_ID,
        "client_secret": DISCORD_CLIENT_SECRET,
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": DISCORD_REDIRECT_URI,
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    response = requests.post(DISCORD_TOKEN_URL, data=data, headers=headers)
    response.raise_for_status()
    return response.json()


def refresh_access_token(refresh_token: str) -> dict:
    """Refresh an expired access token"""
    data = {
        "client_id": DISCORD_CLIENT_ID,
        "client_secret": DISCORD_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    response = requests.post(DISCORD_TOKEN_URL, data=data, headers=headers)
    response.raise_for_status()
    return response.json()


def get_discord_user(access_token: str) -> dict:
    """Fetch Discord user information"""
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    response = requests.get(f"{DISCORD_API_ENDPOINT}/users/@me", headers=headers)
    response.raise_for_status()
    return response.json()


def get_discord_guilds(access_token: str) -> list:
    """Fetch user's Discord guilds"""
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    response = requests.get(f"{DISCORD_API_ENDPOINT}/users/@me/guilds", headers=headers)
    response.raise_for_status()
    return response.json()


def get_discord_avatar_url(user_id: str, avatar_hash: str, size: int = 128) -> str:
    """Get Discord avatar URL for a user"""
    if avatar_hash:
        ext = "gif" if avatar_hash.startswith("a_") else "png"
        return f"https://cdn.discordapp.com/avatars/{user_id}/{avatar_hash}.{ext}?size={size}"
    else:
        # Default avatar based on discriminator or user ID
        default_index = int(user_id) % 5
        return f"https://cdn.discordapp.com/embed/avatars/{default_index}.png"


def revoke_token(access_token: str) -> bool:
    """Revoke a Discord access token"""
    data = {
        "client_id": DISCORD_CLIENT_ID,
        "client_secret": DISCORD_CLIENT_SECRET,
        "token": access_token,
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    try:
        response = requests.post(
            f"{DISCORD_API_ENDPOINT}/oauth2/token/revoke",
            data=data,
            headers=headers
        )
        return response.status_code == 200
    except Exception:
        return False
