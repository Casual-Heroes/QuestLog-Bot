# warden/utils/igdb.py
"""
IGDB (Internet Game Database) Integration

Uses Twitch OAuth2 for authentication since IGDB is owned by Twitch.
Provides game search for the LFG system.

Setup:
1. Go to https://dev.twitch.tv/console
2. Create an application
3. Get Client ID and Client Secret
4. Add to .env: TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET
"""

import os
import time
import aiohttp
import asyncio
import logging
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field
from functools import wraps

logger = logging.getLogger("warden.igdb")

# Twitch/IGDB credentials
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID", "")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET", "")

# API endpoints
TWITCH_TOKEN_URL = "https://id.twitch.tv/oauth2/token"
IGDB_API_URL = "https://api.igdb.com/v4"

# Cache for the access token
_token_cache = {
    "access_token": None,
    "expires_at": 0
}

# Rate limiting - IGDB allows 4 requests per second
_rate_limit_lock = asyncio.Lock()
_last_request_time = 0
_min_request_interval = 0.25  # 4 requests per second = 0.25 seconds between requests


async def _rate_limit():
    """Enforce rate limiting for IGDB API calls (4 requests/second)."""
    global _last_request_time

    async with _rate_limit_lock:
        elapsed = time.time() - _last_request_time
        if elapsed < _min_request_interval:
            await asyncio.sleep(_min_request_interval - elapsed)
        _last_request_time = time.time()


@dataclass
class IGDBGame:
    """Represents a game from IGDB."""
    id: int
    name: str
    slug: str
    cover_url: Optional[str] = None
    platforms: List[str] = field(default_factory=list)
    summary: Optional[str] = None
    release_year: Optional[int] = None
    release_date: Optional[int] = None  # Unix timestamp
    genres: List[str] = field(default_factory=list)  # For game discovery filtering
    themes: List[str] = field(default_factory=list)  # Game themes (Stealth, Open World, etc.)
    game_modes: List[str] = field(default_factory=list)  # Single-player, Co-op, etc.
    keywords: List[str] = field(default_factory=list)  # Keywords like Souls-like, Metroidvania, etc.
    igdb_url: Optional[str] = None
    rating: Optional[float] = None  # Average IGDB user rating (Double)
    hypes: Optional[int] = None  # Number of follows a game gets before release
    screenshots: List[str] = field(default_factory=list)  # Screenshot URLs
    videos: List[Dict[str, str]] = field(default_factory=list)  # Video info (name, video_id)
    websites: List[Dict[str, str]] = field(default_factory=list)  # Website info (category, url)


async def get_twitch_token() -> Optional[str]:
    """Get or refresh Twitch OAuth token for IGDB API access."""
    global _token_cache

    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
        logger.warning("Twitch credentials not configured. IGDB search disabled.")
        return None

    # Check if we have a valid cached token
    if _token_cache["access_token"] and time.time() < _token_cache["expires_at"]:
        return _token_cache["access_token"]

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                TWITCH_TOKEN_URL,
                params={
                    "client_id": TWITCH_CLIENT_ID,
                    "client_secret": TWITCH_CLIENT_SECRET,
                    "grant_type": "client_credentials"
                }
            ) as response:
                if response.status != 200:
                    logger.error(f"Failed to get Twitch token: {response.status}")
                    return None

                data = await response.json()
                _token_cache["access_token"] = data["access_token"]
                # Expire 1 hour early to be safe
                _token_cache["expires_at"] = time.time() + data["expires_in"] - 3600

                logger.info("Obtained new Twitch/IGDB access token")
                return _token_cache["access_token"]

    except Exception as e:
        logger.error(f"Error getting Twitch token: {e}")
        return None


async def search_games(query: str, limit: int = 10) -> List[IGDBGame]:
    """
    Search for games on IGDB.

    Args:
        query: Search term
        limit: Max results (default 10)

    Returns:
        List of IGDBGame objects
    """
    token = await get_twitch_token()
    if not token:
        return []

    await _rate_limit()  # Enforce rate limiting

    try:
        async with aiohttp.ClientSession() as session:
            headers = {
                "Client-ID": TWITCH_CLIENT_ID,
                "Authorization": f"Bearer {token}",
                "Accept": "application/json"
            }

            # IGDB uses a custom query language
            body = f'''
                search "{query}";
                fields name, slug, cover.image_id, platforms.abbreviation, summary, first_release_date;
                where category = 0;
                limit {limit};
            '''

            async with session.post(
                f"{IGDB_API_URL}/games",
                headers=headers,
                data=body
            ) as response:
                if response.status != 200:
                    logger.error(f"IGDB search failed: {response.status}")
                    return []

                data = await response.json()
                games = []

                for game_data in data:
                    # Build cover URL
                    cover_url = None
                    if "cover" in game_data and game_data["cover"]:
                        image_id = game_data["cover"].get("image_id")
                        if image_id:
                            # t_cover_big = 264x374
                            cover_url = f"https://images.igdb.com/igdb/image/upload/t_cover_big/{image_id}.jpg"

                    # Extract platform abbreviations
                    platforms = []
                    if "platforms" in game_data:
                        for platform in game_data["platforms"]:
                            abbr = platform.get("abbreviation")
                            if abbr:
                                platforms.append(abbr)

                    # Extract release year
                    release_year = None
                    if "first_release_date" in game_data:
                        import datetime
                        ts = game_data["first_release_date"]
                        release_year = datetime.datetime.fromtimestamp(ts).year

                    game = IGDBGame(
                        id=game_data["id"],
                        name=game_data["name"],
                        slug=game_data.get("slug", ""),
                        cover_url=cover_url,
                        platforms=platforms,
                        summary=game_data.get("summary"),
                        release_year=release_year
                    )
                    games.append(game)

                return games

    except Exception as e:
        logger.error(f"Error searching IGDB: {e}")
        return []


async def get_game_by_id(game_id: int) -> Optional[IGDBGame]:
    """Get a specific game by IGDB ID."""
    token = await get_twitch_token()
    if not token:
        return None

    await _rate_limit()  # Enforce rate limiting

    try:
        async with aiohttp.ClientSession() as session:
            headers = {
                "Client-ID": TWITCH_CLIENT_ID,
                "Authorization": f"Bearer {token}",
                "Accept": "application/json"
            }

            body = f'''
                fields name, slug, cover.image_id, platforms.abbreviation, summary, first_release_date;
                where id = {game_id};
            '''

            async with session.post(
                f"{IGDB_API_URL}/games",
                headers=headers,
                data=body
            ) as response:
                if response.status != 200:
                    return None

                data = await response.json()
                if not data:
                    return None

                game_data = data[0]

                # Build cover URL
                cover_url = None
                if "cover" in game_data and game_data["cover"]:
                    image_id = game_data["cover"].get("image_id")
                    if image_id:
                        cover_url = f"https://images.igdb.com/igdb/image/upload/t_cover_big/{image_id}.jpg"

                # Extract platform abbreviations
                platforms = []
                if "platforms" in game_data:
                    for platform in game_data["platforms"]:
                        abbr = platform.get("abbreviation")
                        if abbr:
                            platforms.append(abbr)

                # Extract release year
                release_year = None
                if "first_release_date" in game_data:
                    import datetime
                    ts = game_data["first_release_date"]
                    release_year = datetime.datetime.fromtimestamp(ts).year

                return IGDBGame(
                    id=game_data["id"],
                    name=game_data["name"],
                    slug=game_data.get("slug", ""),
                    cover_url=cover_url,
                    platforms=platforms,
                    summary=game_data.get("summary"),
                    release_year=release_year
                )

    except Exception as e:
        logger.error(f"Error getting game from IGDB: {e}")
        return None


async def get_release_dates_bulk(game_ids: List[int]) -> Dict[int, Optional[int]]:
    """
    Fetch release dates for multiple games by IGDB ID.

    Used to refresh release dates for games with placeholder dates (TBD).

    Args:
        game_ids: List of IGDB game IDs to query

    Returns:
        Dict mapping game_id to release_date timestamp (or None if not found)
    """
    if not game_ids:
        return {}

    token = await get_twitch_token()
    if not token:
        return {}

    results = {}

    # IGDB allows querying multiple IDs in one request (up to 500)
    # Process in batches of 100 for safety
    batch_size = 100
    for i in range(0, len(game_ids), batch_size):
        batch = game_ids[i:i + batch_size]

        await _rate_limit()  # Enforce rate limiting

        try:
            async with aiohttp.ClientSession() as session:
                headers = {
                    "Client-ID": TWITCH_CLIENT_ID,
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json"
                }

                # Query multiple IDs at once
                ids_str = ",".join(str(gid) for gid in batch)
                body = f'''
                    fields id, first_release_date;
                    where id = ({ids_str});
                    limit {len(batch)};
                '''

                async with session.post(
                    f"{IGDB_API_URL}/games",
                    headers=headers,
                    data=body
                ) as response:
                    if response.status != 200:
                        logger.error(f"IGDB API returned {response.status} for release date bulk fetch")
                        continue

                    data = await response.json()
                    for game_data in data:
                        game_id = game_data.get("id")
                        release_date = game_data.get("first_release_date")
                        if game_id:
                            results[game_id] = release_date

        except Exception as e:
            logger.error(f"Error fetching release dates from IGDB: {e}")
            continue

    return results


async def get_popular_games(limit: int = 25) -> List[IGDBGame]:
    """Get currently popular games for suggestions."""
    token = await get_twitch_token()
    if not token:
        return []

    await _rate_limit()  # Enforce rate limiting

    try:
        async with aiohttp.ClientSession() as session:
            headers = {
                "Client-ID": TWITCH_CLIENT_ID,
                "Authorization": f"Bearer {token}",
                "Accept": "application/json"
            }

            # Get games with high hype/follows, released in last 2 years
            body = f'''
                fields name, slug, cover.image_id, platforms.abbreviation, summary, first_release_date, follows;
                where category = 0 & follows > 10;
                sort follows desc;
                limit {limit};
            '''

            async with session.post(
                f"{IGDB_API_URL}/games",
                headers=headers,
                data=body
            ) as response:
                if response.status != 200:
                    return []

                data = await response.json()
                games = []

                for game_data in data:
                    cover_url = None
                    if "cover" in game_data and game_data["cover"]:
                        image_id = game_data["cover"].get("image_id")
                        if image_id:
                            cover_url = f"https://images.igdb.com/igdb/image/upload/t_cover_big/{image_id}.jpg"

                    platforms = []
                    if "platforms" in game_data:
                        for platform in game_data["platforms"]:
                            abbr = platform.get("abbreviation")
                            if abbr:
                                platforms.append(abbr)

                    release_year = None
                    if "first_release_date" in game_data:
                        import datetime
                        ts = game_data["first_release_date"]
                        release_year = datetime.datetime.fromtimestamp(ts).year

                    game = IGDBGame(
                        id=game_data["id"],
                        name=game_data["name"],
                        slug=game_data.get("slug", ""),
                        cover_url=cover_url,
                        platforms=platforms,
                        summary=game_data.get("summary"),
                        release_year=release_year
                    )
                    games.append(game)

                return games

    except Exception as e:
        logger.error(f"Error getting popular games: {e}")
        return []


async def get_all_keywords(max_results: int = 10000) -> List[Dict[str, Any]]:
    """
    Fetch all keywords from IGDB (paginated).

    Useful for discovering available keywords for filtering.

    Args:
        max_results: Maximum total keywords to return (default 10000)

    Returns:
        List of keyword dicts with 'id', 'name', 'slug'
    """
    token = await get_twitch_token()
    if not token:
        return []

    all_keywords = []
    offset = 0
    batch_size = 500  # IGDB max per request

    try:
        async with aiohttp.ClientSession() as session:
            headers = {
                "Client-ID": TWITCH_CLIENT_ID,
                "Authorization": f"Bearer {token}",
                "Accept": "application/json"
            }

            while len(all_keywords) < max_results:
                await _rate_limit()

                body = f'''
                    fields id, name, slug;
                    sort name asc;
                    limit {batch_size};
                    offset {offset};
                '''

                async with session.post(
                    f"{IGDB_API_URL}/keywords",
                    headers=headers,
                    data=body
                ) as response:
                    if response.status != 200:
                        logger.error(f"IGDB fetch keywords failed: {response.status}")
                        break

                    batch = await response.json()
                    if not batch:
                        break  # No more results

                    all_keywords.extend(batch)
                    offset += batch_size

                    logger.info(f"Fetched {len(all_keywords)} keywords so far...")

                    if len(batch) < batch_size:
                        break  # Last page

            return all_keywords[:max_results]

    except Exception as e:
        logger.error(f"Error fetching keywords: {e}", exc_info=True)
        return all_keywords


async def get_keyword_ids(keyword_names: List[str]) -> List[int]:
    """
    Look up IGDB keyword IDs by name.

    Uses the IGDB /keywords endpoint to find keyword IDs.
    Keywords are words/phrases tagged to games like "Souls-like", "Metroidvania", etc.

    Args:
        keyword_names: List of keyword names to look up

    Returns:
        List of keyword IDs found
    """
    if not keyword_names:
        return []

    token = await get_twitch_token()
    if not token:
        return []

    await _rate_limit()

    try:
        async with aiohttp.ClientSession() as session:
            headers = {
                "Client-ID": TWITCH_CLIENT_ID,
                "Authorization": f"Bearer {token}",
                "Accept": "application/json"
            }

            # Build OR query for all keyword names (case-insensitive)
            name_conditions = " | ".join([f'name ~ *"{name}"*' for name in keyword_names])
            body = f'''
                fields id, name;
                where {name_conditions};
                limit 100;
            '''

            logger.info(f"Looking up keyword IDs for: {keyword_names}")

            async with session.post(
                f"{IGDB_API_URL}/keywords",
                headers=headers,
                data=body
            ) as response:
                if response.status != 200:
                    logger.error(f"IGDB keyword lookup failed: {response.status}")
                    text = await response.text()
                    logger.error(f"Response: {text}")
                    return []

                data = await response.json()

                # Return all matching keyword IDs from IGDB's fuzzy search
                # IGDB already does the fuzzy matching, so we trust its results
                ids = []
                for kw in data:
                    ids.append(kw["id"])
                    logger.info(f"Found keyword '{kw['name']}' (ID: {kw['id']})")

                return ids

    except Exception as e:
        logger.error(f"Error looking up keyword IDs: {e}", exc_info=True)
        return []


async def search_upcoming_games(
    days_ahead: int = 90,
    days_behind: int = 0,
    genres: Optional[List[str]] = None,
    themes: Optional[List[str]] = None,
    keywords: Optional[List[str]] = None,
    game_modes: Optional[List[str]] = None,
    platforms: Optional[List[str]] = None,
    min_hype: Optional[int] = None,
    min_rating: Optional[float] = None,
    limit: int = 50
) -> List[IGDBGame]:
    """
    Search for game releases with optional filters (past and future).

    Args:
        days_ahead: How many days in the future to search (default 90)
        days_behind: How many days in the past to search (default 0)
        genres: List of genre names to filter by (e.g., ["RPG", "Action"])
        themes: List of theme names to filter by (e.g., ["Fantasy", "Stealth"])
        keywords: List of keywords to filter by (e.g., ["Souls-like", "Metroidvania"])
        game_modes: List of game modes (e.g., ["Single player", "Co-op"])
        platforms: List of platform names (e.g., ["PC", "PlayStation 5"])
        min_hype: Minimum hype score (number of follows before release)
        min_rating: Minimum user rating (IGDB Double field)
        limit: Max results (default 50)

    Returns:
        List of IGDBGame objects matching filters

    Note:
        IGDB uses specific IDs for genres, modes, and platforms.
        This function maps common names to IGDB IDs internally.

        Examples:
        - Recently released (last 7 days): days_ahead=0, days_behind=7
        - Coming soon (next 7 days): days_ahead=7, days_behind=0
        - Last week to next week: days_ahead=7, days_behind=7
    """
    token = await get_twitch_token()
    if not token:
        return []

    await _rate_limit()

    try:
        # Calculate timestamp range
        now = int(time.time())
        start_date = now - (days_behind * 24 * 60 * 60)  # Past date
        end_date = now + (days_ahead * 24 * 60 * 60)     # Future date

        # Build the WHERE clause
        # Required clauses (always AND): date range, hype, rating
        required_clauses = [
            f"first_release_date >= {start_date}",
            f"first_release_date <= {end_date}",
            # NOTE: Removed category filter - many games don't have this field set in IGDB
            # "category = 0"  # Main game (not DLC, expansion, etc.)
        ]

        # TIERED FILTER LOGIC:
        # - Primary filters (genres, themes): Use AND between categories - game must match BOTH
        # - Secondary filters (keywords, modes, platforms): Only used when no primary filters exist
        #
        # This ensures games like Nioh 3 (has genres/themes but no keywords) aren't excluded
        # when users search with genres + themes + keywords.
        #
        # Within each category, OR logic is used (e.g., RPG OR Adventure)

        primary_filter_clauses = []  # Genres, Themes - AND between these
        secondary_filter_clauses = []  # Keywords, Modes, Platforms - fallback only

        # Add genre filter if specified
        if genres:
            # Map genre names to IGDB genre IDs (must match UI exactly!)
            genre_mapping = {
                # UI-provided genre names (from views.py available_genres)
                "Pinball": 30, "Adventure": 31, "Indie": 32, "Arcade": 33,
                "Visual Novel": 34, "Card & Board Game": 35, "MOBA": 36,
                "Point-and-click": 2, "Fighting": 4, "Shooter": 5,
                "Music": 7, "Platform": 8, "Puzzle": 9, "Racing": 10,
                "Real Time Strategy (RTS)": 11, "Role-playing (RPG)": 12,
                "Simulator": 13, "Sport": 14, "Strategy": 15,
                "Turn-based strategy (TBS)": 16, "Tactical": 24,
                "Hack and slash/Beat 'em up": 25, "Quiz/Trivia": 26,
                # Legacy/shorthand mappings for backwards compatibility
                "RPG": 12, "ARPG": 12, "JRPG": 12, "RTS": 11,
                "FPS": 5, "TPS": 5, "MMO": 32, "MMORPG": 32,
                "Simulation": 13, "Sports": 14, "Sandbox": 33,
                "Roguelike": 33, "Roguelite": 33, "Metroidvania": 31,
                "Souls-Like": 12, "Action": 4
            }
            genre_ids = [genre_mapping.get(g) for g in genres if g in genre_mapping]
            if genre_ids:
                # IGDB syntax: genres = (id1) | genres = (id2) for OR logic
                # genres = (id1, id2) means AND (must have ALL)
                genre_conditions = " | ".join([f"genres = ({gid})" for gid in genre_ids])
                primary_filter_clauses.append(f"({genre_conditions})")

        # Add theme filter if specified
        if themes:
            # Map theme names to IGDB theme IDs (must match UI exactly!)
            theme_mapping = {
                "Action": 1, "Fantasy": 17, "Science fiction": 18,
                "Horror": 19, "Thriller": 20, "Survival": 21,
                "Historical": 22, "Stealth": 23, "Comedy": 27,
                "Business": 28, "Drama": 31, "Non-fiction": 32,
                "Sandbox": 33, "Educational": 34, "Kids": 35,
                "Open world": 38, "Warfare": 39, "Party": 40,
                "4X (explore, expand, exploit, and exterminate)": 41,
                "Erotic": 42, "Mystery": 43, "Romance": 44,
                # Legacy shorthand for backwards compatibility
                "4X": 41
            }
            theme_ids = [theme_mapping.get(t) for t in themes if t in theme_mapping]
            if theme_ids:
                # IGDB syntax: themes = (id1) | themes = (id2) for OR logic
                # themes = (id1, id2) means AND (must have ALL)
                theme_conditions = " | ".join([f"themes = ({tid})" for tid in theme_ids])
                primary_filter_clauses.append(f"({theme_conditions})")

        # Add keyword filter if specified (Souls-like, Metroidvania, etc.)
        # Keywords are SECONDARY filters - only used if no genres/themes are specified
        # This prevents excluding games like Nioh 3 that have no keywords assigned
        if keywords:
            keyword_ids = await get_keyword_ids(keywords)
            if keyword_ids:
                # IGDB syntax: keywords = (id1) | keywords = (id2) for OR logic
                # keywords = (id1, id2) means AND (must have ALL)
                keyword_conditions = " | ".join([f"keywords = ({kid})" for kid in keyword_ids])
                secondary_filter_clauses.append(f"({keyword_conditions})")

        # Add game mode filter if specified
        if game_modes:
            # Map game mode names to IGDB game_mode IDs (must match UI exactly!)
            mode_mapping = {
                # UI-provided mode names (from views.py available_modes)
                "Single player": 1,
                "Multiplayer": 2,
                "Co-operative": 3,
                "Split screen": 4,
                "Massively Multiplayer Online (MMO)": 5,
                "Battle Royale": 6,
                # Legacy/shorthand mappings for backwards compatibility
                "Single-player": 1, "Co-op": 3, "Cooperative": 3,
                "Massively Multiplayer": 5, "Massively Multiplayer Online": 5,
                "MMO": 5
            }
            mode_ids = [mode_mapping.get(m) for m in game_modes if m in mode_mapping]
            if mode_ids:
                # IGDB syntax: game_modes = (id1) | game_modes = (id2) for OR logic
                # game_modes = (id1, id2) means AND (must have ALL)
                mode_conditions = " | ".join([f"game_modes = ({mid})" for mid in mode_ids])
                secondary_filter_clauses.append(f"({mode_conditions})")

        # Add platform filter if specified
        if platforms:
            # Map platform names to IGDB platform IDs (actual gaming platforms only, not stores)
            # Reference: https://api.igdb.com/v4/platforms
            platform_mapping = {
                # UI-provided platform names (from views.py available_platforms)
                "PC (Microsoft Windows)": 6, "Mac": 14, "Linux": 3,
                "PlayStation 3": 9, "PlayStation 4": 48, "PlayStation 5": 167,
                "Xbox 360": 12, "Xbox One": 49, "Xbox Series X|S": 169,
                "Nintendo Switch": 130, "Android": 34, "iOS": 39,
                "Web browser": 82,
                # Legacy/shorthand mappings for backwards compatibility
                "PC": 6, "Windows": 6, "PS3": 9, "PS4": 48, "PS5": 167,
                "Xbox Series": 169, "Switch": 130, "macOS": 14,
                "PlayStation Vita": 46, "PS Vita": 46,
                "PlayStation VR": 165, "PSVR": 165,
                "PlayStation VR2": 390, "PSVR2": 390,
                "Nintendo Switch 2": 471, "Switch 2": 471,
                "Nintendo 3DS": 37, "3DS": 37,
                "Wii U": 41, "Wii": 5,
                "Meta Quest 2": 471, "Meta Quest 3": 473,
                "Oculus Quest": 384, "Oculus Rift": 162
            }
            platform_ids = [platform_mapping.get(p) for p in platforms if p in platform_mapping]
            if platform_ids:
                # IGDB syntax: platforms = (id1) | platforms = (id2) for OR logic
                # platforms = (id1, id2) means AND (must have ALL)
                platform_conditions = " | ".join([f"platforms = ({pid})" for pid in platform_ids])
                secondary_filter_clauses.append(f"({platform_conditions})")

        # Add hype threshold filter if specified (number of follows before release)
        if min_hype is not None and min_hype > 0:
            required_clauses.append(f"hypes >= {min_hype}")

        # Add rating threshold filter if specified (IGDB Double field)
        if min_rating is not None and min_rating > 0:
            required_clauses.append(f"rating >= {min_rating}")

        # Build final WHERE clause with TIERED filter logic:
        # 1. Required clauses (date range, hype, rating) - always AND
        # 2. Primary filters (genres, themes) - AND between them if both exist
        # 3. Secondary filters (keywords, modes, platforms) - ONLY used if NO primary filters
        #
        # This prevents excluding games like Nioh 3 that match genres/themes but have no keywords
        #
        # Examples:
        # - User selects: RPG, Adventure (genres) + Action, Fantasy (themes) + soulslike (keyword)
        #   Query: (genres) AND (themes)  -- keywords ignored because primary filters exist
        #   Result: 63 games including Nioh 3 (which has no keywords)
        #
        # - User selects: soulslike, metroidvania (keywords only)
        #   Query: (keywords)  -- used because no primary filters
        #   Result: Games tagged with those keywords

        if primary_filter_clauses:
            # Primary filters exist - use AND between genres/themes, ignore secondary filters
            # This gives focused results without excluding games missing optional tags
            primary_combined = " & ".join(primary_filter_clauses)
            required_clauses.append(f"({primary_combined})")
            logger.info(f"Using PRIMARY filters (genres/themes): {primary_combined}")
        elif secondary_filter_clauses:
            # No primary filters - fall back to secondary filters with OR between them
            secondary_combined = " | ".join(secondary_filter_clauses)
            required_clauses.append(f"({secondary_combined})")
            logger.info(f"Using SECONDARY filters (keywords/modes/platforms): {secondary_combined}")

        where_clause = " & ".join(required_clauses)

        async with aiohttp.ClientSession() as session:
            headers = {
                "Client-ID": TWITCH_CLIENT_ID,
                "Authorization": f"Bearer {token}",
                "Accept": "application/json"
            }

            # Request comprehensive game data including genres, themes, keywords, modes, ratings, hypes, media
            body = f'''
                fields name, slug, cover.image_id, platforms.name, summary,
                       first_release_date, genres.name, themes.name, keywords.name, game_modes.name, url,
                       rating, hypes,
                       screenshots.image_id, videos.name, videos.video_id,
                       websites.category, websites.url;
                where {where_clause};
                sort first_release_date asc;
                limit {limit};
            '''

            # Log the query for debugging
            logger.info(f"IGDB Query: {body.strip()}")

            async with session.post(
                f"{IGDB_API_URL}/games",
                headers=headers,
                data=body
            ) as response:
                if response.status != 200:
                    logger.error(f"IGDB upcoming games search failed: {response.status}")
                    text = await response.text()
                    logger.error(f"Response: {text}")
                    return []

                data = await response.json()
                games = []

                for game_data in data:
                    # Build cover URL
                    cover_url = None
                    if "cover" in game_data and game_data["cover"]:
                        image_id = game_data["cover"].get("image_id")
                        if image_id:
                            cover_url = f"https://images.igdb.com/igdb/image/upload/t_cover_big/{image_id}.jpg"

                    # Extract platform names
                    platforms_list = []
                    if "platforms" in game_data:
                        for platform in game_data["platforms"]:
                            name = platform.get("name")
                            if name:
                                platforms_list.append(name)

                    # Extract genres
                    genres_list = []
                    if "genres" in game_data:
                        for genre in game_data["genres"]:
                            name = genre.get("name")
                            if name:
                                genres_list.append(name)

                    # Extract themes
                    themes_list = []
                    if "themes" in game_data:
                        for theme in game_data["themes"]:
                            name = theme.get("name")
                            if name:
                                themes_list.append(name)

                    # Extract game modes
                    modes_list = []
                    if "game_modes" in game_data:
                        for mode in game_data["game_modes"]:
                            name = mode.get("name")
                            if name:
                                modes_list.append(name)

                    # Extract keywords (Souls-like, Metroidvania, etc.)
                    keywords_list = []
                    if "keywords" in game_data:
                        for keyword in game_data["keywords"]:
                            name = keyword.get("name")
                            if name:
                                keywords_list.append(name)

                    # Extract release date
                    release_date = game_data.get("first_release_date")
                    release_year = None
                    if release_date:
                        import datetime
                        release_year = datetime.datetime.fromtimestamp(release_date).year

                    # Extract rating and hypes (pre-release follows)
                    rating = game_data.get("rating")
                    hypes = game_data.get("hypes")

                    # Extract screenshots
                    screenshots_list = []
                    if "screenshots" in game_data:
                        for screenshot in game_data["screenshots"][:4]:  # Limit to 4 screenshots
                            image_id = screenshot.get("image_id")
                            if image_id:
                                # Use 1080p screenshots
                                screenshots_list.append(f"https://images.igdb.com/igdb/image/upload/t_screenshot_big/{image_id}.jpg")

                    # Extract videos
                    videos_list = []
                    if "videos" in game_data:
                        for video in game_data["videos"][:2]:  # Limit to 2 videos
                            video_id = video.get("video_id")
                            name = video.get("name", "Trailer")
                            if video_id:
                                videos_list.append({"name": name, "video_id": video_id})

                    # Extract websites
                    websites_list = []
                    if "websites" in game_data:
                        # Website categories: 1=official, 2=wikia, 3=wikipedia, 4=facebook, 5=twitter,
                        # 6=twitch, 8=instagram, 9=youtube, 10=iphone, 11=ipad, 12=android,
                        # 13=steam, 14=reddit, 15=itch, 16=epicgames, 17=gog, 18=discord
                        for website in game_data["websites"]:
                            category = website.get("category")
                            url = website.get("url")
                            if category and url:
                                websites_list.append({"category": category, "url": url})

                    game = IGDBGame(
                        id=game_data["id"],
                        name=game_data["name"],
                        slug=game_data.get("slug", ""),
                        cover_url=cover_url,
                        platforms=platforms_list,
                        summary=game_data.get("summary"),
                        release_year=release_year,
                        release_date=release_date,
                        genres=genres_list,
                        themes=themes_list,
                        game_modes=modes_list,
                        keywords=keywords_list,
                        igdb_url=game_data.get("url"),
                        rating=rating,
                        hypes=hypes,
                        screenshots=screenshots_list,
                        videos=videos_list,
                        websites=websites_list
                    )
                    games.append(game)

                logger.info(f"Found {len(games)} upcoming games matching filters")
                return games

    except Exception as e:
        logger.error(f"Error searching upcoming games: {e}", exc_info=True)
        return []


def is_configured() -> bool:
    """Check if IGDB is configured with credentials."""
    return bool(TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET)