#!/usr/bin/env python3
"""
Fetch all keywords from IGDB API.
Run this to see available keywords for game discovery filtering.

Usage:
    python scripts/fetch_igdb_keywords.py
    python scripts/fetch_igdb_keywords.py --search souls
    python scripts/fetch_igdb_keywords.py --limit 1000
"""

import asyncio
import argparse
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not required if env vars are already set

from utils.igdb import get_all_keywords, get_keyword_ids


async def main():
    parser = argparse.ArgumentParser(description='Fetch IGDB keywords')
    parser.add_argument('--search', '-s', type=str, help='Search for specific keyword (filters full list)')
    parser.add_argument('--lookup', '-k', type=str, help='Look up keyword ID via IGDB API')
    parser.add_argument('--max', '-m', type=int, default=10000, help='Max keywords to fetch (default 10000)')
    parser.add_argument('--json', action='store_true', help='Output as JSON')
    parser.add_argument('--save', type=str, help='Save keywords to file')
    args = parser.parse_args()

    if args.lookup:
        # Look up specific keyword via IGDB API
        print(f"Looking up keyword via IGDB API: {args.lookup}")
        ids = await get_keyword_ids([args.lookup])
        if ids:
            print(f"Found keyword IDs: {ids}")
        else:
            print("No matching keywords found in IGDB")
        return

    # Fetch all keywords (paginated)
    print(f"Fetching all keywords from IGDB (max {args.max})...")
    print("This may take a moment as IGDB limits 500 per request...\n")
    keywords = await get_all_keywords(max_results=args.max)

    if not keywords:
        print("Failed to fetch keywords. Check your TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET.")
        return

    # Filter by search term if provided
    if args.search:
        search_lower = args.search.lower()
        keywords = [kw for kw in keywords if search_lower in kw['name'].lower()]
        print(f"Filtered to {len(keywords)} keywords matching '{args.search}':\n")

    print(f"Found {len(keywords)} keywords:\n")

    if args.json:
        import json
        output = json.dumps(keywords, indent=2)
        print(output)
        if args.save:
            with open(args.save, 'w') as f:
                f.write(output)
            print(f"\nSaved to {args.save}")
    else:
        # Print in a nice format
        for kw in keywords:
            print(f"  {kw['id']:>6}  {kw['name']}")

        print(f"\n\nTotal: {len(keywords)} keywords")

        if args.save:
            with open(args.save, 'w') as f:
                for kw in keywords:
                    f.write(f"{kw['id']}\t{kw['name']}\n")
            print(f"Saved to {args.save}")

        print("\nUsage examples:")
        print("  python scripts/fetch_igdb_keywords.py --search souls     # Filter list by 'souls'")
        print("  python scripts/fetch_igdb_keywords.py --lookup soulslike # Look up ID via API")
        print("  python scripts/fetch_igdb_keywords.py --save keywords.txt # Save all to file")


if __name__ == "__main__":
    asyncio.run(main())
