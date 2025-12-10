#!/usr/bin/env python3
"""Test script to verify ActionType enum has BOOST_EVENT_START"""

import sys
sys.path.insert(0, '/mnt/gamestoreage2/DiscordBots/wardenbot')

from models import ActionType

print("=" * 60)
print("Testing ActionType Enum")
print("=" * 60)

# List all enum values
print("\nAll ActionType values:")
for action in ActionType:
    print(f"  - {action.name} = '{action.value}'")

# Check if BOOST_EVENT_START exists
print("\n" + "=" * 60)
if hasattr(ActionType, 'BOOST_EVENT_START'):
    print("✅ BOOST_EVENT_START exists!")
    print(f"   Value: '{ActionType.BOOST_EVENT_START.value}'")
else:
    print("❌ BOOST_EVENT_START does NOT exist!")

# Try to get it from string value
print("\n" + "=" * 60)
try:
    from_string = ActionType('boost_event_start')
    print(f"✅ Can create from string 'boost_event_start': {from_string}")
except ValueError as e:
    print(f"❌ Cannot create from string 'boost_event_start': {e}")

print("=" * 60)
