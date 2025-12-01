# warden/cogs/__init__.py
"""
Warden Bot Cogs

Cog loading order matters for dependencies:
1. core - Base events, guild sync
2. security - Anti-raid, lockdown
3. verification - Member verification
4. audit - Audit logging
5. xp - XP & leveling
6. roles - React-to-role, level roles
7. discovery - Self-promo, featured pool (premium)
8. admin - Admin commands, settings
"""
