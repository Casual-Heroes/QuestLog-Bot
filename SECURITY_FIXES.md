# Security Fixes for WardenBot

## Issue #1: XP Toggle Desync ✅ ALREADY FIXED
**Status**: Already implemented in views.py:3635-3639
- Both `XPConfig.xp_enabled` and `Guild.xp_enabled` are updated in sync
- Bot checks both tables before awarding XP

## Issue #2: Untrusted Content in Logs 🔧 NEEDS FIX
**Priority**: HIGH
**Files to fix**:
- All error logging that includes user content
- Search for: `logger.error.*message.content|logger.error.*payload`

## Issue #3: SSRF / Unvalidated URLs in Embeds 🔧 NEEDS FIX
**Priority**: MEDIUM
**Files to fix**:
- Any code that sets embed.set_image() or embed.set_thumbnail()
- Add URL validation before setting embed images

## Issue #4: AllowedMentions Defaults ✅ TO VERIFY
**Priority**: HIGH
**Action**: Verify all message sends use safe defaults

## Issue #5: Action Queue Trust Boundary 🔧 NEEDS FIX
**Priority**: HIGH
**Files**: cogs/action_processor.py
**Action**: Add payload validation/signing

## Issue #6: Permission Enforcement ✅ TO AUDIT
**Priority**: CRITICAL
**Action**: Audit all API endpoints for @api_auth_required decorator

## Issue #7: Resource Caching / Empty Dropdowns 🔧 NEEDS FIX
**Priority**: MEDIUM
**Files**: views.py - /api/guild/<id>/resources/
**Action**: Add server-side fallback if cache is empty

## Issue #8: Rate Limiting / Abuse Controls 🔧 NEEDS FIX
**Priority**: MEDIUM
**Action**: Add rate limits on send/broadcast actions

## Issue #9: Dependencies ⚠️ NEEDS AUDIT
**Priority**: HIGH
**Action**: Run pip-audit and update requirements.txt

## Issue #10: URL/HTML Injection in Toasts 🔧 NEEDS FIX
**Priority**: MEDIUM
**Files**: templates with toast/alert displays
**Action**: Sanitize error messages before displaying
