# Warden product boundary and cleanup review

> Superseded for future product direction by
> `docs/warden_questlog_engagement_audit_2026-10-03.md`. The earlier review
> preserved unified QuestLog progression as a migration target. The confirmed
> product boundary is now three independent opt-ins: Network LFG delivery,
> flair delivery, and Unified Web XP. Connecting a Discord community does not
> enable Unified XP by itself. Within Unified XP, linked members use QuestLog
> Web XP while unlinked members retain Warden XP until the account-link flow
> performs its one-time merge.
> Existing production behavior remains compatibility state until a controlled
> migration is approved and deployed.

Reviewed against the QuestLog site on 2026-08-15.

## Product boundary

QuestLog owns durable product state and cross-platform rules:

- accounts, linked identities, profiles, flairs, XP, and economy
- game catalog searches and IGDB metadata
- LFG identity, roster, capacity, lifecycle, and delivery jobs
- community configuration, discovery records, schedules, and owner-visible health

Warden owns Discord execution:

- Discord permissions and interaction acknowledgements
- gateway events, moderation actions, roles, channels, threads, embeds, and buttons
- translating Discord outcomes into canonical QuestLog delivery results
- small bot-local state needed for rate limits, deduplication, and recovery

The bot must use scoped QuestLog APIs when one exists. Direct writes to a
`web_*` table are compatibility debt and must not be introduced for new work.

## Decisions applied

| Surface | Decision | Reason |
| --- | --- | --- |
| `cogs.live_alerts` | Keep as adapter | It delivers dashboard-managed subscriptions to Discord. |
| `cogs.streaming_monitor` | Disabled by default | It duplicates live alerts and imports the website Django runtime into Warden. |
| `cogs.flair_sync` | Keep as adapter | QuestLog owns flair selection; Warden mirrors the selected role. |
| `cogs.flair_cog` | Disabled by default | It is a second bot-local flair store and token economy. |
| VIP owner commands | Removed | The bot is free and the marker no longer controls a product capability. |
| `/purgelfgs` | Removed | It mutates legacy local LFG state outside the canonical lifecycle. |
| Discord emergency service control | Disabled by default | A compromised Discord owner account must not normally grant `sudo systemctl` access to the website and Matrix. |
| `cogs.site_activity_tracker` | Compatibility flag, currently enabled | It still feeds an existing site view and writes into the website repository. Its blocking database and file work now runs off the Discord event loop. Disable only after its API replacement ships. |
| Multi-step verification | Keep, free | The stale paid gate called an undefined helper. |
| Mass role operations | Keep, free | The stale tier message referenced an undefined limit. |
| Moderation log | Keep, free | All communities receive the same 30-day query window. |
| Discord embeds and message composition | Keep | The site does not replace Warden's Discord embed, message, edit, broadcast, or delivery workflows. |
| Verification commands | Keep | Account verification, setup, recovery, and quarantine permissions are Discord-native responsibilities. |
| Tracker configuration | Keep | Warden owns tracker configuration and Discord channel-topic updates until a proven end-to-end replacement exists. |
| Channel and role administration | Keep | Channel management, bulk operations, templates, reaction roles, and permission audits execute against Discord. |
| Audit commands | Keep | Warden collects Discord events and provides operational search, export, statistics, and configuration. |
| LFG game setup commands | Keep | Warden's Discord setup uses the existing IGDB search and metadata flow, including covers and platform data. |
| Discovery, nominations, XP, and welcome commands | Keep | These Discord workflows remain active; related site pages are complementary, not replacements. |

The compatibility files and database columns have not been deleted. The
legacy cogs can be enabled temporarily with explicit environment flags while an
old deployment is migrated:

- `ENABLE_LEGACY_STREAMING_MONITOR=true`
- `ENABLE_LEGACY_DISCORD_FLAIR_STORE=true`
- `ENABLE_LEGACY_SITE_ACTIVITY_EXPORT=true`

Discord feature commands must not be retired merely because the site has a
related route, template, API, or data model. Retirement requires a verified,
end-to-end replacement that can perform the same Discord operation, followed
by an explicit owner decision.

The separate `ENABLE_EMERGENCY_SERVICE_CONTROL=true` break-glass flag is not a
migration switch. Enabling it makes the configured bot owner account
root-equivalent for the allowlisted host services and should be exceptional.

## Security and reliability changes

- Security, verification, audit, moderation, and action-processing cogs now
  fail closed at startup. Warden exits if one fails to load.
- The localhost control API requires a token of at least 32 characters, parses
  the Bearer scheme strictly, compares secrets in constant time, and limits
  request bodies to 64 KiB.
- Failed flair role mutations remain pending and retry instead of being marked
  processed before Discord succeeds.
- RSS fetching already rejects private, loopback, link-local, reserved, and
  metadata addresses and validates every redirect.
- The unused Django OAuth helper was removed from the bot repository.
- Stale package exports for removed subscription models and feature limits were removed.
- Discord-triggered host service control is disabled by default.
- Blocking nomination HTTP and database work now runs outside the Discord event loop.
- `pip-audit -r requirements.txt` reported no known vulnerable dependencies at
  review time.

## Remaining boundary debt

These active paths still access site-owned tables directly and should move to
scoped APIs or claimed delivery jobs in later migrations:

- XP compatibility writes in `cogs/xp.py`
- community count updates in `cogs/welcome.py`
- early-access code creation in `cogs/invite.py`
- nominations and legacy score writes
- live-alert subscription state and flair delivery queue state
- some network broadcast configuration and message bindings

The LFG and progression adapters already provide controlled migration flags.
Keep compatibility writes enabled only through their observation windows, then
remove their direct SQL fallbacks after production traffic proves the API paths.

## Next review wave

1. Add canonical delivery endpoints for flair and live alerts with claim,
   retry, acknowledgement, and owner-visible errors.
2. Move invite codes, nominations, and legacy score events behind narrow API
   scopes.
3. Consolidate startup, periodic, event-driven, and dashboard guild syncing
   into one Warden service instead of repeated logic and loopback HTTP calls.
4. Replace the shared website database credential with adapter-specific API
   credentials once the final direct readers are migrated.
5. After the migration window, delete disabled compatibility cogs and remove
   deprecated VIP columns in a coordinated database migration.
