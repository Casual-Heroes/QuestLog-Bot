# Warden and QuestLog product engagement audit

Date: 2026-10-03

## Executive verdict

Warden is not short on features. It is short on focus, activation, and a clear
product boundary.

The current bot loads 33 cogs, contains about 44,500 lines of Python, exposes
roughly 165 slash commands and subcommands, and has 99 task/listener decorator
registrations. The Discord dashboard presents more than two dozen destinations.
That breadth makes the product harder to understand, operate, test, and trust.

The recommended position is:

> Warden helps gaming communities organize play, welcome members, and run a
> healthy Discord server.

Warden should be its own product and service. QuestLog should be an optional
integration with three narrow, independently enabled capabilities:

1. Deliver an explicitly shared QuestLog LFG group to an opted-in Discord
   community and receive a delivery result.
2. Deliver an equipped QuestLog flair to an opted-in Discord community when
   the member has linked Discord and the bot is present.
3. Provide authoritative Web XP for a community that explicitly enables
   Unified XP. Linked members use QuestLog Web XP; unlinked members continue
   with Warden XP until account linking performs a one-time merge. Warden
   applies the configured Discord roles and rewards in either case.

QuestLog should not own standalone Warden XP, moderation state, dashboard
actions, creator rotations, nominations, Discord discovery, host service
control, or general Discord administration. Connecting a community to
QuestLog must not enable Unified XP automatically.

## Evidence reviewed

- The running bot connected to 7 Discord guilds at 16:32 on 2026-10-03.
- The database also marks 7 guilds as `bot_present=1`.
- All 33 configured cogs loaded successfully.
- Warden contains 41 cog files and about 44,500 Python lines.
- The web application contains about 235,000 Python lines. Several central
  modules are monoliths: `app/views.py` is about 27,000 lines and
  `views_pages.py` is about 17,000 lines.
- The active Discord dashboard sidebar exposes member, engagement, moderation,
  messaging, bridge, customization, and owner sections in one navigation tree.
- Privacy-preserving production aggregates were reviewed. No message content
  was read.

### Current adoption signal

| Capability | Production signal |
| --- | ---: |
| Connected Discord guilds | 7 |
| XP configurations | 3 guilds |
| Verification configurations | 4 guilds |
| Enabled trackers | 1 guild |
| Guilds with enabled LFG games | 2 guilds |
| QuestLog Network LFG subscribers | 1 guild |
| Live-alert subscriptions | 1 guild |
| Discord guilds with an enabled bridge | 1 guild |
| Enabled RSS feeds | 0 |
| Reaction-role records | 0 |
| Suggestion records | 0 |
| Stored discovered games | 921 |
| Guilds with game discovery enabled | 1 |

There are 11 enabled welcome records but only 7 connected guilds, which shows
that retained configuration and current installation state are not presented
as separate concepts consistently.

The general website-to-bot action queue contains 433 historical records, but
no actions were recorded in the last 30 days. This is a large, privileged
integration surface with no current adoption evidence.

Recent audit activity is modest and mostly operational: message deletion,
verification, role changes, joins, and leaves. The evidence supports focusing
on reliable core workflows rather than continuing to add broad feature area.

## Immediate defects found

### Repaired in source

- The QuestLog Network sidebar URL omitted the slash after the domain in two
  locations. Both now resolve through the canonical `/network/` route.
- XP and Live Alerts returned HTTP 500 because CSP style output appeared before
  Django's required first `{% extends %}` tag.
- The same template compiler defect existed in Templates, RSS Feeds, and the
  retired Discovery Network template.
- All 105 application templates now compile successfully.
- XP and Live Alerts both render HTTP 200 using a synthetic authorized request
  against configured production data.
- Additional malformed `casual-heroes.com...` links were corrected in profile,
  creator, member portal, LFG, Fluxer, and discovery surfaces.
- Marketing copy that claimed every bot feature had a matching dashboard was
  narrowed to supported features.

Authenticated browser verification is still required. Python source changes
will require a controlled web-service reload, while corrected templates are
normally read at render time.

### Other operational findings

- The bot log is over 400 MB.
- The Gunicorn access log is approximately 5 GB.
- The Stream Arena access log is approximately 2.8 GB.
- A Palworld watchdog currently emits a failed instance-selection traceback
  about every six seconds because `HeroesofPalpagos02` does not uniquely match
  the available `CH-HeroesofPalpagos02` instance.
- The website contains duplicate top-level view definitions, including
  `bot_discord`, `bot_fluxer`, several Fluxer LFG moderation APIs, and discovery
  helpers. Later definitions silently replace earlier definitions.
- The public Discord bot template contains duplicate Dashboard and Connect to
  QuestLog callouts.
- The source tree is the production runtime. A service restart loads every
  uncommitted change in that directory, which prevents safe, isolated releases.

These are reliability and governance problems, not cosmetic cleanup.

## Recommended product boundary

### Warden owns

- Discord installation, permissions, and role-hierarchy health.
- Discord-native moderation, verification, audit, welcome, roles, channels,
  messages, and configuration.
- Discord-local XP, boosts, balances, and levels when Unified XP is disabled,
  and for unlinked members while a community is in hybrid Unified mode.
- Discord role and reward fulfillment in both XP modes.
- Discord-local game-aware LFG creation, RSVP, reminders, attendance, and
  delivery into Discord channels and threads.
- Discord-local live alerts, scheduled messages, raffles, and trackers when
  implemented as optional Warden modules.
- Warden's dashboard, database schema, audit trail, health status, and API.
- Delivery receipts, retries, idempotency, and visible errors for actions Warden
  performs in Discord.

### QuestLog owns

- Player accounts, profiles, Journeys, builds, posts, communities, and public
  discovery.
- QuestLog flairs and the choice to equip one.
- QuestLog's public/network LFG identity and web lifecycle.
- Web XP rules, boosts, balances, levels, and ledger for linked members when a
  connected community explicitly enables Unified XP. Discord XP is not
  automatically QuestLog progression merely because the community is
  connected.

### The only supported integration contracts

#### Flair delivery

- QuestLog creates a scoped, per-community delivery.
- Warden verifies that the bot is installed and flair sync is enabled.
- Warden verifies the linked Discord member is currently in that guild.
- Warden creates or assigns only the configured flair role.
- Warden acknowledges success, retryable failure, or permanent failure.
- No shared database credential is used.

#### LFG delivery

- A member explicitly chooses to share an LFG group to a Discord community, or
  an admin explicitly subscribes a destination channel.
- Warden receives the smallest necessary, game-aware payload through a scoped
  API: canonical group ID, game and activity, schedule, capacity, and any
  game-specific role/class/experience slots needed to render the Discord card.
- Warden validates the target against the subscribed guild and channel.
- Warden returns message/thread identifiers and delivery status.
- Roster and lifecycle authority remain singular. Warden must not maintain a
  second conflicting network roster.

The current `network_broadcasts` receiver was reviewed. It claims QuestLog
delivery jobs, validates optional URLs, creates or updates the Discord message
and thread, persists a receipt/binding, and acknowledges the result. Its tests
cover malformed thumbnail URLs, callback conflicts, retries, terminal receipts,
message/thread creation, deletion, and missing destinations or permissions.
This is the right adapter shape; the remaining work is to remove shared-table
fallbacks and make the game-specific presentation contract explicit and
versioned.

#### Unified Web XP

- A connected community admin explicitly enables Unified XP. Connection alone
  enables no XP transfer.
- For linked members, Warden durably queues deduplicated activity evidence,
  such as a Discord message, reaction, voice interval, or game session. It does
  not calculate that member's authoritative award.
- For unlinked members, Warden applies its local XP rules and boosts. These
  events do not enter the Web XP retry queue.
- QuestLog applies the Web XP rules and boosts once for linked members, records
  the canonical ledger entry, and returns the current XP and level.
- When a member links Discord, QuestLog performs a one-time, idempotent merge:
  it imports the highest eligible Warden XP balance, preserves local Hero
  Tokens as Hero Points, creates the unified leaderboard entry, and clears the
  migrated local balances in the same transaction.
- Warden mirrors that state for Discord display and applies only the Discord
  roles, notifications, or other rewards configured by that server's admin.
- The dashboard labels Warden rates and boosts as the "Unlinked member
  fallback" while Unified XP is enabled. QuestLog controls remain authoritative
  for linked members; Warden controls remain available only for the unlinked
  lane.
- If QuestLog is temporarily unavailable, Warden queues the evidence and
  retries it idempotently. It must not silently award local XP, which would
  create two balances and possible double credit.

### XP authority modes

| Community state | XP authority | Boost/rate controls | Discord roles and rewards |
| --- | --- | --- | --- |
| Not connected to QuestLog | Warden | Warden dashboard | Warden applies the server's configuration |
| Connected, Unified XP off | Warden | Warden dashboard | Warden applies the server's configuration |
| Connected, Unified XP on; member linked | QuestLog Web XP | QuestLog community settings | Warden applies the server's configuration from QuestLog's returned level |
| Connected, Unified XP on; member unlinked | Warden until one-time account-link merge | Warden's clearly labeled unlinked-member fallback settings | Warden applies the server's local level configuration |

LFG, flair, and Unified XP are separate switches. A community can receive
QuestLog Network LFG groups or flair updates while keeping Discord-local XP.

The current implementation routes through `site_xp_to_guild`: Web progression
is selected only for an active, approved Discord community with that flag
enabled and a linked member identity. The scoped progression API returns the
authoritative XP and level. Warden-local progression remains active for
unlinked members, and the account-link flow already contains the transactional
one-time merge. This review added a durable, idempotent progression outbox,
per-member authority routing, a mixed leaderboard, and correct profile mode
labeling. Remaining debt is the disabled legacy direct `web_*` XP-write
fallback and the dashboard presentation of the two member lanes.

### Explicitly outside the integration boundary

- General website action queues that can kick, ban, modify roles, send DMs,
  create channels, or change XP.
- QuestLog progression events from communities that have not explicitly
  enabled Unified XP.
- Direct Warden writes to `web_users`, `web_xp_events`, `web_legacy_events`,
  nominations, creator rotations, or community counters.
- Importing the QuestLog Django runtime into Warden.
- Writing JSON into the QuestLog repository.
- Discord commands that start, stop, or place host services into maintenance.
- Direct reads of QuestLog static files from `/srv/ch-webserver`.

## Feature-by-feature disposition

| Area | Current condition | Decision |
| --- | --- | --- |
| Core/help/setup | Help is incomplete and stale. `/questlog setup` says it is a wizard but contains a TODO and only displays instructions. | Rebuild as Warden onboarding and permission health. Preserve old command aliases during migration. |
| Installation permissions | Administrator was used for full channel visibility. Named permissions now exist, but private-channel overrides still matter. | Keep named-permission default and an explicit full-coverage option. Show a permission preflight after install. |
| Anti-raid/security | Useful safety layer, but generic and partly overlaps Discord native controls. | Keep as a core Warden safety module. Lead with health, evidence, and recovery rather than fear. |
| Moderation | Broad command set with warnings, timeout, jail, kick, and ban. | Keep, but simplify default UI. Hide advanced jail/mass actions until enabled. |
| Verification | Substantial implementation and some real usage. | Keep. Provide one recommended setup path and a test/recovery check. |
| Audit | Useful operational evidence with active recent records. | Keep. Add retention controls and a clear distinction between Discord audit data and Warden events. |
| Welcome | Common community need and configuration exists beyond current installations. | Keep. Combine welcome, auto-role, rules, and first game-role selection into onboarding. |
| Roles/reaction roles | Large administrative surface; reaction-role adoption is currently zero. | Keep role health and self-role basics. Park templates, mass operations, and advanced reaction modes behind an optional Server Tools module. |
| Channels/templates | Powerful but dangerous bulk operations increase support and permission complexity. | Optional Server Tools module, disabled by default. Require preview and confirmation for destructive actions. |
| XP/levels | Familiar engagement mechanic, configured in three guilds, with hybrid per-member routing, a one-time link merge, and legacy direct-write debt. | Keep standalone Warden XP or opt-in Unified XP. In Unified mode, QuestLog owns linked-member calculations and Warden owns unlinked-member calculations until merge. Warden always fulfills Discord rewards. Never silently switch an already-linked member to local XP during an outage. |
| LFG/events | The strongest gaming-specific capability, but split between local tables, canonical APIs, Network broadcasts, old URLs, and duplicate lifecycle code. | Make this Warden's flagship. Warden owns standalone Discord LFG; QuestLog owns Network-originated LFG while Warden renders and operates the Discord delivery. Use one canonical group and roster per journey. |
| Attendance/reliability | Valuable for gaming groups but can feel punitive if presented as a score. | Keep as organizer evidence. Use attendance history and private organizer notes; avoid public shame mechanics and provide pardon/context. |
| Game discovery | 921 stored games serve one enabled guild. The cog is over 4,000 lines and runs many periodic jobs. | Sunset automatic broad discovery as a default feature. Replace with simple game-role onboarding and admin-curated game interests. |
| Creator promo/COTW/COTM | Mixes Warden, QuestLog profiles, tokens, and cross-site creator systems. | Remove from default Warden. If retained, make it a separate optional Creator module owned by Warden data. |
| Nominations/Legacy | Directly writes QuestLog social and economy tables. | Remove from Warden default and migrate any desired recognition workflow to QuestLog. |
| Raffles | Four historical records and no evidence it drives core activation. | Optional module. Do not place it in primary onboarding or navigation. |
| Suggestions | No production records. | Sunset or park until a community requests it. Discord forums already cover many suggestion workflows. |
| RSS | Zero enabled production feeds and a substantial implementation. | Park outside the default product. Consider a supported integration pack only if demand appears. |
| Live alerts | One subscription. Current dashboard data is QuestLog-owned. | Keep only if subscriptions move into Warden ownership. Otherwise remove from Warden and expose it as a separate integration. |
| Scheduled messages | One record. Useful but not differentiating. | Optional Content module. |
| Trackers/channel directory | One enabled tracker. Potentially useful for server health and game-role counts. | Keep as an optional Community Ops module and make setup outcome-driven. |
| FFXIV timers | Game-specific value, but reads a static file from the QuestLog repository. | Convert into an optional Warden game pack with packaged/versioned data. Do not couple to QuestLog filesystem paths. |
| Game-server status | Useful for hosted gaming communities, but AMP behavior and live logs are operationally sensitive. | Separate Game Server module with its own permission and credential boundary. Disable for ordinary guilds. |
| Chat bridge | High complexity and risk, used by one Discord guild, and not core gaming administration. | Move to a separate integration service/product. Keep disabled by default in Warden. |
| Flair store | Warden had a second local store while QuestLog has the player flair source. | Remove local store. Retain only scoped QuestLog-to-Warden flair delivery. |
| QuestLog progression adapter | Sends linked-member evidence through a scoped API, applies the returned level, and now persists retryable work in a durable idempotent outbox. | Keep for explicit Unified XP. Remove direct database fallbacks and expose outbox and per-member authority health in the dashboard. |
| Network broadcasts | Valuable only as the scoped QuestLog LFG adapter. | Keep a much smaller claim, deliver, acknowledge implementation. Remove direct shared-table fallbacks. |
| Site activity exporter | Writes into the website repository every 30 seconds. | Replace with a read API or Warden-owned metric endpoint, then delete. |
| Invite/early access | Creates QuestLog access codes from a Discord command. | Move to QuestLog. It is not a Warden function. |
| Emergency service control | Gives Discord an indirect path to host service control. | Delete from the normal product. Host incident response belongs on the host. |

## Where the experience is overcomplicated

### 1. The bot and site have two identities

The repository and owner language call it Warden, while the Discord account,
commands, help copy, website, and dashboard call it QuestLog. Users cannot tell
whether they installed an independent community bot or a QuestLog client.

Recommended migration:

- Brand the bot and dashboard as Warden.
- Introduce `/warden` as the main command group.
- Keep `/questlog` as a compatibility alias with a deprecation message.
- Present QuestLog under Integrations with separate LFG, Flair, and Unified XP
  permissions and health.

### 2. The dashboard is a feature inventory, not an admin workflow

The sidebar exposes almost every implementation detail. Member pages and admin
configuration compete for attention. Features remain visible even when unused.

Recommended primary navigation:

1. Overview
2. Setup and Health
3. Play: LFG and Events
4. Members: Welcome, Roles, and XP
5. Safety: Moderation, Verification, and Audit
6. Content: Alerts and Scheduled Messages
7. Integrations
8. Settings

Only activated modules should expand into detailed pages.

### 3. There are multiple control planes

The site writes shared tables, Warden polls them, Warden calls its own localhost
API, several loops resync the same guild state, and other cogs call QuestLog
APIs. This makes ownership and failure recovery unclear.

Recommended control plane:

- The Warden dashboard calls a Warden API.
- Warden stores Warden state using a Warden database credential.
- Discord jobs use a transactional outbox with claim, lease, retry, result,
  actor, and idempotency fields.
- QuestLog receives no general Discord mutation scope.
- QuestLog LFG, flair, and progression tokens are separate and narrowly
  scoped.

### 4. Background work is fragmented

Warden contains frequent 2-second, 3-second, 5-second, 10-second, 30-second,
60-second, and periodic polling loops. Some perform synchronous database work
inside the Discord event loop. Three different mechanisms participate in guild
sync, including a loopback HTTP call to Warden itself.

Recommended change:

- One scheduler/worker abstraction.
- Event-driven updates where Discord already emits an event.
- Shared bounded job queues for retries and backpressure.
- No SQLAlchemy session held across awaited Discord calls.
- One guild state synchronizer with explicit freshness.

### 5. Deployment is not isolated

Production executes directly from a dirty source checkout. Code review,
rollback, and restarts are therefore coupled to every unrelated edit.

Recommended change:

- Build immutable release directories or containers.
- Pin dependencies in a release artifact.
- Run database migrations separately.
- Atomically switch the service to a release path.
- Keep the previous release and environment for one-command rollback.
- Never make an active checkout the production artifact.

## What Warden lacks

### A real first-run experience

The current setup command is not a wizard. A strong first run should:

1. Explain what Warden will and will not access.
2. Run a permission and role-hierarchy preflight.
3. Ask the admin to choose one outcome: organize play, welcome members, or
   strengthen safety.
4. Configure the minimum channels and roles for that outcome.
5. Perform a harmless test.
6. Show a completed health checklist and the next useful action.

### A permission and delivery health center

Admins need one page showing:

- Bot presence and current role position.
- Missing named permissions.
- Private channels Warden cannot see when a feature targets them.
- Deleted or renamed configured roles/channels.
- Last successful job per enabled module.
- Recent retryable and permanent delivery failures.
- A safe test button for each enabled destination.

The missing FFXIV role that currently forces the activity count to zero should
be visible and repairable there rather than existing only in logs.

### Feature-level activation and observability

Warden currently knows implementation state but not whether a community found
value. Add privacy-preserving product events such as:

- Setup started/completed.
- First LFG created.
- First member joined an LFG.
- Reminder delivered.
- Event completed and attendance recorded.
- Verification flow tested/completed.
- Permission health degraded/repaired.
- Module enabled/disabled.

Do not store message content for product analytics.

### A coherent game event experience

Focused event products currently emphasize fast creation, signups, reminders,
timezones, recurring events, capacity, waitlists, threads, and calendar sync.
Warden implements pieces of this but presents them across commands, a web
browser, local groups, network groups, attendance, and calendar pages.

The winning Warden loop should be:

> Pick a game, choose an activity and time, publish once, fill the group,
> remind participants, run the session, capture attendance, and make the next
> session easy.

Priority gaps:

- One progressive event creator in Discord and web.
- Automatic timezone display.
- Capacity and waitlist promotion.
- Recurring sessions.
- Discord Scheduled Event synchronization.
- Calendar subscription/export.
- Role/experience slots appropriate to the selected game.
- Reminder and quiet-hour controls.
- Post-session follow-up and one-click repeat.

### Ethical engagement, not artificial urgency

Engagement should help people play together and administer safely. Avoid
manufactured scarcity, constant pings, public shame, or rewards that require
daily checking.

Use:

- Opt-in reminders and digests.
- Quiet hours and frequency controls.
- Honest capacity and schedule information.
- Private organizer attendance context.
- Weekly community pulse based on aggregate activity.
- Clear reasons for every notification and an immediate opt-out.

## Competitive position

Broad moderation and leveling are already established categories. MEE6
positions around moderation, leveling, and content notifications. Carl-bot
positions around modular reaction roles, automod, logging, and custom commands.
Warden should meet the safety baseline but should not lead with "all in one."

Focused event products are clearer. Apollo describes scheduling, signups,
reminders, recurring events, waitlists, event threads, restrictions, and
calendar sync. Sesh emphasizes simple creation, RSVP, timezone conversion,
polls, calendar links, recurring events, and attendee export.

Warden's credible differentiation is game-aware community operations:

- LFG schemas that understand the selected game.
- Discord roles and channels connected to game interests.
- Reliable session reminders and attendance.
- Admin health for the places where gaming communities actually organize.
- Optional QuestLog group discovery without making QuestLog mandatory.

Official comparison sources reviewed:

- https://mee6.xyz/en/features
- https://carl.gg/
- https://apollo.fyi/
- https://sesh.fyi/

## Recommended dashboard home

The first screen should answer four questions:

1. Is Warden healthy?
2. What is enabled?
3. What needs attention?
4. What is the next useful action?

Suggested cards:

- Health: permissions, role hierarchy, missing destinations, retries.
- This week: groups created, unique participants, attendance, verifications,
  moderation events. Aggregate only.
- Quick actions: create group, test welcome, review safety, post announcement.
- Setup progress: no more than three recommended next steps.
- Integrations: QuestLog LFG, Flair, and Unified XP shown as separate opt-ins
  with authority labels and last delivery status.

## Roadmap

### P0: stabilize before adding features

- Deploy the repaired templates and links through a controlled release.
- Add a template compilation test to CI.
- Fix the missing FFXIV role mapping.
- Stop the Palworld watchdog error loop or disable the sunset service.
- Install log rotation for Warden and the website logs.
- Produce a release artifact and rollback path before the next service restart.
- Reconcile the 7 connected guilds against the owner's expected 3 active
  communities and classify each as expected, test, authorized external, or
  remove-requested.

### P1: establish the product boundary

- Rename the user-facing product to Warden while preserving command aliases.
- Write and publish a Warden data/permissions page.
- Split Warden dashboard navigation from QuestLog player navigation.
- Preserve the scoped progression API for explicit Unified XP, remove legacy
  direct database XP writes, and make the active XP authority unmistakable.
- Stop new general website action-queue writes.
- Define scoped LFG, flair, and progression contracts.
- Add module flags and hide inactive modules from primary navigation.

### P2: make first value obvious

- Replace the setup TODO with a real onboarding flow.
- Ship the permission and delivery health center.
- Instrument the activation funnel without message content.
- Make one recommended template for a gaming community:
  Welcome plus game roles plus LFG channel plus safety baseline.
- Rewrite help and the public bot page around outcomes, not inventory.

### P3: make LFG the flagship

- Consolidate local and network LFG lifecycle behavior.
- Ship progressive creation, reminders, recurring sessions, waitlists,
  calendar integration, and post-session repeat.
- Preserve game-specific schemas without forcing admins through every option.
- Add aggregate organizer insights and delivery health.

### P4: remove compatibility debt

- Move Warden to a separate database credential/schema.
- Remove direct `web_*` writes and `/srv/ch-webserver` filesystem reads.
- Delete retired streaming, flair-store, legacy direct-XP-write, legacy,
  nomination, site-export, and emergency-control code after migrations are
  verified. Retain the scoped Unified XP adapter.
- Move the bridge and game-server operations into explicitly installed modules
  or separate services.
- Split monolithic cogs and Django view modules along product boundaries.

## Success measures

Avoid server-count and command-count vanity metrics. Measure:

- Installation to completed permission preflight.
- Installation to first configured outcome.
- Median time to first published LFG.
- Percentage of new guilds with one successful member interaction in 24 hours.
- Weekly active guilds by meaningful action.
- LFG fill rate, attendance rate, and repeat-session rate.
- Verification completion and recovery rate.
- Delivery success and retry age by integration.
- D7 and D30 guild retention.
- Feature disable and bot removal reasons.
- Privacy export/deletion completion time.

## Release gates

No pivot phase is complete until it has:

- Automated tests for the affected routes, templates, and adapters.
- A configured-database read test without personal-content inspection.
- Permission and role-hierarchy tests.
- Idempotency and retry tests for LFG, flair, and progression delivery.
- A canary guild verification checklist.
- A rollback artifact and command.
- Owner-visible documentation of data, permissions, and notification behavior.
- Post-deployment verification of the real authenticated dashboard.

## Final recommendation

Do not add another major Warden feature until setup, health, LFG, and the
QuestLog boundary are coherent. The current product can become competitive by
removing cognitive load and making its best gaming workflow excellent. More
surface area would make the present problem worse.
