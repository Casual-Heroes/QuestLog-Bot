# Warden canonical LFG adapter

QuestLog is the source of truth for LFG identity, validation, roster, roles,
capacity, schedule, lifecycle, authorization, idempotency, and delivery health.
Warden retains only Discord account resolution, Discord permission checks,
interactions, threads, embeds, buttons, Scheduled Events, and delivery results.

## Configuration

Create a QuestLog service token with these scopes and store it only in Warden's
secret environment:

```text
lfg:read lfg:write lfg:act-as
```

The adapter uses these variables:

```text
QUESTLOG_LFG_API_URL=https://questlog.casual-heroes.com/api/v1/lfg
QUESTLOG_LFG_API_TOKEN=qlp_<one-time service token>
LFG_CANONICAL_API_ENABLED=false
LFG_LEGACY_WRITES_ENABLED=true
```

The create command prints two different values. `Client: qlc_...` is the
non-secret identifier used for listing or revoking the client. Warden must be
given `Token: qlp_...`, which is displayed only once. Store only the `qlp_...`
value. Do not include `client:`, `Token:`, quotes, or spaces.

Apply `migrations/canonical_lfg_adapter.sql` before enabling the canonical API.
The migration adds the canonical group binding and the durable delivery receipt
table. Do not remove the legacy LFG tables during the compatibility period.

## Controlled rollout

1. Deploy with the canonical API disabled and legacy writes enabled. Confirm the
   existing bot behavior is unchanged.
2. Apply the database migration and configure the scoped service token.
3. Enable `LFG_CANONICAL_API_ENABLED`. Keep `LFG_LEGACY_WRITES_ENABLED` enabled
   while old persistent Discord components and bot-local groups remain active.
4. Verify that new Discord groups receive a canonical ID and share token, and
   that create, join, leave, member update, and lifecycle actions appear once in
   QuestLog.
5. Observe duplicate interactions, bot restarts, retry delivery, full group
   reopen, and channel deletion behavior.
6. Stop legacy writes only after persistent views and every retained read path
   can hydrate canonical groups and rosters from the API. The current flag is a
   safety switch for that future cutover, not approval to remove the tables.
7. Remove browse-time merging and compatibility tables only after the observation
   window proves that no active command or component depends on them.

## Delivery contract

Canonical delivery work is handled at least once. Warden claims each
`delivery_job_id`, performs the Discord request, stores the result, and then calls
the supplied callback. Queue rows remain until QuestLog accepts that callback.
A replay of a completed job reports the stored result without sending another
Discord message.

Delivery job IDs are immutable attempts. A terminal result, including `failed`,
is replayed but never changed or resent under the same ID. QuestLog must issue a
new `delivery_job_id` when retrying a failed attempt. If the callback reports an
idempotency conflict, Warden treats the remote job as already finalized and
removes the duplicate queue row instead of retrying it indefinitely.

Reported outcomes are `delivered`, `retrying`, `permission_missing`,
`destination_missing`, and `failed`. The receipt also records the Discord guild,
channel, message, and thread IDs when available.

## Command ownership inventory

No command is removed during the compatibility window.

| Command | Decision | Long-term owner or action |
| --- | --- | --- |
| `/lfg` | Keep | Discord create surface backed by the canonical API |
| `/lfg_search`, `/lfg_add`, `/lfg_custom`, `/lfg_options`, `/lfg_rank`, `/lfg_remove` | Admin-only | Discord game/menu configuration |
| `/lfg_list`, `/lfg_setup` | Admin-only | Discord menu inventory and publishing |
| `/lfg_mark`, `/lfg_blacklist`, `/lfg_config` | Admin-only | Keep temporarily; move roster policy to canonical API before cutover |
| `/lfg_stats`, `/lfg_leaderboard` | Keep temporarily | Replace local calculations with canonical read endpoints |
| `/lfg_calendar`, `/lfg_groups` | Merge | One canonical browse command with calendar and list views |
| `/lfg_join`, `/lfg_leave` | Retire after migration | Buttons and canonical browse should own member actions |
| `/lfg_delete` | Merge | Use `/lfg_status ... cancelled` and retain the alias during migration |
| `/lfg_status` | Keep | Canonical start, complete, and cancel transitions |
| `/lfg_discord`, `/lfg_ql` | Merge | One QuestLog browser/link command |
| `/questlog-network setup`, `status`, `disable` | Admin-only | Discord destination subscription and health |

## Required verification matrix

- Bot not installed or guild unavailable: report `destination_missing`.
- User not linked: reject canonical mutations before the Discord side effect.
- Channel or thread missing: report `destination_missing`.
- Missing send, thread, or message permissions: report `permission_missing`.
- Rate limit and transient HTTP errors: report `retrying` and keep queue work.
- Duplicate interaction: reuse the Discord interaction ID as the idempotency key.
- Duplicate delivery: reuse the stored delivery receipt without another send.
- Restart after claim: reclaim stale processing receipts; preserve completed ones.
- Full group, leave, and reopen: QuestLog decides capacity and state.
- Edit, start, complete, cancel, and channel deletion: use canonical lifecycle
  endpoints and record precise Discord delivery outcomes.
- Website, Discord, and Fluxer: verify all surfaces resolve the same canonical ID.
