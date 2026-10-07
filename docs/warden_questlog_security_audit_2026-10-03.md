# Warden and QuestLog security audit

Date: 2026-10-03

Status: source and production-aggregate review complete; remediation is not yet
deployed.

## Remediation implementation update

Prepared and verified in source:

- Added a live Discord execution-boundary policy for legacy queue actions.
- Preserved current delegated custom-admin roles while rechecking that the
  initiating member still holds the configured role.
- Added age, payload-size, bulk-size, hierarchy, dangerous-role, and permission
  controls; web-authored messages can no longer create mentions.
- Changed queue claiming so no SQLAlchemy session remains open during Discord
  network awaits.
- New deleted-message audit events are metadata-only by default.
- Added and approved a 90-day audit retention policy.
- Added per-community flair receipts and bot-owned role-ID bindings.
- Routed compatibility flair assignment through the owned-role adapter.
- Updated QuestLog's five advisory-affected dependency pins; an isolated
  overlay passes Django checks, JWT encode/decode, protobuf streaming imports,
  and a clean `pip-audit` scan.
- Warden's final pre-commit suite passes 76 tests.

Applied safely to production data/schema without a service restart:

- Added the two empty flair receipt/binding tables.
- Quarantined 249 abandoned `processing` jobs as failed; zero were replayed and
  no rows were deleted.
- Deleted 1,136 audit records older than the approved 90-day window. A
  post-check found zero records beyond the cutoff.

Pending:

- The source changes and patched QuestLog packages are not active in the live
  processes. Production still requires an isolated release/canary and
  controlled restart.
- New deleted-message events will be metadata-only when the prepared Warden
  release is activated. Existing in-window content remains subject to the
  90-day audit cutoff.

## Executive verdict

Warden is not currently showing evidence of an active compromise. The scoped
QuestLog LFG and progression adapters have a sound direction, Warden's internal
HTTP service is bound to localhost, production secrets are not tracked in the
Warden repository, and the Warden dependency scan currently reports no known
vulnerabilities.

The largest risk is architectural: the legacy shared `pending_actions` table is
a broad website-to-Discord control plane. A database row can cause Warden to
change roles, moderate members, send messages or DMs, create Discord resources,
or change local economy data. Warden trusts the row and does not independently
verify that the initiating person still has authority in that Discord community
at execution time. This violates the intended product boundary and creates a
large blast radius if the website, a privileged web session, or the shared
database credential is compromised.

The second major issue is privacy governance. Deleted message content is stored
in audit records and retained indefinitely. That is more collection than the
product needs for normal security evidence and is inconsistent with the stated
goal of monitoring without spying on members.

The remediation should preserve the three communities the owner actively uses
and safely cover all seven current installations. Do not abruptly disable the
queue, reset stuck rows, remove Administrator from a live bot role, or restart
the production bot from the dirty source checkout.

## Scope and method

Reviewed:

- Warden authentication, authorization, permissions, role hierarchy, queues,
  moderation, audit logging, flair, LFG, progression, bridge controls, secrets,
  dependency state, tests, and network binding.
- QuestLog dashboard session authorization, API authentication, queue writers,
  integration adapters, security headers, dependency state, and the site-to-bot
  trust boundary.
- Privacy-preserving production aggregates. No member message content, token,
  secret, or private server conversation was read.
- OWASP ASVS 5.0, OWASP API Security Top 10 2023, OWASP authorization,
  transaction authorization, and logging guidance.

Limitations:

- This was a source/configuration review, not an external penetration test.
- Discord, CDN, reverse-proxy, host firewall, database grants, backup encryption,
  and cloud account settings were not independently penetration-tested.
- Production services were not restarted and no live Discord mutation was sent.
- No historical audit record or stuck queue row was deleted, reset, or replayed.

## Trust boundary

```text
Discord admin browser
        |
        v
QuestLog dashboard session ----> legacy shared pending_actions table
        |                                      |
        |                                      v
        |                              Warden executes Discord mutations
        |
        +----> scoped LFG API --------> Warden LFG adapter
        |
        +----> scoped XP API <-------- Warden progression outbox

Target state:

QuestLog --[separate scoped token per capability/community]--> Warden adapter
Warden   --[least-privilege database credential]-------------> Warden data
Discord admin authorization is checked again at the final mutation boundary.
```

## Findings

| ID | Severity | Finding | Current status |
| --- | --- | --- | --- |
| SEC-01 | Critical | The legacy shared action queue can perform broad Discord mutations without bot-side actor reauthorization or job integrity verification. | Open; retire by capability, not with an abrupt shutdown. |
| SEC-02 | High | Dashboard authorization relies on session guild claims and a 30-minute permission cache; the Discord token validator fails open on network errors. | Open for privileged mutations; acceptable availability tradeoff only for low-risk reads. |
| SEC-03 | High | Queue claiming is not atomic, a SQLAlchemy session is held across Discord awaits, jobs have no lease, and old `processing` rows are never safely recovered. | Open; historical processing rows date back to November 2025. Do not replay automatically. |
| SEC-04 | High | Deleted message content is stored up to 500 characters and audit retention is explicitly unlimited. | Open; 1,649 audit rows exist and 185 contain deleted-message content. Oldest sampled audit date is 2025-12-01. |
| SEC-05 | High | The QuestLog environment reports 22 advisories across five installed packages. PyJWT is used on an authentication path and protobuf is used by live-chat streaming. | Open; fixed versions resolve cleanly in a dry run. |
| SEC-06 | High | Warden and QuestLog share data and legacy direct-write paths beyond LFG, flair, and explicit Unified XP. A compromise can cross product boundaries. | Open; remove after scoped replacements are proven. |
| SEC-07 | Medium | Flair sync is a global shared-table fan-out. Prefix-based cleanup can affect administrator-created `Flair:` roles and one guild failure can repeat changes in other guilds. | Open; replace with per-community deliveries and bot-owned role IDs. |
| SEC-08 | Medium | Legacy message jobs permit user and role mentions, and role templates can request dangerous permissions including Administrator and Manage Roles. | Open; add an explicit allowlist, preview, and final policy gate. |
| SEC-09 | Medium | Full guild member snapshots containing IDs, names, display names, role IDs, avatars, and join dates are copied into a shared cache without a documented retention/minimization policy. | Open; replace broad snapshots with purpose-specific records where practical. |
| SEC-10 | Medium | Production runs from a dirty source checkout and large logs lack confirmed deployed rotation. A restart activates unrelated edits and recovery is harder to prove. | Open; release isolation is a security control, not only an operations improvement. |
| SEC-11 | Medium | Administrator has historically been presented as recommended. It bypasses channel restrictions and makes every bot defect higher impact. | Partially corrected in source; retain a clearly labeled opt-in full-coverage profile, not a default recommendation. |

## Detailed evidence and required controls

### SEC-01: legacy general-purpose action queue

The processor accepts actions for:

- role add/remove and bulk changes;
- XP and token mutations;
- kick, ban, unban, timeout, and warnings;
- channel messages, DMs, and role pings;
- channel topics, role creation, and channel creation;
- discovery, flair, RSS tests, and LFG thread operations.

`triggered_by` is evidence only. Warden does not use it to prove that the actor
is still a member, still has the required permission, outranks the target, and
is authorized for the exact guild, action, and resource. The queue also has no
signature or narrow capability token that binds actor, action, target, payload,
and expiration.

Required target:

1. Stop adding new action types.
2. Classify every current writer as Warden-owned, QuestLog integration, or
   retire.
3. Move Warden-owned dashboard operations to a Warden API with a per-request
   actor and guild authorization check.
4. Give QuestLog only three independent capabilities: LFG delivery, flair
   delivery, and explicit Unified XP progression.
5. At the bot, validate the actor, guild, target, role hierarchy, payload schema,
   expiry, idempotency key, and capability immediately before mutation.
6. Deny unrecognized fields, permissions, roles, mentions, channels, and action
   types by default.

### SEC-02: stale authorization

The dashboard keeps a three-day sliding database session. Guild authorization
is derived from Discord OAuth guild data or configured custom admin roles and
may be cached for 30 minutes. The `discord_required` decorator attempts live
token validation but intentionally allows the request when Discord is
unreachable, while `api_auth_required` relies on session/cache state.

For a normal page read this is reasonable availability behavior. It is not a
sufficient final gate for bans, role changes, channel creation, bulk actions,
or messages. Privileged operations must fail closed when fresh authorization
cannot be established. Particularly destructive operations should require a
short-lived transaction authorization bound to the displayed action and target.

### SEC-03: queue reliability and replay risk

The processor selects up to ten pending rows, marks each row processing, commits,
and then awaits Discord while holding the database session. There is no atomic
claim token, worker lease, lease expiry, or action idempotency key. A crash after
Discord accepts a request but before the completion commit can leave an
ambiguous `processing` row. Automatically resetting historical rows could repeat
DMs, moderation, channel creation, or economy changes.

Required schema for any replacement worker:

- immutable job ID and idempotency key;
- capability, guild, actor, target, payload version, and request digest;
- `available_at`, `claimed_at`, `lease_expires_at`, and claimant ID;
- bounded attempts with retryable/permanent error classification;
- Discord resource receipt IDs;
- terminal result and security audit event;
- atomic claim using database locking or compare-and-set;
- no database transaction/session held across a network await.

Historical jobs must be classified, not blindly replayed. Preserve them as
evidence until a retention and incident policy is approved.

### SEC-04: privacy and retention

Warden's deleted-message listener stores channel reference plus up to 500
characters of content. The daily cleanup task explicitly performs no cleanup.
Warnings can also contain triggering message content. Even when collection is
helpful for moderation, unlimited content retention is not data minimization.

Recommended default policy:

| Data | Default | Admin option | Hard maximum |
| --- | --- | --- | --- |
| Security/audit metadata | 90 days | 30, 90, 180, or 365 days | 365 days unless a legal hold is recorded |
| Deleted-message content | Off | Explicit opt-in with notice | 7 or 30 days |
| Warning evidence content | Redacted excerpt or hash | Short evidence window | 90 days |
| Delivery payload/error body | Metadata and error code | No raw member content | 30 days |
| Product analytics | Aggregate counts only | Opt out | 13 months for trends |
| Member cache | Minimum fields required by an enabled feature | Refreshable | Purge shortly after bot removal |

Do not delete existing rows until the owner approves a policy, export/hold
requirements are checked, and a reversible migration/backup is prepared.

### SEC-05: dependency advisories

`pip-audit` results on 2026-10-03:

- Warden `requirements.txt`: no known vulnerabilities found.
- QuestLog `requirements.txt`: 22 advisories in five packages.

| Package | Installed | Staged target | Observed application relevance |
| --- | ---: | ---: | --- |
| anyio | 4.12.0 | 4.14.2 | Async HTTP/auth dependency path. |
| PyJWT | 2.13.0 | 2.15.0 | Used to issue and decode QuestChat JWTs. Highest priority. |
| protobuf | 6.32.1 | 6.33.5 | Used by YouTube live-chat streaming. |
| urllib3 | 2.7.0 | 2.8.0 | HTTP dependency used through the application stack. |
| soupsieve | 2.8.4 | 2.9.0 | HTML parsing dependency. |

The fixed set resolves together without changing the environment in a pip dry
run. It still requires a staging install, authentication/QuestChat tests,
YouTube streaming tests, outbound HTTP tests, template tests, and a canary
before production deployment.

### SEC-07: flair ownership

The replacement must create one delivery per guild, store a receipt per guild,
and record the exact role ID created or adopted by Warden. Warden may only
remove a role it owns or a role an administrator explicitly mapped. Role names
are presentation, not ownership. Sanitize and bound names; reject permissions;
verify the member is linked and is currently in the target community; and make
delivery idempotent per `(guild, member, equipped_flair_version)`.

### SEC-08 and SEC-11: Discord permissions

Administrator is not required for every Warden feature. It is a convenient
full-coverage mode because it bypasses channel-specific restrictions, but it
also grants far more authority than moderation, role assignment, messaging, or
LFG individually require.

Support two installation profiles without breaking current communities:

- **Named permissions (default):** request only the permissions for enabled
  modules and show missing private-channel overrides in Permission Health.
- **Full coverage (explicit opt-in):** Administrator for owners who knowingly
  want every feature across every channel. Show the blast-radius warning and
  re-check bot role position.

Do not automatically edit any live Discord role. First report exactly which
enabled feature would stop working under named permissions, then let that
community's owner approve the change.

## Controls that are working

- Warden's internal aiohttp API binds to `localhost`, not `0.0.0.0`.
- The real Warden `.env` is ignored and no production private key/certificate
  file was found in tracked files.
- QuestLog production settings use secure, HTTP-only cookies, HSTS, content
  type protections, frame denial, and an enforcing CSP.
- The LFG API stores token hashes, uses constant-time token comparison, enforces
  scopes and expiry, and requires idempotency keys for writes.
- Network LFG delivery validates optional URLs, persists Discord receipts, and
  distinguishes retryable, terminal, and already-finalized callbacks.
- Linked-member Unified XP uses a scoped API and a durable Warden outbox;
  unlinked members remain on local Warden XP until the existing one-time link
  merge.
- Warden's bridge blocks private/local media targets and uses an allowlist for
  remote attachments.
- Warden's 76 local tests pass. They cover permission calculation, malformed
  LFG URLs, retries/idempotency, progression routing, bridge protections, audit
  invite behavior, and product-boundary assertions.

These controls reduce risk, but they do not compensate for the legacy queue or
unlimited message-content retention.

## Secure integration contract

### QuestLog Network LFG

- Community admin explicitly subscribes a guild/channel destination.
- QuestLog sends the canonical group ID, game-aware schema version, minimum
  render fields, action, expiry, and idempotency key using an LFG-only token.
- Warden confirms the token is authorized for that community and destination,
  validates all external URLs, writes Discord once, and returns message/thread
  receipt IDs.
- QuestLog remains roster/lifecycle authority for Network-originated LFG.
- Warden remains authority for Discord-local LFG.

### Flair

- Separate flair-only token and per-community opt-in.
- Delivery is for one linked member, one guild, and one flair version.
- Warden can only manage the explicitly mapped/bot-owned role ID.
- Per-community receipts prevent one guild failure from repeating success in
  every other guild.

### Hybrid Unified XP

- Connection alone never enables Unified XP.
- Linked member plus Unified XP enabled: QuestLog calculates Web XP; Warden
  durably retries evidence and applies Discord roles/rewards from the returned
  level.
- Unlinked member: Warden calculates local XP and local boosts.
- Account link: QuestLog performs the existing one-time idempotent merge.
- QuestLog outage: linked-member events remain queued; Warden does not silently
  create a second local balance.
- Local rate/boost settings remain available in Unified mode but are clearly
  labeled as the unlinked-member fallback.

## Remediation plan that minimizes breakage

### Phase 0: release safety

1. Snapshot the exact source revision, requirements, service units, schema
   version, and current feature flags.
2. Build an immutable Warden release and rollback artifact.
3. Install log rotation and verify it on a non-production log first.
4. Use the test guild as the canary. Do not restart directly from the current
   dirty checkout.

### Phase 1: immediate containment without feature removal

1. Add a bot-side policy gate in front of every legacy queue action.
2. Reject dangerous role permissions, managed roles, roles above Warden,
   unbounded bulk targets, and unapproved mentions.
3. Require an actor for human dashboard mutations and fail closed when fresh
   actor authorization cannot be established.
4. Stop creating new general-purpose queue action types.
5. Add an owner-visible queue health page showing pending, claimed, retryable,
   permanent, and stale jobs without payload content.

### Phase 2: privacy and dependency patch

1. Approve the retention values and content-logging default.
2. Change new deleted-message logging to metadata-only by default.
3. Add scheduled retention enforcement and a legal-hold mechanism.
4. Patch the five QuestLog dependencies in staging and run targeted regression
   tests before a canary release.

### Phase 3: replace integration paths

1. Migrate flair to a scoped per-community API and receipts.
2. Keep the working scoped LFG adapter; remove its shared-table fallback after
   parity and rollback testing.
3. Keep the scoped progression API/outbox; remove legacy direct `web_*` XP
   writes after hybrid behavior is visible in the dashboard.
4. Give Warden and QuestLog separate database credentials and grants.

### Phase 4: retire the queue

1. Inventory and remove website writers one action type at a time.
2. Disable each bot handler only after its replacement is deployed and its
   historical rows are classified.
3. Archive terminal history under the approved retention policy.
4. Quarantine ambiguous `processing` rows. Replay only after manual proof that
   the Discord side effect did not occur.

## Release gates

No security remediation is complete until it has:

- permission and role-hierarchy tests;
- actor/guild/object authorization tests for every mutation;
- payload schema and unexpected-field tests;
- expiry, signature/scope, idempotency, crash, lease, and replay tests;
- privacy/retention tests using synthetic content;
- a canary guild result for each enabled module;
- an authenticated browser test of the dashboard;
- a rollback artifact and verified rollback procedure;
- post-release checks for all seven installations, without reading private
  member content.

## Immediate owner decisions needed before destructive work

1. Choose the default audit metadata retention: recommended 90 days.
2. Choose whether deleted-message content should be off by default: recommended
   yes, with a separate explicit 7- or 30-day opt-in.
3. Confirm that legacy ambiguous `processing` jobs should be quarantined and
   never replayed automatically: recommended yes.
4. Confirm that Full Coverage/Administrator remains an explicit opt-in while
   Named Permissions becomes the normal recommendation: recommended yes.

## 2026-10-07 hardening addendum

The follow-up review remediated the repository-local findings discovered during
implementation:

- self-service role requests and templates now fail closed for privileged,
  managed, default, cross-guild, and out-of-hierarchy roles;
- role approval has an atomic claim and rollback path;
- RSS fetching is HTTPS-only, fails closed on DNS errors, pins validated public
  IPs, preserves TLS hostname validation, and revalidates redirects;
- legacy queued mutations are never automatically replayed once execution may
  have started;
- legacy LFG operations are scoped to their guild, configured channel, thread,
  and group membership, with controlled mentions;
- Warden no longer caches game-server passwords in the shared database and
  never publishes them in channels visible to `@everyone`;
- disabled Soulmask RCON commands have administrator checks if re-enabled.

Post-fix verification:

- 84 unit/regression tests passed;
- Semgrep ran 293 rules on 81 tracked files with 0 findings;
- Bandit reported 0 high and 0 medium findings across 34,675 lines;
- `pip-audit` reported no known vulnerabilities in the locked dependencies;
- `detect-secrets` reported 0 findings;
- Python compilation and `git diff --check` passed.

Deployment work that cannot be completed from the Warden repository alone:

1. Install `logrotate` with host-administrator access and install
   `deploy/logrotate/wardenbot` as `/etc/logrotate.d/wardenbot`.
2. Replace the legacy website-to-bot master bearer token with scoped service
   credentials and cryptographically bound actor identity in a coordinated
   website and bot protocol cutover. The API is loopback-only and rechecks live
   Discord permissions, but possession of the current master token still allows
   a caller to spoof `requester_id`.
3. Complete the separately offered Codex Security installation in the product
   UI; its state was still reported as not installed at the end of this review.

This code review found no evidence of Warden credential compromise. That does
not replace production forensics of Discord, host, database, cloud, and payment
provider audit logs. If independent evidence suggests compromise, rotate all
authoritative credentials from their provider consoles and do not commit the
replacements to this repository.
