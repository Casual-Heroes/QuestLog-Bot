# Signed Warden API authentication migration

Warden and QuestLog now support scoped Ed25519 request signatures. Each
signature binds the key ID, timestamp, unique nonce, scope, Discord actor,
HTTP method, route, and SHA-256 digest of the exact request body. Warden keeps
only public keys and rejects stale or replayed requests.

## Credential boundaries

- `WARDEN_API_SIGNING_PRIVATE_KEY` exists only on the QuestLog web host.
- `WARDEN_API_TRUSTED_SIGNERS` contains only public keys and exists on Warden.
- `WARDEN_API_LOCAL_SYNC_TOKEN` exists only on Warden and is accepted only
  from loopback for the `guilds.sync` scope.
- `DISCORD_BOT_API_TOKEN` is a temporary migration credential. Remove it from
  both services after signed traffic is verified.

Never paste generated credentials into source control, tickets, or chat.

## Rollout without downtime

1. Generate a key pair on a trusted administrative host:

   ```bash
   python scripts/generate_api_signing_key.py --key-id questlog-web-v1
   ```

2. Generate an independent local sync token:

   ```bash
   python -c "import secrets; print(secrets.token_urlsafe(32))"
   ```

3. On Warden, configure the generated public `WARDEN_API_TRUSTED_SIGNERS`, the
   local sync token, and `WARDEN_API_AUTH_MODE=dual`. Keep the old bearer token
   temporarily, then deploy Warden.

4. On QuestLog, configure the generated `WARDEN_API_SIGNING_KEY_ID` and
   `WARDEN_API_SIGNING_PRIVATE_KEY`, then deploy the website. Exercise guild
   listing, manual sync, moderation, creator announcements, message deletion,
   and network announcements. Warden logs should show no new legacy bearer
   warnings for website routes.

5. Set `WARDEN_API_AUTH_MODE=signed`, restart Warden, and repeat the smoke
   tests. The process fails closed at startup if a route scope or the local
   sync credential is missing.

6. Remove `DISCORD_BOT_API_TOKEN` from both services and rotate any copy that
   may have existed outside the approved secret stores.

## Rotation

Add a second public key entry to `WARDEN_API_TRUSTED_SIGNERS`, deploy Warden,
switch the website to the new private key and key ID, verify signed traffic,
then remove the old public key. This overlap permits rotation without downtime.

## Incident response

If the website private key may be compromised, remove its key ID from
`WARDEN_API_TRUSTED_SIGNERS` immediately, generate a new pair, and review
Warden authentication and moderation logs. A Warden host compromise does not
expose the private signing key.
