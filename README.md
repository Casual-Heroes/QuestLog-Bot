# QuestLog Bot

The Discord bot that pairs with the [QuestLog platform](https://github.com/CasualHeroes/QuestLog). Handles server security, engagement, community discovery, XP/leveling, LFG, streaming integrations, and more - all fully open source and free.

> "WardenBot" was the internal codename during development. The bot is publicly known as **QuestLog Bot**.

---

## Features

### Security & Moderation
- Anti-raid detection - mass join thresholds, automatic lockdown, quarantine
- Member verification - button, CAPTCHA, account age gate, multi-step
- Moderation actions - warn, mute, jail, kick, ban with full audit logging
- Audit log with configurable retention and action history
- Role and channel template management

### XP & Engagement
- Per-server XP system with configurable rates, cooldowns, and level roles
- Voice, message, reaction, gaming activity, and invite XP
- Flair store - members customize their QuestLog profile from Discord
- Leaderboards and rank display

### Discovery Network
- Creator of the Week / Creator of the Month announcements
- Self-promotion channel management
- Featured pool - community member spotlight rotation
- Cross-server network announcements (opt-in)
- Streaming monitor - live Twitch/YouTube alerts via creator profiles

### LFG (Looking for Group)
- Create, browse, and join LFG groups by game
- LFG role management and expiration

### Community Tools
- Welcome messages and onboarding flows
- Scheduled messages and announcements
- RSS feed delivery to Discord channels
- Raffle / giveaway management
- Reaction roles, temp roles, bulk role assignment

### Platform Integration
- Pairs with [QuestLog](https://github.com/CasualHeroes/QuestLog) via internal API
- QuestLog web profile linking (Steam, Discord account)
- Server poll and rotation voting sync
- Site activity tracker - Discord-driven game status

### Emergency Kill Switch
- Owner-only `/emergency` commands for incident response
- Toggle maintenance mode, stop/start web server and Matrix without SSH
- CONFIRM modal required for all destructive actions
- All responses ephemeral - never visible in channels
- Audit channel logging for all emergency actions
- Requires `BOT_OWNER_ID` env var - silently disabled if unset, logs a warning on startup

---

## Coming Soon

QuestLog Bot is Discord-first, but the platform is expanding. Ports to other networks are actively in development:

- **Matrix** - A QuestLog Bot for Matrix rooms, built on the same codebase
- **Fluxer** - A QuestLog Bot for Fluxer, as the platform grows

The goal is a single cohesive community toolkit that works across open and federated platforms - not just Discord.

---

## Tech Stack

| Layer | Tech |
|---|---|
| Language | Python 3.11+ |
| Discord library | py-cord |
| Database | MySQL / MariaDB via SQLAlchemy |
| Internal API | aiohttp |
| Config | python-dotenv |
| Scheduling | discord.ext.tasks |

---

## Related Repos

- [CasualHeroes/QuestLog](https://github.com/CasualHeroes/QuestLog) - The web platform this bot integrates with

---

## Quick Start

### Requirements

- Python 3.11+
- MySQL / MariaDB 8+
- A Discord application and bot token ([discord.com/developers](https://discord.com/developers/applications))

### 1. Clone and install

```bash
git clone https://github.com/CasualHeroes/WardenBot.git
cd WardenBot
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your values
```

In production, place secrets at `/etc/casual-heroes/warden.env` (loaded automatically if present):

```bash
sudo cp .env /etc/casual-heroes/warden.env
sudo chown root:www-data /etc/casual-heroes/warden.env
sudo chmod 640 /etc/casual-heroes/warden.env
```

### 3. Create the database

```bash
mysql -u root -e "CREATE DATABASE warden CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
mysql -u root -e "CREATE USER 'warden'@'localhost' IDENTIFIED BY 'yourpassword';"
mysql -u root -e "GRANT ALL PRIVILEGES ON warden.* TO 'warden'@'localhost';"
```

### 4. Run the bot

```bash
python bot.py
```

Tables are created automatically on first run.

---

## Environment Variables

Copy `.env.example` for the full list. Key variables:

### Required

| Variable | Description |
|---|---|
| `WARDEN_BOT_TOKEN` | Discord bot token |
| `DB_USERNAME` | MySQL username |
| `DB_PASSWORD` | MySQL password |
| `DB_NAME` | MySQL database name (default: `warden`) |
| `DB_SOCKET` or `DB_HOST` | Unix socket path or TCP host |

### Integration with QuestLog (optional)

| Variable | Description |
|---|---|
| `DISCORD_BOT_API_TOKEN` | Shared secret for QuestLog <-> bot API calls |
| `DJANGO_DB_NAME` / `DJANGO_DB_USER` / `DJANGO_DB_PASSWORD` | Access to the QuestLog web database for shared data |

### Discord OAuth (optional - for web dashboard)

| Variable | Description |
|---|---|
| `DISCORD_CLIENT_ID` | Discord OAuth2 client ID |
| `DISCORD_CLIENT_SECRET` | Discord OAuth2 client secret |
| `DISCORD_REDIRECT_URI` | OAuth2 callback URL |

### Integrations (optional)

| Variable | Description |
|---|---|
| `TWITCH_CLIENT_ID` / `TWITCH_CLIENT_SECRET` | Twitch API for streaming monitor |
| `BOT_OWNER_ID` | Discord user ID - enables `/emergency` commands |
| `DISCOVERY_APPROVERS` | Comma-separated user IDs who can approve Discovery communities |
| `EMERGENCY_AUDIT_CHANNEL_ID` | Channel ID for emergency action logging |
| `EMERGENCY_WEB_SERVICE` | systemd service name for the web server (default: `casualheroes`) |
| `EMERGENCY_MATRIX_SERVICE` | systemd service name for Matrix (default: `matrix-synapse`) |

### Development

| Variable | Description |
|---|---|
| `ENVIRONMENT` | `production` or `development` |
| `DEBUG_GUILD_ID` | Restrict slash command sync to one guild (faster iteration) |
| `LOG_LEVEL` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |

---

## Project Structure

```
wardenbot/
- bot.py               - Entry point, cog loader, guild sync
- config.py            - Settings, DB engine, bot instance
- models.py            - SQLAlchemy models
- api_server.py        - Internal aiohttp API (used by QuestLog)
- db.py                - Database session helpers
- actions.py           - Shared action processing logic
- cogs/
  - core.py            - Core bot info commands
  - security.py        - Anti-raid, lockdown
  - verification.py    - Member verification flows
  - audit.py           - Mod log and audit history
  - moderation.py      - Warn, mute, jail, ban
  - roles.py           - Role management, temp roles, bulk assign
  - channels.py        - Channel and category templates
  - xp.py              - XP system, levels, leaderboards
  - flair_cog.py       - Flair store sync with QuestLog web
  - lfg_cog.py         - Looking for Group
  - discovery.py       - Creator spotlight, self-promo, featured pool
  - streaming_monitor.py - Live stream alerts
  - welcome.py         - Welcome messages and onboarding
  - rss_feeds.py       - RSS feed delivery
  - raffles.py         - Raffle and giveaway management
  - scheduled_messages.py - Timed announcements
  - admin.py           - Bot owner admin commands
  - guild_sync.py      - Real-time role/channel cache sync
  - guild_sync_cog.py  - Periodic member count sync
  - activity_tracker.py - Discord activity tracking
  - site_activity_tracker.py - Site-driven Discord game status
  - action_processor.py - Async action queue
  - emergency.py       - Owner-only kill switch
```

---

## Emergency Kill Switch Setup

The `/emergency` commands allow the bot owner to stop and start the web server and Matrix without needing SSH. One-time sudo setup required on the host:

```bash
echo "wardenbot ALL=(ALL) NOPASSWD: /bin/systemctl stop casualheroes" > /etc/sudoers.d/wardenbot-emergency
echo "wardenbot ALL=(ALL) NOPASSWD: /bin/systemctl start casualheroes" >> /etc/sudoers.d/wardenbot-emergency
echo "wardenbot ALL=(ALL) NOPASSWD: /bin/systemctl stop matrix-synapse" >> /etc/sudoers.d/wardenbot-emergency
echo "wardenbot ALL=(ALL) NOPASSWD: /bin/systemctl start matrix-synapse" >> /etc/sudoers.d/wardenbot-emergency
chmod 440 /etc/sudoers.d/wardenbot-emergency
```

Replace `wardenbot` with whatever user the bot process runs as. Replace service names with yours if they differ - set `EMERGENCY_WEB_SERVICE` and `EMERGENCY_MATRIX_SERVICE` in your env.

---

## Security

- Bot token and all credentials stored outside the repo at `/etc/casual-heroes/warden.env` in production
- Internal API authenticated with a pre-shared token (`DISCORD_BOT_API_TOKEN`)
- Error responses never leak internal exception details - exceptions log server-side only
- Emergency commands locked to `BOT_OWNER_ID` - Discord permissions are not used for this
- All emergency responses ephemeral and require a `CONFIRM` modal for destructive actions
- Guild channel access scoped to guild - cross-guild channel manipulation not possible

If you find a security issue, please report it at [github.com/CasualHeroes/WardenBot/issues](https://github.com/CasualHeroes/WardenBot/issues).

---

## License

See [LICENSE](LICENSE).
