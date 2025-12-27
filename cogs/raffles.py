import asyncio
import time
import json
import discord
from discord.ext import commands, tasks

from config import logger, db_session_scope
from models import Raffle, RaffleEntry, GuildMember


def _render_announcement(raffle: Raffle, role_id: int, guild_name: str):
    content = raffle.announce_message or "A new raffle is live! {role}"
    content = content.replace("{title}", raffle.title)
    content = content.replace("{cost}", str(raffle.cost_tokens))
    content = content.replace("{end}", f"<t:{raffle.end_at}:f>" if raffle.end_at else "N/A")
    if "{role}" in content and role_id:
        content = content.replace("{role}", f"<@&{role_id}>")
    else:
        content = content.replace("{role}", "")
        if role_id:
            content = f"<@&{role_id}> {content}"
    content = content.replace("{guild}", guild_name or "")
    return content.strip()


def _render_winner_msg(raffle: Raffle, winner_name: str, guild_name: str):
    content = raffle.winner_message or "Congrats {user}, you won {title}!"
    content = content.replace("{user}", winner_name or "winner")
    content = content.replace("{title}", raffle.title)
    content = content.replace("{guild}", guild_name or "")
    return content


def _draw_winners(session, raffle: Raffle):
    entries = session.query(RaffleEntry).filter_by(raffle_id=raffle.id).all()
    if not entries:
        raffle.winners = json.dumps([])
        raffle.active = False
        raffle.winners_announced = False  # Needs announcement even if empty
        return []

    population = []
    for e in entries:
        population.append({'user_id': e.user_id, 'username': e.username, 'weight': max(1, e.tickets)})

    winners = []
    remaining = population[:]
    total_winners = max(1, raffle.max_winners or 1)
    rng = asyncio.get_running_loop().random if hasattr(asyncio.get_running_loop(), "random") else None
    import random
    sys_rng = random.SystemRandom()

    for _ in range(total_winners):
        if not remaining:
            break
        total_weight = sum(item['weight'] for item in remaining)
        pick = sys_rng.uniform(0, total_weight) if sys_rng else random.uniform(0, total_weight)
        cumulative = 0
        chosen = None
        for item in remaining:
            cumulative += item['weight']
            if pick <= cumulative:
                chosen = item
                break
        if chosen:
            winners.append({'user_id': chosen['user_id'], 'username': chosen.get('username')})
            remaining = [r for r in remaining if r['user_id'] != chosen['user_id']]

    raffle.winners = json.dumps(winners)
    raffle.active = False
    raffle.winners_announced = False  # Mark as needing announcement
    return winners


class RafflesCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.message_cache = {}  # message_id -> raffle_id
        self.poll_raffles.start()

    def cog_unload(self):
        self.poll_raffles.cancel()

    async def _post_announcement(self, raffle: Raffle, session):
        guild = self.bot.get_guild(raffle.guild_id)
        if not guild or not raffle.announce_channel_id:
            return

        channel = guild.get_channel(raffle.announce_channel_id)
        if not channel:
            try:
                channel = await guild.fetch_channel(raffle.announce_channel_id)
            except Exception:
                logger.warning(f"Raffle {raffle.id}: channel {raffle.announce_channel_id} not found")
                return

        content = _render_announcement(raffle, raffle.announce_role_id, guild.name)
        entry_count = session.query(RaffleEntry).filter_by(raffle_id=raffle.id).count()

        # Add link to raffle browser
        raffle_url = f"https://dashboard.casual-heroes.com/questlog/guild/{raffle.guild_id}/raffle-browser/"
        emoji = raffle.entry_emoji or "🎟️"
        description = raffle.description or ""
        if description:
            description += f"\n\n{emoji} **React with the emoji to enter** or [**go to the dashboard!**]({raffle_url})"
        else:
            description = f"{emoji} **React with the emoji to enter** or [**go to the dashboard!**]({raffle_url})"

        embed = discord.Embed(
            title=raffle.title,
            description=description,
            color=discord.Color.gold()
        )
        embed.add_field(name="Cost", value=f"{raffle.cost_tokens} tokens", inline=True)
        embed.add_field(name="Max Winners", value=str(raffle.max_winners), inline=True)
        if raffle.start_at:
            embed.add_field(name="Starts", value=f"<t:{raffle.start_at}:f>", inline=True)
        if raffle.end_at:
            embed.add_field(name="Ends", value=f"<t:{raffle.end_at}:f>", inline=True)
        embed.add_field(name="Entries", value=str(entry_count), inline=True)

        try:
            # content already contains the role mention if provided; avoid duplicating
            msg = await channel.send(content=content.strip(), embed=embed)
            emoji = raffle.entry_emoji or "🎟️"
            try:
                await msg.add_reaction(emoji)
            except Exception:
                logger.warning(f"Raffle {raffle.id}: failed to add emoji {emoji}, using 🎟️")
                await msg.add_reaction("🎟️")
            return msg.id
        except Exception as e:
            logger.warning(f"Failed to post raffle {raffle.id} announcement: {e}", exc_info=True)
            return None

    @tasks.loop(seconds=5)
    async def poll_raffles(self):
        await self.bot.wait_until_ready()
        now = int(time.time())
        try:
            with db_session_scope() as session:
                # Post announcements for started raffles without message
                to_post = session.query(Raffle).filter(
                    Raffle.active == True,
                    Raffle.announce_message_id == None,
                    (Raffle.start_at == None) | (Raffle.start_at <= now),
                    (Raffle.end_at == None) | (Raffle.end_at > now),
                    Raffle.winners == None
                ).all()
                for r in to_post:
                    msg_id = await self._post_announcement(r, session)
                    if msg_id:
                        r.announce_message_id = msg_id
                        self.message_cache[msg_id] = r.id
                        session.flush()  # Flush immediately to prevent duplicate posts
                if to_post:
                    session.flush()

                # Auto pick winners if time passed
                to_pick = session.query(Raffle).filter(
                    Raffle.active == True,
                    Raffle.auto_pick == True,
                    Raffle.end_at != None,
                    Raffle.end_at <= now,
                    Raffle.winners == None
                ).all()
                for r in to_pick:
                    winners = _draw_winners(session, r)
                    r.winners_announced = True  # Mark as announced before announcing
                    session.flush()  # Commit immediately to prevent duplicate
                    await self._announce_winners(r, winners)
                if to_pick:
                    session.flush()

                # Send admin reminders for ended raffles without auto-pick
                to_remind = session.query(Raffle).filter(
                    Raffle.active == True,
                    Raffle.auto_pick == False,
                    Raffle.end_at != None,
                    Raffle.end_at <= now,
                    Raffle.winners == None,
                    Raffle.reminder_channel_id != None,
                    Raffle.reminder_sent == False
                ).all()
                for r in to_remind:
                    r.reminder_sent = True
                    session.flush()  # Commit immediately to prevent duplicate reminders
                    await self._send_admin_reminder(r)
                if to_remind:
                    session.flush()

                # Announce manually picked winners that haven't been announced yet
                to_announce = session.query(Raffle).filter(
                    Raffle.winners != None,
                    Raffle.winners_announced == False
                ).all()
                for r in to_announce:
                    try:
                        winners = json.loads(r.winners) if r.winners else []
                    except Exception:
                        winners = []
                    # Set BEFORE announcing to prevent duplicate announcements
                    r.winners_announced = True
                    session.flush()  # Commit immediately
                    await self._announce_winners(r, winners)
                if to_announce:
                    session.flush()

                # Update entry counts in active raffle embeds (every poll cycle)
                active_raffles = session.query(Raffle).filter(
                    Raffle.active == True,
                    Raffle.announce_message_id != None,
                    Raffle.winners == None
                ).all()
                for r in active_raffles:
                    await self._update_raffle_embed(r, session)
        except Exception as e:
            logger.warning(f"Raffle poll error: {e}", exc_info=True)

    async def _update_raffle_embed(self, raffle: Raffle, session):
        """Update the entry count in an active raffle's embed."""
        try:
            guild = self.bot.get_guild(raffle.guild_id)
            if not guild or not raffle.announce_channel_id:
                return

            channel = guild.get_channel(raffle.announce_channel_id)
            if not channel:
                return

            message = await channel.fetch_message(raffle.announce_message_id)
            if not message or not message.embeds:
                return

            entry_count = session.query(RaffleEntry).filter_by(raffle_id=raffle.id).count()

            # Get current embed and update the Entries field
            emb = message.embeds[0]
            embed_dict = emb.to_dict()
            if 'fields' in embed_dict:
                for field in embed_dict['fields']:
                    if field.get('name') == 'Entries':
                        # Only update if the value changed
                        if field['value'] != str(entry_count):
                            field['value'] = str(entry_count)
                            new_emb = discord.Embed.from_dict(embed_dict)
                            await message.edit(embed=new_emb)
                        break
        except Exception as e:
            logger.warning(f"Failed to update raffle {raffle.id} embed: {e}")

    async def _announce_winners(self, raffle: Raffle, winners):
        guild = self.bot.get_guild(raffle.guild_id)
        if not guild or not raffle.announce_channel_id:
            return
        channel = guild.get_channel(raffle.announce_channel_id)
        if not channel:
            try:
                channel = await guild.fetch_channel(raffle.announce_channel_id)
            except Exception:
                return
        if not winners:
            await channel.send(f"No winners for raffle **{raffle.title}** (no entries).")
            return
        entry_count = self._entry_count_for_announce(raffle.id)
        winner_mentions = []
        for w in winners:
            if w.get('user_id'):
                winner_mentions.append(f"<@{w['user_id']}>")
            else:
                winner_mentions.append(w.get('username', 'winner'))
        content = _render_winner_msg(raffle, ", ".join(winner_mentions), guild.name)
        await channel.send(f"{content}\nEntries: {entry_count}")

    def _entry_count_for_announce(self, raffle_id: int):
        try:
            with db_session_scope() as session:
                return session.query(RaffleEntry).filter_by(raffle_id=raffle_id).count()
        except Exception:
            return 0

    async def _send_admin_reminder(self, raffle: Raffle):
        """Send a reminder to admins to manually pick a winner for an ended raffle."""
        guild = self.bot.get_guild(raffle.guild_id)
        if not guild or not raffle.reminder_channel_id:
            return

        channel = guild.get_channel(raffle.reminder_channel_id)
        if not channel:
            try:
                channel = await guild.fetch_channel(raffle.reminder_channel_id)
            except Exception:
                logger.warning(f"Raffle {raffle.id}: reminder channel {raffle.reminder_channel_id} not found")
                return

        entry_count = self._entry_count_for_announce(raffle.id)

        embed = discord.Embed(
            title=f"⏰ Raffle Ended - Action Required",
            description=f"The raffle **{raffle.title}** has ended and needs a winner to be picked manually.",
            color=discord.Color.orange()
        )
        embed.add_field(name="Raffle ID", value=str(raffle.id), inline=True)
        embed.add_field(name="Entries", value=str(entry_count), inline=True)
        embed.add_field(name="Max Winners", value=str(raffle.max_winners), inline=True)
        embed.add_field(
            name="Action Needed",
            value="Please go to the QuestLog dashboard and manually pick a winner for this raffle.",
            inline=False
        )

        try:
            await channel.send(embed=embed)
            logger.info(f"Sent admin reminder for raffle {raffle.id}")
        except Exception as e:
            logger.warning(f"Failed to send admin reminder for raffle {raffle.id}: {e}", exc_info=True)

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        if payload.user_id == self.bot.user.id:
            return
        msg_id = payload.message_id
        if msg_id not in self.message_cache:
            # Try to resolve from DB lazily
            with db_session_scope() as session:
                raffle = session.query(Raffle).filter_by(announce_message_id=msg_id).first()
                if raffle:
                    self.message_cache[msg_id] = raffle.id
                else:
                    return

        raffle_id = self.message_cache[msg_id]
        try:
            with db_session_scope() as session:
                raffle = session.query(Raffle).filter_by(id=raffle_id).first()
                if not raffle or not raffle.active:
                    return

                now = int(time.time())
                if raffle.start_at and now < raffle.start_at:
                    return
                if raffle.end_at and now > raffle.end_at:
                    return

                # Check emoji match
                entry_emoji = raffle.entry_emoji or "🎟️"
                emoji_str = str(payload.emoji)
                if entry_emoji not in emoji_str and emoji_str not in entry_emoji:
                    return

                member = session.query(GuildMember).filter_by(
                    guild_id=raffle.guild_id,
                    user_id=payload.user_id
                ).first()
                if not member:
                    return

                # Check max entries per user limit
                if raffle.max_entries_per_user:
                    from sqlalchemy import func
                    existing_entries = session.query(func.sum(RaffleEntry.tickets)).filter_by(
                        raffle_id=raffle.id,
                        user_id=payload.user_id
                    ).scalar() or 0

                    if existing_entries >= raffle.max_entries_per_user:
                        # Remove reaction and send DM
                        guild = self.bot.get_guild(raffle.guild_id)
                        channel = guild.get_channel(payload.channel_id) if guild else None
                        try:
                            if channel:
                                message = await channel.fetch_message(msg_id)
                                user = guild.get_member(payload.user_id)
                                await message.remove_reaction(payload.emoji, user)
                                # Send DM to user explaining they hit the limit
                                try:
                                    await user.send(
                                        f"❌ You've already reached the maximum entries for **{raffle.title}**.\n"
                                        f"**Max entries per person:** {raffle.max_entries_per_user}\n"
                                        f"**Your entries:** {existing_entries}\n\n"
                                        f"This limit applies across both Discord reactions and dashboard entries."
                                    )
                                except Exception:
                                    # User has DMs disabled, skip
                                    pass
                        except Exception:
                            pass
                        return

                cost = raffle.cost_tokens
                if member.hero_tokens < cost:
                    # remove reaction if insufficient and notify user
                    guild = self.bot.get_guild(raffle.guild_id)
                    channel = guild.get_channel(payload.channel_id) if guild else None
                    try:
                        if channel:
                            message = await channel.fetch_message(msg_id)
                            user = guild.get_member(payload.user_id)
                            await message.remove_reaction(payload.emoji, user)
                            # Send DM to user explaining why
                            try:
                                await user.send(
                                    f"❌ You don't have enough tokens to enter **{raffle.title}**.\n"
                                    f"**Required:** {cost} tokens\n"
                                    f"**You have:** {member.hero_tokens} tokens\n"
                                    f"**Need:** {cost - member.hero_tokens} more tokens"
                                )
                            except Exception:
                                # User has DMs disabled, skip
                                pass
                    except Exception:
                        pass
                    return

                # Deduct and add entry
                member.hero_tokens -= cost
                entry = RaffleEntry(
                    raffle_id=raffle.id,
                    guild_id=raffle.guild_id,
                    user_id=payload.user_id,
                    username=str(payload.member.display_name if payload.member else payload.user_id),
                    tickets=1
                )
                session.add(entry)
                # Note: Not updating embed in real-time as it causes Discord to reset reactions
                # Users can see entry count from the reaction count on the message
        except Exception as e:
            logger.warning(f"Error processing raffle reaction: {e}", exc_info=True)


def setup(bot):
    bot.add_cog(RafflesCog(bot))
