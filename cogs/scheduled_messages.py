# cogs/scheduled_messages.py - Scheduled Message Processor
"""
Background task to process and send scheduled messages.
Runs every minute to check for pending messages that need to be sent.
"""

import discord
from discord.ext import commands, tasks
import time
import json
from config import db_session_scope, logger
from models import ScheduledMessage


class ScheduledMessageProcessor(commands.Cog):
    """Processes scheduled messages and sends them at the scheduled time."""

    def __init__(self, bot):
        self.bot = bot
        self.process_scheduled_messages.start()
        logger.info("✅ Scheduled message processor initialized")

    def cog_unload(self):
        """Clean up when cog is unloaded."""
        self.process_scheduled_messages.cancel()
        logger.info("Scheduled message processor stopped")

    @tasks.loop(minutes=1)
    async def process_scheduled_messages(self):
        """Check for and send pending scheduled messages every minute."""
        try:
            current_time = int(time.time())

            with db_session_scope() as session:
                # Query for pending messages that are due
                pending_messages = session.query(ScheduledMessage).filter(
                    ScheduledMessage.status == 'pending',
                    ScheduledMessage.scheduled_time <= current_time
                ).all()

                if not pending_messages:
                    return

                logger.info(f"Processing {len(pending_messages)} scheduled messages")

                for msg in pending_messages:
                    try:
                        await self.send_scheduled_message(msg)

                        # Update to sent status
                        msg.status = 'sent'
                        msg.sent_at = int(time.time())
                        msg.updated_at = int(time.time())
                        session.commit()
                        logger.info(f"✅ Sent scheduled message {msg.id} (type: {msg.message_type})")

                    except Exception as e:
                        # Update to failed status with error
                        msg.status = 'failed'
                        msg.error_message = str(e)[:500]  # Limit error message length
                        msg.updated_at = int(time.time())
                        session.commit()
                        logger.error(f"❌ Failed to send scheduled message {msg.id}: {e}")

        except Exception as e:
            logger.error(f"Error in scheduled message processor: {e}", exc_info=True)

    async def send_scheduled_message(self, msg: ScheduledMessage):
        """Send a single scheduled message based on its type."""
        content_data = json.loads(msg.content_data)

        if msg.message_type == 'message':
            await self.send_message(msg, content_data)
        elif msg.message_type == 'embed':
            await self.send_embed(msg, content_data)
        elif msg.message_type == 'broadcast':
            await self.send_broadcast(msg, content_data)
        else:
            raise ValueError(f"Unknown message type: {msg.message_type}")

    async def send_message(self, msg: ScheduledMessage, content_data: dict):
        """Send a plain message to a channel."""
        channel = self.bot.get_channel(msg.channel_id)
        if not channel:
            raise ValueError(f"Channel {msg.channel_id} not found")

        content = content_data.get('content', '')
        silent = content_data.get('silent', False)

        await channel.send(
            content=content,
            silent=silent
        )

    async def send_embed(self, msg: ScheduledMessage, content_data: dict):
        """Send an embed to a channel."""
        channel = self.bot.get_channel(msg.channel_id)
        if not channel:
            raise ValueError(f"Channel {msg.channel_id} not found")

        embed = discord.Embed(
            title=content_data.get('title', ''),
            description=content_data.get('description', ''),
            color=content_data.get('color', 0x5865F2)
        )

        footer = content_data.get('footer', '')
        if footer:
            embed.set_footer(text=footer)

        silent = content_data.get('silent', False)

        await channel.send(
            embed=embed,
            silent=silent
        )

    async def send_broadcast(self, msg: ScheduledMessage, content_data: dict):
        """Send a message to all text channels under a category."""
        category = self.bot.get_channel(msg.category_id)
        if not category:
            raise ValueError(f"Category {msg.category_id} not found")

        if not isinstance(category, discord.CategoryChannel):
            raise ValueError(f"Channel {msg.category_id} is not a category")

        content = content_data.get('content', '')
        silent = content_data.get('silent', False)

        sent_count = 0
        failed_channels = []

        # Send to all text channels in the category
        for channel in category.text_channels:
            try:
                await channel.send(content=content, silent=silent)
                sent_count += 1
            except discord.Forbidden:
                failed_channels.append(channel.name)
                logger.warning(f"No permission to send to {channel.name}")
            except Exception as e:
                failed_channels.append(channel.name)
                logger.error(f"Error sending to {channel.name}: {e}")

        if failed_channels:
            logger.warning(f"Broadcast sent to {sent_count} channels, failed: {', '.join(failed_channels)}")

        if sent_count == 0:
            raise ValueError(f"Failed to send to any channels in category {category.name}")

    @process_scheduled_messages.before_loop
    async def before_process_scheduled_messages(self):
        """Wait for bot to be ready before starting the processor."""
        await self.bot.wait_until_ready()
        logger.info("Scheduled message processor is ready")


def setup(bot):
    """Add the cog to the bot."""
    bot.add_cog(ScheduledMessageProcessor(bot))
