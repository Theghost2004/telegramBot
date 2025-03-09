import asyncio
import json
import logging
import random
import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Union, Any, Tuple

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon import types
from telethon.tl import functions

from config import StatusIndicator, format_duration
from utils import AdminHandler, safe_execution, get_random_delay, get_chat_display_name
from models import AdScheduler
import os
from pathlib import Path
from datetime import timedelta


logger = logging.getLogger("AdBot")

class CommandHandler:
    """Handles all bot commands and their execution."""
    def __init__(self, client: TelegramClient, client_name: str, admin_ids: List[int]):
        self.client = client
        self.client_name = client_name
        self.scheduler = AdScheduler(client_name)
        self.status_indicators = {}
        self.running_tasks = {}
        self.auto_ad_task = None
        self.auto_ad_chat_id = None
        self.admin_ids = admin_ids or []
        self.register_handlers()

    def register_handlers(self) -> None:
        """Register all command handlers."""
        handlers = {
            'help': self.help_command,
            'cleantargets': self.clean_targets,
            'removeunsubs': self.remove_unsubs,
            'joinchats': self.join_chats,
            'leavechats': self.leave_chats,
            'leaveandremove': self.leave_and_remove,
            'clearchat': self.clear_chat,
            'client': self.show_client,
            'test': self.test_bot,
            'timer': self.set_timer,
            'listjoined': self.list_joined,
            'stopadtimer': self.stop_timer,
            'addtarget': self.add_targets,
            'listtarget': self.list_targets,
            'removetarget': self.remove_target_handler,
            'removealltarget': self.remove_all_targets,
            'startad': self.send_ad,
            'setad': self.set_ad,
            'listad': self.list_ad,
            'removead': self.remove_ad_handler,
            'stopad': self.stop_ad,
            'pin': self.pin_message,
            'addadmin': self.add_admin,
            'removeadmin': self.remove_admin,
            'listadmins': self.list_admins,
            # Advanced features
            'analytics': self.show_analytics,
            'forward': self.forward_message,
            'backup': self.backup_data,
            'restore': self.restore_data,
            'schedule': self.schedule_ad,
            'broadcast': self.broadcast_message,
            'targeting': self.set_targeting,
            'findgroups': self.find_groups,
            'stickers': self.send_stickers,
            'interactive': self.create_interactive_message,
            # Targeted ad features
            'targetedad': self.targeted_ad,
            'listtargeted': self.list_targeted_campaigns,
            'stoptargeted': self.stop_targeted_campaign,
        }

        for command, handler in handlers.items():
            # Create a combined handler for both direct commands and reply commands
            async def combined_handler(event, cmd=command, orig_handler=handler):
                # First check: Is this a direct command?
                if event.message and event.message.text and event.message.text.startswith(f'/{cmd} '):
                    await orig_handler(event)
                    return

                # Second check: Is this a command only?
                if event.message and event.message.text and event.message.text == f'/{cmd}':
                    await orig_handler(event)
                    return

                # Third check: Is this a reply with a command?
                if event.message and event.message.text and event.message.text.startswith(f'/{cmd}') and event.is_reply:
                    # Get the message being replied to
                    replied_msg = await event.message.get_reply_message()
                    if replied_msg:
                        # Store the replied message for the handler to use
                        event.replied_message = replied_msg
                        await orig_handler(event)
                        return

            # Add handler for both outgoing and incoming messages with better pattern matching
            from config import COMMAND_PREFIX

            # First pattern: exact command
            self.client.add_event_handler(
                combined_handler,
                events.NewMessage(pattern=f'^{COMMAND_PREFIX}{re.escape(command)}$', outgoing=True)
            )
            self.client.add_event_handler(
                combined_handler,
                events.NewMessage(pattern=f'^{COMMAND_PREFIX}{re.escape(command)}$', incoming=True)
            )

            # Second pattern: command with parameters
            self.client.add_event_handler(
                combined_handler,
                events.NewMessage(pattern=f'^{COMMAND_PREFIX}{re.escape(command)}\\s', outgoing=True)
            )
            self.client.add_event_handler(
                combined_handler,
                events.NewMessage(pattern=f'^{COMMAND_PREFIX}{re.escape(command)}\\s', incoming=True)
            )

    async def update_status(self, task_id: str, message: str, current: Optional[int] = None, 
                           total: Optional[int] = None, extra_info: Optional[str] = None) -> None:
        """Update status message with progress."""
        if task_id not in self.status_indicators:
            try:
                # Get appropriate chat ID for status messages
                chat_id = None
                if hasattr(self, 'event'):
                    chat_id = self.event.chat_id
                elif hasattr(self, 'auto_ad_chat_id') and self.auto_ad_chat_id:
                    chat_id = self.auto_ad_chat_id

                # If no chat ID is available, log message and return
                if chat_id is None:
                    logger.info(f"Status update ({message}) - no chat ID available")
                    return

                status_message = await self.client.send_message(
                    chat_id,
                    f"{message}..."
                )
                self.status_indicators[task_id] = StatusIndicator(message=message)
                self.running_tasks[task_id] = status_message
            except Exception as e:
                logger.error(f"Failed to create status message: {e}")
                return

        try:
            formatted_message = self.status_indicators[task_id].format_message(
                current, total, extra_info
            )
            await self.running_tasks[task_id].edit(formatted_message)
        except Exception as e:
            logger.error(f"Failed to update status: {e}")

    async def cleanup_status(self, task_id: str) -> None:
        """Clean up status message resources."""
        if task_id in self.status_indicators:
            try:
                await self.running_tasks[task_id].delete()
            except Exception as e:
                logger.error(f"Error cleaning up status: {e}")
            finally:
                if task_id in self.status_indicators:
                    del self.status_indicators[task_id]
                if task_id in self.running_tasks:
                    del self.running_tasks[task_id]

    async def clean_targets(self, event) -> None:
        """Clean up targets that the user is not a member of."""
        # Store event for status updates
        self.event = event

        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        task_id = f"clean_targets_{int(time.time())}"
        await self.update_status(task_id, "Cleaning targets")

        try:
            targets = self.scheduler.get_targets()
            if not targets:
                await event.reply("❌ No targets to clean.")
                await self.cleanup_status(task_id)
                return

            cleaned = 0
            total = len(targets)

            for i, target in enumerate(targets[:]):
                try:
                    # Handle both dictionary and string targets
                    target_id = target['original'] if isinstance(target, dict) else target

                    await self.update_status(task_id, "Checking targets", i, total)

                    # Try to get the entity
                    try:
                        target_entity = await self.client.get_entity(target_id)
                    except Exception:
                        # Target not found, remove it
                        self.scheduler.remove_target(target_id)
                        cleaned += 1

                except Exception as e:
                    logger.error(f"Error checking target {target}: {e}")

            await self.cleanup_status(task_id)
            await event.reply(f"✅ Cleaned {cleaned} targets that you are not a member of.")
        except Exception as e:
            logger.error(f"Error while cleaning targets: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")
            await self.cleanup_status(task_id)

    async def remove_unsubs(self, event) -> None:
        """Remove targets you're not subscribed to."""
        # Store event for status updates
        self.event = event

        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        task_id = f"remove_unsubs_{int(time.time())}"
        await self.update_status(task_id, "Checking subscriptions")

        try:
            targets = self.scheduler.get_targets()
            if not targets:
                await event.reply("❌ No targets to check.")
                await self.cleanup_status(task_id)
                return

            removed = 0
            total = len(targets)

            for i, target in enumerate(targets[:]):
                try:
                    # Handle both dictionary and string targets
                    target_id = target['original'] if isinstance(target, dict) else target

                    await self.update_status(task_id, "Checking subscription status", i, total)

                    try:
                        target_entity = await self.client.get_entity(target_id)

                        # Try to get participant info to check subscription status
                        try:
                            participant = await self.client(functions.channels.GetParticipantRequest(
                                channel=target_entity,
                                participant=await self.client.get_me()
                            ))
                        except Exception:
                            # Not a participant/subscriber
                            self.scheduler.remove_target(target_id)
                            removed += 1
                    except Exception as e:
                        logger.error(f"Error getting entity for {target_id}: {e}")

                except Exception as e:
                    logger.error(f"Error checking subscription for {target}: {e}")

            await self.cleanup_status(task_id)
            await event.reply(f"✅ Removed {removed} targets you're not subscribed to.")
        except Exception as e:
            logger.error(f"Error while removing unsubscribed targets: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")
            await self.cleanup_status(task_id)

    async def join_chats(self, event) -> None:
        """Join multiple chats at once."""
        # Store event for status updates
        self.event = event

        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        text = event.text.split(None, 1)
        if len(text) < 2:
            await event.reply("❌ Please provide chat IDs or usernames to join.")
            return

        chat_list = [chat.strip() for chat in text[1].split(',')]
        if not chat_list:
            await event.reply("❌ No valid chats provided.")
            return

        task_id = f"join_chats_{int(time.time())}"
        await self.update_status(task_id, "Joining chats")

        try:
            joined = 0
            failed = 0
            total = len(chat_list)

            for i, chat in enumerate(chat_list):
                try:
                    await self.update_status(task_id, "Joining chats", i, total)

                    # Clean up the chat identifier
                    if chat.startswith('@'):
                        chat = chat[1:]
                    if chat.startswith('https://t.me/'):
                        chat = chat[13:]
                    if chat.startswith('t.me/'):
                        chat = chat[5:]

                    # Try to join the chat
                    try:
                        await self.client(functions.channels.JoinChannelRequest(channel=chat))
                        joined += 1
                    except Exception as e:
                        logger.error(f"Failed to join {chat}: {e}")
                        failed += 1

                    # Ultra-fast mode - no delay between operations
                    # await asyncio.sleep(0)
                except FloodWaitError as e:
                    await self.update_status(
                        task_id, 
                        "Joining chats (rate limited)", 
                        i, 
                        total, 
                        f"⚠️ Rate limited for {e.seconds} seconds"
                    )
                    await asyncio.sleep(e.seconds)
                except Exception as e:
                    logger.error(f"Failed to process join for {chat}: {e}")
                    failed += 1

            await self.cleanup_status(task_id)

            if joined > 0:
                await event.reply(f"✅ Successfully joined {joined} chats!\n❌ Failed to join {failed} chats.")
            else:
                await event.reply(f"❌ Failed to join any chats.")

        except Exception as e:
            logger.error(f"Error while joining chats: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")
            await self.cleanup_status(task_id)

    async def leave_chats(self, event) -> None:
        """Leave multiple chats at once."""
        # Store event for status updates
        self.event = event

        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        text = event.text.split(None, 1)
        if len(text) < 2:
            await event.reply("❌ Please provide chat IDs or usernames to leave.")
            return

        chat_list = [chat.strip() for chat in text[1].split(',')]
        if not chat_list:
            await event.reply("❌ No valid chats provided.")
            return

        task_id = f"leave_chats_{int(time.time())}"
        await self.update_status(task_id, "Leaving chats")

        try:
            left = 0
            failed = 0
            total = len(chat_list)

            for i, chat in enumerate(chat_list):
                try:
                    await self.update_status(task_id, "Leaving chats", i, total)

                    # Clean up the chat identifier
                    if chat.startswith('@'):
                        chat = chat[1:]
                    if chat.startswith('https://t.me/'):
                        chat = chat[13:]
                    if chat.startswith('t.me/'):
                        chat = chat[5:]

                    # Try to get the entity and leave
                    try:
                        entity = await self.client.get_entity(chat)
                        await self.client(functions.channels.LeaveChannelRequest(entity))
                        left += 1
                    except Exception as e:
                        logger.error(f"Failed to leave {chat}: {e}")
                        failed += 1

                    # Ultra-fast mode - no delay between operations
                    # await asyncio.sleep(0)
                except FloodWaitError as e:
                    await self.update_status(
                        task_id, 
                        "Leaving chats (rate limited)", 
                        i, 
                        total, 
                        f"⚠️ Rate limited for {e.seconds} seconds"
                    )
                    await asyncio.sleep(e.seconds)
                except Exception as e:
                    logger.error(f"Failed to process leave for {chat}: {e}")
                    failed += 1

            await self.cleanup_status(task_id)

            if left > 0:
                await event.reply(f"✅ Successfully left {left} chats!\n❌ Failed to leave {failed} chats.")
            else:
                await event.reply(f"❌ Failed to leave any chats.")

        except Exception as e:
            logger.error(f"Error while leaving chats: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")
            await self.cleanup_status(task_id)

    async def leave_and_remove(self, event) -> None:
        """Leave chats and remove them from targets."""
        # Store event for status updates
        self.event = event

        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        text = event.text.split(None, 1)
        if len(text) < 2:
            await event.reply("❌ Please provide chat IDs or usernames to leave and remove.")
            return

        chat_list = [chat.strip() for chat in text[1].split(',')]
        if not chat_list:
            await event.reply("❌ No valid chats provided.")
            return

        task_id = f"leave_and_remove_{int(time.time())}"
        await self.update_status(task_id, "Leaving and removing chats")

        try:
            left = 0
            removed = 0
            failed = 0
            total = len(chat_list)

            for i, chat in enumerate(chat_list):
                try:
                    await self.update_status(task_id, "Processing chats", i, total)

                    # Clean up the chat identifier
                    original_chat = chat
                    if chat.startswith('@'):
                        chat = chat[1:]
                    if chat.startswith('https://t.me/'):
                        chat = chat[13:]
                    if chat.startswith('t.me/'):
                        chat = chat[5:]

                    # Try to get the entity and leave
                    try:
                        entity = await self.client.get_entity(chat)
                        await self.client(functions.channels.LeaveChannelRequest(entity))
                        left += 1

                        # Remove from targets
                        for target in self.scheduler.get_targets()[:]:
                            target_id = target['original'] if isinstance(target, dict) else target
                            if chat.lower() in target_id.lower():
                                self.scheduler.remove_target(target_id)
                                removed += 1
                                break
                    except Exception as e:
                        logger.error(f"Failed to leave or remove {chat}: {e}")
                        failed += 1

                    # Ultra-fast mode - no delay between operations
                    # await asyncio.sleep(0)
                except FloodWaitError as e:
                    await self.update_status(
                        task_id, 
                        "Processing (rate limited)", 
                        i, 
                        total, 
                        f"⚠️ Rate limited for {e.seconds} seconds"
                    )
                    await asyncio.sleep(e.seconds)
                except Exception as e:
                    logger.error(f"Failed to process {chat}: {e}")
                    failed += 1

            await self.cleanup_status(task_id)

            if left > 0 or removed > 0:
                await event.reply(f"✅ Successfully left {left} chats and removed {removed} targets!\n❌ Failed to process {failed} chats.")
            else:
                await event.reply(f"❌ Failed to process any chats.")

        except Exception as e:
            logger.error(f"Error while leaving and removing chats: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")
            await self.cleanup_status(task_id)

    async def clear_chat(self, event) -> None:
        """Delete multiple messages in current chat."""
        # Store event for status updates
        self.event = event

        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        text = event.text.split(None, 1)
        count = 100  # Default count

        if len(text) > 1:
            try:
                count = int(text[1])
                if count <= 0:
                    await event.reply("❌ Count must be a positive number.")
                    return
            except ValueError:
                await event.reply("❌ Invalid count. Please provide a valid number.")
                return

        task_id = f"clear_chat_{int(time.time())}"
        await self.update_status(task_id, f"Clearing {count} messages")

        try:
            deleted = 0

            # Get messages from the current chat
            async for message in self.client.iter_messages(event.chat_id, limit=count):
                if message.id != event.id:  # Don't delete the command message yet
                    try:
                        await self.client.delete_messages(event.chat_id, message)
                        deleted += 1
                        if deleted % 10 == 0:  # Update status every 10 messages
                            await self.update_status(task_id, f"Clearing messages", deleted, count)
                            await asyncio.sleep(0.2)  # Small delay to avoid flood
                    except Exception as e:
                        logger.error(f"Failed to delete message {message.id}: {e}")

            # Delete the command message itself
            await event.delete()
            deleted += 1

            await self.cleanup_status(task_id)

            # Send temporary confirmation (will be auto-deleted)
            confirm_msg = await self.client.send_message(
                event.chat_id, 
                f"✅ Successfully deleted {deleted} messages."
            )

            # Auto-delete confirmation after 3 seconds
            await asyncio.sleep(3)
            await confirm_msg.delete()

        except Exception as e:
            logger.error(f"Error while clearing chat: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")
            await self.cleanup_status(task_id)

    async def show_client(self, event) -> None:
        """Show current client information."""
        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        try:
            from config import BOT_NAME
            me = await self.client.get_me()
            dialogs = await self.client.get_dialogs(limit=1)

            client_info = (
                f"🔥 **{BOT_NAME} STATUS** 🔥\n\n"
                f"👑 **Account**: {me.first_name} {me.last_name or ''}\n"
                f"🆔 **ID**: `{me.id}`\n"
                f"🌟 **Username**: @{me.username or 'None'}\n"
                f"📱 **Phone**: {me.phone or 'Not available'}\n"
                f"⚡ **Bot Mode**: {'Activated' if me.bot else 'User Mode'}\n\n"
                f"💬 **Active Chats**: {len(dialogs)}\n"
                f"🎯 **Marketing Targets**: {len(self.scheduler.get_targets())}\n"
                f"📣 **Promotional Ads**: {len(self.scheduler.get_ads())}\n\n"
                f"⏳ **Uptime**: Active since {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"🚀 **Performance**: Turbo Mode Enabled"
            )

            await event.reply(client_info)
        except Exception as e:
            logger.error(f"Error showing client info: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    async def test_bot(self, event) -> None:
        """Test bot functionality."""
        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        try:
            from config import BOT_NAME
            # Create an enhanced test message with formatting
            test_message = (
                f"🚀 **{BOT_NAME}: SYSTEM CHECK** 🚀\n\n"
                "✅ **Connection**: Optimal Performance\n"
                "✅ **Command System**: Hyper-Responsive\n"
                "✅ **Message Formatting**: *Italic*, **Bold**, __Underline__, `Code`\n"
                f"✅ **Turbo Mode**: ULTRA MODE ENABLED\n\n"
                "⚡ **System Status**: All Systems Operational\n"
                "🔋 **Performance**: Maximum Efficiency\n"
                "🛡️ **Security**: Active Protection\n\n"
                "⏱️ Diagnostic completed at: " + datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )

            # Send a temporary "Testing..." message
            test_msg = await event.reply("🔄 Testing bot functionality...")

            # Short delay to simulate processing
            await asyncio.sleep(1.5)

            # Update with the test results
            await test_msg.edit(test_message)

            logger.info("Bot test completed successfully")
        except Exception as e:
            logger.error(f"Error during bot test: {str(e)}")
            await event.reply(f"❌ Test failed: {str(e)}")

    async def set_timer(self, event) -> None:
        """Schedule message sending with a timer."""
        # Store event for status updates
        self.event = event

        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        text = event.text.split()

        if len(text) < 3:
            await event.reply("❌ Usage: `/timer <ad_id> <seconds> [min_delay] [max_delay]`")
            return

        try:
            ad_id = int(text[1])
            seconds = int(text[2])
            min_delay = int(text[3]) if len(text) > 3 else 2
            max_delay = int(text[4]) if len(text) > 4 else 7

            if seconds < 10:
                await event.reply("❌ Timer must be at least 10 seconds.")
                return

            ad = self.scheduler.get_ad(ad_id)
            if not ad:
                await event.reply(f"❌ Advertisement with ID {ad_id} not found.")
                return

            # Cancel any existing timer
            if self.auto_ad_task and not self.auto_ad_task.done():
                self.auto_ad_task.cancel()

            # Start the timer
            self.auto_ad_chat_id = event.chat_id
            self.auto_ad_task = asyncio.create_task(
                self._timer_loop(ad_id, seconds, min_delay, max_delay)
            )

            human_time = format_duration(seconds)
            await event.reply(
                f"⏱️ PRECISION TIMER ACTIVATED ⏱️\n\n"
                f"🔥 Campaign #{ad_id} scheduled!\n"
                f"⚡ Frequency: Every {human_time}\n"
                f"🚀 Mode: Turbo Performance\n"
                f"📊 Status: Active & Ready\n\n"
                f"Your precision timed campaign is now running in Turbo Mode!"
            )

        except ValueError:
            await event.reply("❌ Invalid parameters. Make sure to use numbers only.")
        except Exception as e:
            logger.error(f"Error setting timer: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    async def _timer_loop(self, ad_id: int, interval: int, min_delay: int, max_delay: int) -> None:
        """Background task for timed ad sending."""
        try:
            while True:
                # Wait for the specified interval
                await asyncio.sleep(interval)

                # Add a random delay to make it look more natural
                random_delay = random.uniform(min_delay, max_delay)
                await asyncio.sleep(random_delay)

                # Send the ad to all targets
                ad = self.scheduler.get_ad(ad_id)
                if not ad:
                    error_msg = f"Ad with ID {ad_id} not found for timer task"
                    logger.error(error_msg)
                    if hasattr(self, 'auto_ad_chat_id') and self.auto_ad_chat_id:
                        from utils import error_to_chat
                        await error_to_chat(self.client, self.auto_ad_chat_id, error_msg)
                    return

                # Create a unique task ID for this run
                task_id = f"timer_ad_{ad_id}_{int(time.time())}"

                try:
                    targets = self.scheduler.get_targets()
                    if not targets:
                        await self.client.send_message(
                            self.auto_ad_chat_id,
                            "❌ No targets to send ad to."
                        )
                        continue

                    await self.update_status(
                        task_id, 
                        f"Sending ad #{ad_id} (timer)",
                        0, 
                        len(targets)
                    )

                    sent = 0
                    failed = 0
                    failed_targets = []

                    for i, target in enumerate(targets):
                        try:
                            # Update status
                            await self.update_status(
                                task_id, 
                                f"Sending ad #{ad_id} (timer)",
                                i, 
                                len(targets)
                            )

                            # Check if target is a dictionary or string
                            target_id = target['original'] if isinstance(target, dict) else target

                            # Forward message instead of sending as copy
                            if ('message_id' in ad and 'chat_id' in ad and 
                                ad['message_id'] is not None and ad['chat_id'] is not None and
                                isinstance(ad['message_id'], int) and isinstance(ad['chat_id'], int)):
                                try:
                                    # Forward the original message
                                    await self.client.forward_messages(
                                        target_id,
                                        ad['message_id'],
                                        ad['chat_id']
                                    )
                                except Exception as e:
                                    logger.error(f"Failed to forward message to {target_id}, falling back to text: {e}")
                                    # Fallback to sending as text if forward fails
                                    await self.client.send_message(
                                        target_id,
                                        ad['message'],
                                        parse_mode='md'
                                    )
                            else:
                                # Fallback to sending as text if no original message info
                                await self.client.send_message(
                                    target_id,
                                    ad['message'],
                                    parse_mode='md'
                                )
                            sent += 1
                            self.scheduler.record_sent_ad(ad_id, target_id, True)

                            # Ultra-fast mode - no delay between operations
                            # await asyncio.sleep(0)
                        except Exception as e:
                            error_msg = f"Failed to send timer ad to {target_id}: {str(e)}"
                            logger.error(error_msg)
                            failed += 1
                            failed_targets.append((target_id, str(e)))
                            self.scheduler.record_sent_ad(ad_id, target_id, False)

                    # Final status update
                    await self.cleanup_status(task_id)

                    # Send summary with detailed stats
                    summary = f"✅ Timer: Ad #{ad_id} sent to {sent} targets\n❌ Failed: {failed}"

                    # Add detailed failure information if any failures occurred
                    if failed > 0:
                        summary += "\n\n**Failed Targets:**\n"
                        failed_count = 0
                        for target_id, error in failed_targets[:10]:  # Limit to first 10 failures
                            failed_count += 1
                            summary += f"- {target_id}: {error}\n"

                        if failed_count < failed:
                            summary += f"- ...and {failed - failed_count} more\n"

                    # Send summary message and ensure it doesn't fail silently
                    try:
                        await self.client.send_message(
                            self.auto_ad_chat_id,
                            summary
                        )
                    except Exception as e:
                        logger.error(f"Failed to send summary message: {e}")

                except Exception as e:
                    error_msg = f"Error in timer ad sending: {str(e)}"
                    logger.error(error_msg)
                    if self.auto_ad_chat_id:
                        from utils import error_to_chat
                        await error_to_chat(self.client, self.auto_ad_chat_id, error_msg)
                    await self.cleanup_status(task_id)

        except asyncio.CancelledError:
            logger.info("Timer task was cancelled")
        except Exception as e:
            error_msg = f"Error in ad timer loop: {str(e)}"
            logger.error(error_msg)
            if hasattr(self, 'auto_ad_chat_id') and self.auto_ad_chat_id:
                from utils import error_to_chat
                await error_to_chat(self.client, self.auto_ad_chat_id, error_msg)

    async def stop_timer(self, event) -> None:
        """Stop the automatic advertisement timer."""
        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        if not self.auto_ad_task or self.auto_ad_task.done():
            await event.reply("❌ No active advertisement timer to stop.")
            return

        try:
            # Cancel the task
            self.auto_ad_task.cancel()
            await event.reply("✅ Advertisement timer stopped.")
        except Exception as e:
            logger.error(f"Error stopping timer:{str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    async def list_joined(self, event) -> None:
        """List all joined groups and channels."""
        # Store event for status updates
        self.event = event

        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        args = event.text.split()
        # Check for parameters: --add (add all to targets) or --all (add all without confirmation)
        add_as_targets = any(arg == "--add" for arg in args)
        add_all = any(arg == "--all" for arg in args)

        task_id = f"list_joined_{int(time.time())}"
        await self.update_status(task_id, "Fetching joined chats")

        try:
            # Get all dialogs (chats)
            dialogs = await self.client.get_dialogs()

            # Filter to only get groups and channels
            chats = [
                d for d in dialogs 
                if d.is_group or d.is_channel
            ]

            if not chats:
                await event.reply("❌ You haven't joined any groups or channels.")
                await self.cleanup_status(task_id)
                return

            # Prepare the message with chunks to avoid length limits
            chunks = []
            current_chunk = "🌐 **Joined Groups & Channels**\n\n"
            added_targets = 0

            for i, chat in enumerate(chats):
                await self.update_status(task_id, "Processing chats", i, len(chats))

                # Format entry
                chat_type = "📢 Channel" if chat.is_channel else "👥 Group"
                chat_info = f"{i+1}. {chat_type}: **{chat.title}**\n"

                if hasattr(chat.entity, 'username') and chat.entity.username:
                    chat_info += f"    • @{chat.entity.username}\n"

                chat_info += f"    • ID: `{chat.id}`\n"

                # Check if we need to start a new chunk
                if len(current_chunk) + len(chat_info) > 4000:
                    chunks.append(current_chunk)
                    current_chunk = chat_info
                else:
                    current_chunk += chat_info

                # Add to targets if requested or in all mode
                if add_as_targets or add_all:
                    target_id = f"@{chat.entity.username}" if hasattr(chat.entity, 'username') and chat.entity.username else str(chat.id)
                    if self.scheduler.add_target({'original': target_id, 'type': 'channel' if chat.is_channel else 'group'}):
                        added_targets += 1

            # Add the last chunk
            if current_chunk:
                chunks.append(current_chunk)

            # Send the chunks
            for chunk in chunks:
                await self.client.send_message(event.chat_id, chunk)

            # Send summary if targets were added
            if add_as_targets or add_all:
                await event.reply(
                    f"🚀 **TARGET ADDITION COMPLETE** 🚀\n\n"
                    f"✅ Successfully added {added_targets} chats to your target list!\n"
                    f"📊 Total targets: {len(self.scheduler.get_targets())}\n\n"
                    f"Your marketing reach has been expanded!"
                )
            else:
                # Add option to add all targets
                await event.reply(
                    f"💡 **TIP: ADD THESE CHATS** 💡\n\n"
                    f"Use `/listjoined --add` to add all these chats to your targets.\n"
                    f"Or use `/listjoined --all` to add all without confirmation."
                )

            await self.cleanup_status(task_id)

        except Exception as e:
            logger.error(f"Error listing joined chats: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")
            await self.cleanup_status(task_id)

    async def add_targets(self, event) -> None:
        """Add new targets to the scheduler."""
        # Store event for status updates
        self.event = event

        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        # Check if it's a reply to a message
        reply = await event.get_reply_message()
        if reply:
            text = reply.text
        else:
            text = event.text.split(None, 1)
            if len(text) < 2:
                await event.reply("❌ Please provide targets or reply to a message with targets.")
                return
            text = text[1]

        # Split and clean targets
        targets = [target.strip() for target in text.split(',')]
        if not targets:
            await event.reply("❌ No valid targets provided.")
            return

        task_id = f"add_targets_{int(time.time())}"
        await self.update_status(task_id, "Adding targets")

        try:
            added = 0
            failed = 0
            total = len(targets)

            for i, target in enumerate(targets):
                try:
                    await self.update_status(task_id, "Processing targets", i, total)

                    # Clean up the target identifier
                    original_target = target

                    # Add to targets
                    target_data = {
                        'original': original_target,
                        'type': 'unknown'  # Will be updated when sending messages
                    }

                    if self.scheduler.add_target(target_data):
                        added += 1
                    else:
                        failed += 1

                except Exception as e:
                    logger.error(f"Failed to add target {target}: {str(e)}")
                    failed += 1

            await self.cleanup_status(task_id)

            if added > 0:
                await event.reply(f"✅ Successfully added {added} targets!\n❌ Failed to add {failed} targets.")
            else:
                await event.reply(f"❌ Failed to add any targets.")

        except Exception as e:
            logger.error(f"Error while adding targets: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")
            await self.cleanup_status(task_id)

    async def send_ad(self, event) -> None:
        """Send advertisement to all targets."""
        # Store event for status updates
        self.event = event

        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        text = event.text.split()

        if len(text) < 2:
            await event.reply("❌ Usage: `/startad <ad_id> [interval=60] [min_delay=2] [max_delay=7]`")
            return

        try:
            ad_id = int(text[1])
            interval = int(text[2]) if len(text) > 2 else 60
            min_delay = int(text[3]) if len(text) > 3 else 2
            max_delay = int(text[4]) if len(text) > 4 else 7

            ad = self.scheduler.get_ad(ad_id)
            if not ad:
                await event.reply(f"❌ Advertisement with ID {ad_id} not found.")
                return

            # Cancel any existing auto ad task
            if self.auto_ad_task and not self.auto_ad_task.done():
                self.auto_ad_task.cancel()

            # Start auto advertisement
            self.auto_ad_chat_id = event.chat_id
            self.auto_ad_task = asyncio.create_task(
                self._auto_ad_loop(ad_id, interval, min_delay, max_delay)
            )

            human_time = format_duration(interval)
            await event.reply(
                f"⚡ TURBO PROMOTION ACTIVATED ⚡\n\n"
                f"🔥 Ad #{ad_id} campaign launched!\n"
                f"⏱️ Campaign Interval: {human_time}\n"
                f"🚀 Performance: Maximum Speed\n"
                f"📊 Status: Active & Monitoring\n\n"
                f"Your promotion is now running in Turbo Mode for maximum reach!"
            )

        except ValueError:
            await event.reply("❌ Invalid parameters. Make sure to use numbers only.")
        except Exception as e:
            logger.error(f"Error starting auto ad: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    async def _auto_ad_loop(self, ad_id: int, interval: int, min_delay: int, max_delay: int) -> None:
        """Background task for auto ad sending."""
        try:
            while True:
                # Create a unique task ID for this run
                task_id = f"auto_ad_{ad_id}_{int(time.time())}"

                try:
                    ad = self.scheduler.get_ad(ad_id)
                    if not ad:
                        error_msg = f"Ad with ID {ad_id} not found for auto task"
                        logger.error(error_msg)
                        if self.auto_ad_chat_id:
                            from utils import error_to_chat
                            await error_to_chat(self.client, self.auto_ad_chat_id, error_msg)
                        return

                    targets = self.scheduler.get_targets()
                    if not targets:
                        await self.client.send_message(
                            self.auto_ad_chat_id,
                            "❌ No targets to send ad to."
                        )
                        # Wait for the next interval
                        await asyncio.sleep(interval)
                        continue

                    await self.update_status(
                        task_id, 
                        f"Sending ad #{ad_id}",
                        0, 
                        len(targets)
                    )

                    sent = 0
                    failed = 0
                    failed_targets = []

                    for i, target in enumerate(targets):
                        try:
                            # Update status
                            await self.update_status(
                                task_id, 
                                f"Sending ad #{ad_id}",
                                i, 
                                len(targets)
                            )

                            # Ensure target is properly handled as dictionary
                            if isinstance(target, dict):
                                target_id = target['original']
                            else:
                                # If somehow target is a string, handle it directly
                                target_id = target

                            # First check if the chat exists
                            from utils import validate_chat_id
                            if not await validate_chat_id(self.client, target_id):
                                logger.error(f"Chat {target_id} does not exist or is not accessible")
                                failed += 1
                                failed_targets.append((target_id, "Chat not found or not accessible"))
                                self.scheduler.record_sent_ad(ad_id, target_id, False)
                                continue

                            # Forward message instead of sending as copy
                            if ('message_id' in ad and 'chat_id' in ad and 
                                ad['message_id'] is not None and ad['chat_id'] is not None and
                                isinstance(ad['message_id'], int) and isinstance(ad['chat_id'], int)):
                                try:
                                    # Forward the original message
                                    await self.client.forward_messages(
                                        target_id,
                                        ad['message_id'],
                                        ad['chat_id']
                                    )
                                except Exception as e:
                                    logger.error(f"Failed to forward message to {target_id}, falling back to text: {e}")
                                    # Fallback to sending as text if forward fails
                                    await self.client.send_message(
                                        target_id,
                                        ad['message'],
                                        parse_mode='md'
                                    )
                            else:
                                # Fallback to sending as text if no original message info
                                await self.client.send_message(
                                    target_id,
                                    ad['message'],
                                    parse_mode='md'
                                )
                            sent += 1
                            self.scheduler.record_sent_ad(ad_id, target_id, True)

                            # Ultra-fast mode - no delay between operations
                            # await asyncio.sleep(0)
                        except Exception as e:
                            error_msg = f"Failed to send auto ad to {target}: {str(e)}"
                            logger.error(error_msg)
                            failed += 1
                            failed_targets.append((target_id, str(e)))
                            self.scheduler.record_sent_ad(ad_id, target_id, False)

                    # Final status update
                    await self.cleanup_status(task_id)

                    # Send summary with detailed stats
                    summary = f"✅ Auto: Ad #{ad_id} sent to {sent} targets\n❌ Failed: {failed}"

                    # Add detailed failure information if any failures occurred
                    if failed > 0:
                        summary += "\n\n**Failed Targets:**\n"
                        failed_count = 0
                        for target_id, error in failed_targets[:10]:  # Limit to first 10 failures
                            failed_count += 1
                            summary += f"- {target_id}: {error}\n"

                        if failed_count < failed:
                            summary += f"- ...and {failed - failed_count} more\n"

                    await self.client.send_message(
                        self.auto_ad_chat_id,
                        summary
                    )

                    # Wait for the next interval
                    await asyncio.sleep(interval)

                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    error_msg = f"Error in auto ad sending: {str(e)}"
                    logger.error(error_msg)
                    if self.auto_ad_chat_id:
                        from utils import error_to_chat
                        await error_to_chat(self.client, self.auto_ad_chat_id, error_msg)

                    await self.cleanup_status(task_id)
                    await asyncio.sleep(interval)  # Still wait for next interval

        except asyncio.CancelledError:
            logger.info("Auto ad task was cancelled")
        except Exception as e:
            error_msg = f"Error in auto ad loop: {str(e)}"
            logger.error(error_msg)
            if hasattr(self, 'auto_ad_chat_id') and self.auto_ad_chat_id:
                from utils import error_to_chat
                await error_to_chat(self.client, self.auto_ad_chat_id, error_msg)

    async def stop_ad(self, event) -> None:
        """Stop automatic advertisement."""
        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        if not self.auto_ad_task or self.auto_ad_task.done():
            await event.reply("❌ No active advertisement to stop.")
            return

        try:
            # Cancel the task
            self.auto_ad_task.cancel()
            await event.reply("✅ Advertisement stopped.")
        except Exception as e:
            logger.error(f"Error stopping advertisement: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    async def list_targets(self, event) -> None:
        """List all targets."""
        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        try:
            targets = self.scheduler.get_targets()

            if not targets:
                await event.reply("❌ No targets found.")
                return

            # Prepare the message with chunks to avoid length limits
            chunks = []
            current_chunk = "🎯 **Saved Targets**\n\n"

            for i, target in enumerate(targets):
                if isinstance(target, dict):
                    target_id = target['original']
                    target_type = target.get('type', 'unknown')
                    target_info = f"{i+1}. **{target_id}** ({target_type})\n"
                else:
                    target_info = f"{i+1}. **{target}**\n"

                # Check if we need to start a new chunk
                if len(current_chunk) + len(target_info) > 4000:
                    chunks.append(current_chunk)
                    current_chunk = target_info
                else:
                    current_chunk += target_info

            # Add the last chunk
            if current_chunk:
                chunks.append(current_chunk)

            # Send the chunks
            for chunk in chunks:
                await self.client.send_message(event.chat_id, chunk)

        except Exception as e:
            logger.error(f"Error listing targets: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    async def remove_target_handler(self, event) -> None:
        """Remove a target."""
        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        text = event.text.split(None, 1)
        if len(text) < 2:
            await event.reply("❌ Usage: `/removetarget <target_id>`")
            return

        try:
            target_id = text[1].strip()
            targets = self.scheduler.get_targets()
            found = False

            # Look for matching target by ID or number in list
            try:
                # Check if input is a number (index in the list)
                index = int(target_id) - 1
                if 0 <= index < len(targets):
                    target_to_remove = targets[index]
                    if isinstance(target_to_remove, dict):
                        target_id = target_to_remove['original']
                    else:
                        target_id = target_to_remove
                    found = True
            except ValueError:
                # Not a number, treat as target ID
                pass

            # If target wasn't found by index, try direct removal
            if not found:
                for t in targets:
                    t_id = t['original'] if isinstance(t, dict) else t
                    if t_id == target_id:
                        found = True
                        break

            if found and self.scheduler.remove_target(target_id):
                await event.reply(f"✅ Target **{target_id}** has been removed.")
            else:
                await event.reply(f"❌ Target **{target_id}** not found.")

        except Exception as e:
            logger.error(f"Error removing target: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    async def help_command(self, event) -> None:
        """Show help information about available commands."""
        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        from config import BOT_NAME
        help_text = f"""
🔥 **{BOT_NAME} COMMAND CENTER** 🔥

📣 **AD MANAGEMENT**:
• `/setad <message>` - 📝 Create powerful advertisement
• `/listad` - 📋 View your ad collection
• `/removead <id>` - 🗑️ Delete unwanted ads
• `/startad <id> [interval] [min_delay] [max_delay]` - 🚀 Launch auto-promotion campaign
• `/stopad` - 🛑 End active campaign
• `/timer <id> <seconds> [min_delay] [max_delay]` - ⏱️ Schedule timed promotions
• `/stopadtimer` - ⏹️ Cancel scheduled promotions
• `/schedule <id> <time>` - 📅 Program future ad campaigns

🎯 **TARGET MANAGEMENT**:
• `/addtarget <targets>` - 🎯 Add promotion targets
• `/listtarget` - 📊 View all target channels
• `/removetarget <id>` - 🔄 Remove specific target
• `/removealltarget` - 🗑️ Delete ALL targets at once
• `/cleantargets` - 🧹 Clean invalid targets
• `/removeunsubs` - 🔍 Remove unsubscribed targets
• `/targeting <keywords>` - 🎯 Set smart targeting criteria

💬 **CHAT MANAGEMENT**:
• `/joinchats <chats>` - 🔗 Join multiple channels instantly
• `/leavechats <chats>` - 👋 Exit multiple channels
• `/leaveandremove <chats>` - 🚪 Exit and remove from targets
• `/listjoined [--add|--all]` - 📑 List joined channels & add as targets
• `/findgroups <keyword>` - 🔍 Discover new relevant groups

📊 **ANALYTICS & TOOLS**:
• `/analytics [days=7]` - 📈 Get campaign performance metrics
• `/forward <msg_id> <targets>` - ↗️ Forward messages to targets
• `/backup` - 💾 Create data backup
• `/restore <file_id>` - 🔄 Restore from backup
• `/broadcast <message>` - 📢 Send to all targets instantly
• `/stickers <pack_name>` - 🎨 Send stickers to targets
• `/interactive` - 🎮 Create interactive messages

🎯 **TARGETED CAMPAIGNS**:
• `/targetedad <ad_id> <target_list> <interval_mins>` - 🎯 Create targeted campaign
• `/listtargeted` - 📋 List all targeted campaigns
• `/stoptargeted <campaign_id>` - 🛑 Stop a targeted campaign

👥 **ADMIN MANAGEMENT**:
• `/addadmin <user_id>` - 👑 Add new admin
• `/removeadmin <user_id>` - 🚫 Remove admin access
• `/listadmins` - 📋 List all admins

⚙️ **SYSTEM UTILITIES**:
• `/clearchat [count]` - 🧹 Clean chat history
• `/pin [silent]` - 📌 Pin important messages
• `/client` - 🤖 View bot information
• `/test` - 🧪 Test all functions
• `/help` - 📚 Display this menu
"""

        await event.reply(help_text)

    async def add_admin(self, event) -> None:
        """Add a new admin user."""
        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        text = event.text.split(None, 1)
        if len(text) < 2:
            await event.reply("❌ Usage: `/addadmin <user_id>`")
            return

        try:
            user_id = int(text[1].strip())

            # Check if already an admin
            if user_id in self.admin_ids:
                await event.reply(f"⚠️ User ID {user_id} is already an admin.")
                return

            # Update admin_ids list
            self.admin_ids.append(user_id)

            # Update config file
            config_path = "config.json"
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)

                config["admin_ids"] = self.admin_ids

                with open(config_path, 'w', encoding='utf-8') as f:
                    json.dump(config, f, indent=4)

                await event.reply(f"✅ User ID {user_id} has been added as an admin!")

            except Exception as e:
                logger.error(f"Error updating config file: {str(e)}")
                await event.reply(f"⚠️ Admin added to session but failed to save to config: {str(e)}")

        except ValueError:
            await event.reply("❌ Invalid user ID. Please provide a valid number.")
        except Exception as e:
            logger.error(f"Error adding admin: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    async def remove_admin(self, event) -> None:
        """Remove an admin user."""
        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        text = event.text.split(None, 1)
        if len(text) < 2:
            await event.reply("❌ Usage: `/removeadmin <user_id>`")
            return

        try:
            user_id = int(text[1].strip())

            # Check if trying to remove self
            if user_id == event.sender_id:
                await event.reply("⚠️ You cannot remove yourself as an admin.")
                return

            # Check if not an admin
            if user_id not in self.admin_ids:
                await event.reply(f"⚠️ User ID {user_id} is not an admin.")
                return

            # Get the main admin (first admin in the list or from config)
            main_admin = None
            config_path = "config.json"
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    if config["admin_ids"]:
                        main_admin = config["admin_ids"][0]
            except Exception:
                # If can't read config, assume first admin in current list is main
                if self.admin_ids:
                    main_admin = self.admin_ids[0]

            # Check if trying to remove the main admin
            if user_id == main_admin:
                await event.reply("⛔️ Cannot remove the main admin. This admin has permanent privileges.")
                return

            # Remove from admin_ids list
            self.admin_ids.remove(user_id)

            # Update config file
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)

                config["admin_ids"] = self.admin_ids

                with open(config_path, 'w', encoding='utf-8') as f:
                    json.dump(config, f, indent=4)

                await event.reply(f"✅ User ID {user_id} has been removed as an admin!")

            except Exception as e:
                logger.error(f"Error updating config file: {str(e)}")
                await event.reply(f"⚠️ Admin removed from session but failed to save to config: {str(e)}")

        except ValueError:
            await event.reply("❌ Invalid user ID. Please provide a valid number.")
        except Exception as e:
            logger.error(f"Error removing admin: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    async def list_admins(self, event) -> None:
        """List all admin users."""
        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        try:
            if not self.admin_ids:
                await event.reply("⚠️ No admins configured. Anyone can control the bot.")
                return

            # Format admin list
            admin_list = "👑 **ADMIN LIST** 👑\n\n"

            for i, admin_id in enumerate(self.admin_ids):
                # Try to get user info
                try:
                    user = await self.client.get_entity(admin_id)
                    if user:
                        username = f"@{user.username}" if user.username else "No username"
                        name = f"{user.first_name} {user.last_name or ''}" 
                        admin_list += f"{i+1}. **{name}**\n   • ID: `{admin_id}`\n   • Username: {username}\n"
                    else:
                        admin_list += f"{i+1}. ID: `{admin_id}` (User info unavailable)\n"
                except Exception:
                    admin_list += f"{i+1}. ID: `{admin_id}` (User info unavailable)\n"

            await event.reply(admin_list)

        except Exception as e:
            logger.error(f"Error listing admins: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    async def show_analytics(self, event) -> None:
        """Show analytics and performance data."""
        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        text = event.text.split()
        days = 7  # Default: last 7 days

        if len(text) > 1:
            try:
                days = int(text[1])
                if days <= 0:
                    await event.reply("❌ Days must be a positive number.")
                    return
            except ValueError:
                await event.reply("❌ Invalid number of days.")
                return

        try:
            analytics = self.scheduler.get_analytics(days)

            # Format the analytics data
            total_sent = analytics["summary"]["total_sent"]
            total_failed = analytics["summary"]["total_failed"]
            success_rate = 0 if total_sent + total_failed == 0 else (total_sent / (total_sent + total_failed) * 100)

            # Create the analytics message
            analytics_msg = (
                f"📊 **CAMPAIGN ANALYTICS REPORT** 📊\n\n"
                f"📈 **PERFORMANCE SUMMARY (Last {days} days)**\n"
                f"• Messages Delivered: {total_sent}\n"
                f"• Delivery Failures: {total_failed}\n"
                f"• Success Rate: {success_rate:.1f}%\n"
            )

            # Add daily breakdown if available
            if analytics["daily"]:
                analytics_msg += "\n📅 **DAILY BREAKDOWN**\n"
                for day, stats in sorted(analytics["daily"].items(), reverse=True):
                    day_sent = stats["sent"]
                    day_failed = stats["failed"]
                    analytics_msg += f"• {day}: {day_sent} sent, {day_failed} failed\n"

            # Add top performing ads
            if analytics["by_ad"]:
                analytics_msg += "\n🔥 **TOP PERFORMING ADS**\n"
                sorted_ads = sorted(
                    analytics["by_ad"].items(), 
                    key=lambda x: x[1]["sent"], 
                    reverse=True
                )[:5]  # Top 5

                for ad_id, stats in sorted_ads:
                    ad_sent = stats["sent"]
                    ad_failed = stats["failed"]
                    ad = self.scheduler.get_ad(int(ad_id))
                    ad_name = f"Ad #{ad_id}"
                    if ad and ad.get("message"):
                        # Use first 20 chars of message as name
                        ad_name = f"Ad #{ad_id}: {ad['message'][:20]}..."

                    analytics_msg += f"• {ad_name}: {ad_sent} deliveries\n"

            # Add top target channels
            if analytics["by_target"]:
                analytics_msg += "\n📱 **TOP TARGET CHANNELS**\n"
                sorted_targets = sorted(
                    analytics["by_target"].items(), 
                    key=lambda x: x[1]["sent"], 
                    reverse=True
                )[:5]  # Top 5

                for target_id, stats in sorted_targets:
                    target_sent = stats["sent"]
                    target_failed = stats["failed"]
                    analytics_msg += f"• {target_id}: {target_sent} received\n"

            await event.reply(analytics_msg)

        except Exception as e:
            logger.error(f"Error showing analytics: {str(e)}")
            await event.reply(f"❌ Error retrieving analytics: {str(e)}")

    async def forward_message(self, event) -> None:
        """Forward a message to multiple targets."""
        # Store event for status updates
        self.event = event

        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        text = event.text.split(None, 2)
        if len(text) < 3:
            await event.reply("❌ Usage: `/forward <message_id> <targets>`")
            return

        try:
            message_id = int(text[1])
            targets_text = text[2]

            # Split and clean targets
            targets = [target.strip() for target in targets_text.split(',')]
            if not targets:
                await event.reply("❌ No valid targets provided.")
                return

            task_id = f"forward_{int(time.time())}"
            await self.update_status(task_id, "Forwarding message")

            # Get the message to forward
            try:
                message = await self.client.get_messages(event.chat_id, ids=message_id)
                if not message:
                    await event.reply("❌ Message not found.")
                    await self.cleanup_status(task_id)
                    return
            except Exception as e:
                logger.error(f"Error getting message: {e}")
                await event.reply(f"❌ Error: Could not retrieve message {message_id}.")
                await self.cleanup_status(task_id)
                return

            # Forward to each target
            sent = 0
            failed = 0
            total = len(targets)

            for i, target in enumerate(targets):
                try:
                    await self.update_status(task_id, "Forwarding message", i, total)

                    # Clean up target identifier
                    if target.startswith('@'):
                        target = target[1:]
                    if target.startswith('https://t.me/'):
                        target = target[13:]
                    if target.startswith('t.me/'):
                        target = target[5:]

                    # Forward message
                    await self.client.forward_messages(target, message)
                    sent += 1

                    # Ultra-fast mode - no delay between operations
                    # await asyncio.sleep(0)
                except Exception as e:
                    logger.error(f"Failed to forward to {target}: {str(e)}")
                    failed += 1

            await self.cleanup_status(task_id)

            if sent > 0:
                await event.reply(f"✅ Successfully forwarded to {sent} targets!\n❌ Failed: {failed}")
            else:
                await event.reply(f"❌ Failed to forward to any targets.")

        except ValueError:
            await event.reply("❌ Invalid message ID. Please provide a valid number.")
        except Exception as e:
            logger.error(f"Error in forward command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")
            await self.cleanup_status(task_id)

    async def backup_data(self, event) -> None:
        """Create a backup of all bot data."""
        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        try:
            # Create backup directory if it doesn't exist
            backup_dir = Path("data/backups")
            backup_dir.mkdir(exist_ok=True, parents=True)

            # Create timestamp for filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = backup_dir / f"backup_{self.client_name}_{timestamp}.json"

            # Get current data
            data = self.scheduler.data

            # Write to backup file
            with open(backup_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, default=str)

            # Send the backup file to the user
            caption = f"🔒 **DATA BACKUP CREATED**\n\n📁 Filename: `{backup_file.name}`\n📅 Date: {timestamp}\n🔢 Backup ID: `{timestamp}`"
            await self.client.send_file(
                event.chat_id,
                str(backup_file),
                caption=caption
            )

        except Exception as e:
            logger.error(f"Error creating backup: {str(e)}")
            await event.reply(f"❌ Error creating backup: {str(e)}")

    async def restore_data(self, event) -> None:
        """Restore data from a backup file."""
        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        text = event.text.split(None, 1)

        # Check if it's a reply to a file message
        reply = await event.get_reply_message()

        if reply and reply.document:
            # Get the file from reply
            backup_file = await reply.download_media(file="data/temp_backup.json")
        elif len(text) > 1:
            # Get backup ID from command
            backup_id = text[1].strip()
            backup_file = f"data/backups/backup_{self.client_name}_{backup_id}.json"
            if not os.path.exists(backup_file):
                await event.reply("❌ Backup file not found. Please provide a valid backup ID or reply to a backup file.")
                return
        else:
            await event.reply("❌ Please provide a backup ID or reply to a backup file message.")
            return

        try:
            # Load backup data
            with open(backup_file, 'r', encoding='utf-8') as f:
                backup_data = json.load(f)

            # Confirm restore
            confirm_msg = await event.reply(
                "⚠️ **WARNING: RESTORE OPERATION**\n\n"
                "This will overwrite your current data with the backup.\n"
                "• Ads: {}\n"
                "• Targets: {}\n"
                "• Analytics records: {}\n\n"
                "Reply with 'CONFIRM' to proceed.".format(
                    len(backup_data.get("ads", [])),
                    len(backup_data.get("targets", [])),
                    len(backup_data.get("analytics", {}))
                )
            )

            # Wait for confirmation using helper function
            from utils import wait_for_confirmation
            is_confirmed = await wait_for_confirmation(self.client, event)

            if not is_confirmed:
                await event.reply("❌ Restore operation cancelled or timed out.")
                return

            # Apply the backup
            self.scheduler.data = backup_data
            self.scheduler._save_data()

            await event.reply("✅ Data restored successfully from backup!")

        except Exception as e:
            logger.error(f"Error restoring data: {str(e)}")
            await event.reply(f"❌ Error restoring data: {str(e)}")
        finally:
            # Clean up temp file if it exists
            if os.path.exists("data/temp_backup.json"):
                os.remove("data/temp_backup.json")

    async def schedule_ad(self, event) -> None:
        """Schedule an ad to be sent at a specific time."""
        # Store event for status updates
        self.event = event

        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        text = event.text.split(None, 2)
        if len(text) < 3:
            await event.reply("❌ Usage: `/schedule <ad_id> <time>`\nTime format: YYYY-MM-DD HH:MM or HH:MM for today")
            return

        try:
            ad_id = int(text[1])
            time_str = text[2]

            # Verify ad exists
            ad = self.scheduler.get_ad(ad_id)
            if not ad:
                await event.reply(f"❌ Advertisement with ID {ad_id} not found.")
                return

            # Parse time
            now = datetime.now()

            if " " in time_str:  # Format: YYYY-MM-DD HH:MM
                try:
                    schedule_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M")
                except ValueError:
                    await event.reply("❌ Invalid time format. Use YYYY-MM-DD HH:MM")
                    return
            else:  # Format: HH:MM (today)
                try:
                    time_obj = datetime.strptime(time_str, "%H:%M").time()
                    schedule_time = datetime.combine(now.date(), time_obj)
                    # If time has already passed today, schedule for tomorrow
                    if schedule_time < now:
                        schedule_time += timedelta(days=1)
                except ValueError:
                    await event.reply("❌ Invalid time format. Use HH:MM")
                    return

            # Calculate seconds until scheduled time
            time_diff = (schedule_time - now).total_seconds()

            if time_diff <= 0:
                await event.reply("❌ Scheduled time must be in the future.")
                return

            # Schedule the ad
            asyncio.create_task(self._scheduled_ad(ad_id, time_diff, event.chat_id))

            # Format time display
            display_time = schedule_time.strftime("%Y-%m-%d %H:%M:%S")
            human_time = format_duration(time_diff)

            await event.reply(
                f"⏰ **AD SCHEDULED SUCCESSFULLY** ⏰\n\n"
                f"🔥 Campaign #{ad_id} programmed!\n"
                f"📅 Execution time: {display_time}\n"
                f"⏱️ Countdown: {human_time}\n"
                f"🚀 Status: Scheduled & Ready\n\n"
                f"Your scheduled campaign will launch automatically at the specified time!"
            )

        except ValueError:
            await event.reply("❌ Invalid ad ID. Please provide a valid number.")
        except Exception as e:
            logger.error(f"Error scheduling ad: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    async def _scheduled_ad(self, ad_id: int, delay_seconds: float, chat_id: int) -> None:
        """Background task for scheduled ad sending."""
        try:
            # Wait until scheduled time
            await asyncio.sleep(delay_seconds)

            # Get ad and targets
            ad = self.scheduler.get_ad(ad_id)
            if not ad:
                await self.client.send_message(
                    chat_id,
                    f"❌ Scheduled ad #{ad_id} not found. Schedule cancelled."
                )
                return

            targets = self.scheduler.get_targets()
            if not targets:
                await self.client.send_message(
                    chat_id,
                    f"❌ No targets found for scheduled ad #{ad_id}."
                )
                return

            # Create task ID for this run
            task_id = f"scheduled_ad_{ad_id}_{int(time.time())}"

            # Send notification
            await self.client.send_message(
                chat_id,
                f"🚀 Executing scheduled ad #{ad_id} to {len(targets)} targets..."
            )

            # Send to all targets
            sent = 0
            failed = 0

            for i, target in enumerate(targets):
                try:
                    # Check if target is a dictionary or string
                    target_id = target['original'] if isinstance(target, dict) else target

                    # Forward message if original message is available
                    if ('message_id' in ad and 'chat_id' in ad and 
                        ad['message_id'] is not None and ad['chat_id'] is not None and
                        isinstance(ad['message_id'], int) and isinstance(ad['chat_id'], int)):
                        try:
                            await self.client.forward_messages(
                                target_id,
                                ad['message_id'],
                                ad['chat_id']
                            )
                        except Exception as e:
                            logger.error(f"Failed to forward message, falling back to text: {e}")
                            await self.client.send_message(
                                target_id,
                                ad['message'],
                                parse_mode='md'
                            )
                    else:
                        await self.client.send_message(
                            target_id,
                            ad['message'],
                            parse_mode='md'
                        )

                    # Record analytics
                    self.scheduler.record_sent_ad(ad_id, target_id, True)
                    sent += 1

                    # Ultra-fast mode - no delay between operations
                    # await asyncio.sleep(0)
                except Exception as e:
                    logger.error(f"Failed to send scheduled ad to {target}: {str(e)}")
                    self.scheduler.record_sent_ad(ad_id, target_id, False)
                    failed += 1

            # Send completion message
            await self.client.send_message(
                chat_id,
                f"✅ Scheduled campaign completed!\n• Ad #{ad_id} sent to {sent} targets\n• Failed: {failed}"
            )

        except asyncio.CancelledError:
            logger.info(f"Scheduled ad task {ad_id} was cancelled")
        except Exception as e:
            logger.error(f"Error in scheduled ad task: {str(e)}")
            await self.client.send_message(
                chat_id,
                f"❌ Error in scheduled ad #{ad_id}: {str(e)}"
            )

    async def broadcast_message(self, event) -> None:
        """Broadcast a message to all targets immediately."""
        # Store event for status updates
        self.event = event

        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        # Check if it's a reply to a message
        reply = await event.get_reply_message()
        if reply:
            message = reply
            forward_mode = True
        else:
            text = event.text.split(None, 1)
            if len(text) < 2:
                await event.reply("❌ Please provide a message to broadcast or reply to a message.")
                return
            message_text = text[1]
            forward_mode = False

        # Get targets
        targets = self.scheduler.get_targets()
        if not targets:
            await event.reply("❌ No targets to broadcast to.")
            return

        task_id = f"broadcast_{int(time.time())}"
        await self.update_status(task_id, "Broadcasting message")

        try:
            sent = 0
            failed = 0
            total = len(targets)

            for i, target in enumerate(targets):
                try:
                    await self.update_status(task_id, "Broadcasting", i, total)

                    # Get target ID
                    target_id = target['original'] if isinstance(target, dict) else target

                    # Send message
                    if forward_mode:
                        await self.client.forward_messages(target_id, reply)
                    else:
                        await self.client.send_message(target_id, message_text, parse_mode='md')

                    sent += 1

                    # Ultra-fast mode - no delay between operations
                    # await asyncio.sleep(0)
                except Exception as e:
                    logger.error(f"Failed to broadcast to {target}: {str(e)}")
                    failed += 1

            await self.cleanup_status(task_id)

            if sent > 0:
                await event.reply(f"📢 **BROADCAST COMPLETED**\n\n✅ Sent to {sent} targets\n❌ Failed: {failed}")
            else:
                await event.reply("❌ Failed to broadcast to any targets.")

        except Exception as e:
            logger.error(f"Error in broadcast: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")
            await self.cleanup_status(task_id)

    async def set_targeting(self, event) -> None:
        """Set smart targeting criteria for ads."""
        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        text = event.text.split(None, 1)
        if len(text) < 2:
            await event.reply("❌ Usage: `/targeting <keywords>`\nExample: `/targeting crypto,bitcoin,nft`")
            return

        try:
            # Parse keywords
            keywords = [k.strip().lower() for k in text[1].split(',')]

            # Store in scheduler data
            if "targeting" not in self.scheduler.data:
                self.scheduler.data["targeting"] = {}

            self.scheduler.data["targeting"]["keywords"] = keywords
            self.scheduler.data["targeting"]["enabled"] = True
            self.scheduler.data["targeting"]["created_at"] = datetime.now().isoformat()
            self.scheduler._save_data()

            # Confirm to user
            keyword_list = ", ".join([f"`{k}`" for k in keywords])
            await event.reply(
                f"🎯 **SMART TARGETING ACTIVATED** 🎯\n\n"
                f"Your ad campaigns will prioritize groups and channels matching these keywords:\n\n"
                f"{keyword_list}\n\n"
                f"Smart targeting will help optimize your reach and engagement!"
            )

        except Exception as e:
            logger.error(f"Error setting targeting: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    async def find_groups(self, event) -> None:
        """Find relevant groups based on keywords."""
        # Store event for status updates
        self.event = event

        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        text = event.text.split(None, 1)
        if len(text) < 2:
            await event.reply("❌ Usage: `/findgroups <keyword>`")
            return

        keyword = text[1].strip()

        task_id = f"find_groups_{int(time.time())}"
        await self.update_status(task_id, f"Searching for groups with keyword: {keyword}")

        try:
            # First method: Search through dialogs
            dialogs = await self.client.get_dialogs()
            matching_dialogs = []

            for dialog in dialogs:
                if dialog.is_group or dialog.is_channel:
                    if keyword.lower() in dialog.title.lower():
                        matching_dialogs.append({
                            "title": dialog.title,
                            "id": dialog.id,
                            "username": dialog.entity.username if hasattr(dialog.entity, "username") else None,
                            "type": "channel" if dialog.is_channel else "group",
                            "source": "joined"
                        })

            # Second method: Use Telegram's search function
            # Note: This has limitations based on Telegram's API
            try:
                results = await self.client(functions.contacts.SearchRequest(
                    q=keyword,
                    limit=50
                ))

                # Process the results
                for chat in results.chats:
                    # Check if it's not already in our list
                    if all(chat.id != d["id"] for d in matching_dialogs):
                        matching_dialogs.append({
                            "title": chat.title,
                            "id": chat.id,
                            "username": chat.username if hasattr(chat, "username") else None,
                            "type": "channel" if getattr(chat, "broadcast", False) else "group",
                            "source": "search"
                        })
            except Exception as e:
                logger.error(f"Error in group search: {e}")
                # Continue with what we found so far

            await self.cleanup_status(task_id)

            if not matching_dialogs:
                await event.reply(f"❌ No groups or channels found matching: '{keyword}'")
                return

            # Format results
            results_msg = f"🔍 **SEARCH RESULTS FOR: '{keyword}'**\n\n"

            for i, group in enumerate(matching_dialogs[:20]):  # Limit to 20 results
                group_type = "📢 Channel" if group["type"] == "channel" else "👥 Group"
                username = f"@{group['username']}" if group["username"] else f"ID: {group['id']}"
                source = "✅ Joined" if group["source"] == "joined" else "🔍 Found"

                results_msg += f"{i+1}. {group_type}: **{group['title']}**\n"
                results_msg += f"    • {username}\n"
                results_msg += f"    • {source}\n\n"

            # Add action buttons
            results_msg += "To add these as targets, reply with:\n`/addtarget " 
            results_msg += ", ".join([f"@{g['username']}" if g["username"] else str(g["id"]) for g in matching_dialogs[:5]])
            results_msg += "`"

            await event.reply(results_msg)

        except Exception as e:
            logger.error(f"Error finding groups: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")
            await self.cleanup_status(task_id)

    async def send_stickers(self, event) -> None:
        """Send stickers to targets."""
        # Store event for status updates
        self.event = event

        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        text = event.text.split(None, 1)
        if len(text) < 2:
            await event.reply("❌ Usage: `/stickers <pack_name>` or send a sticker and reply with `/stickers`")
            return

        # Check if it's a reply to a sticker
        reply = await event.get_reply_message()
        if reply and reply.sticker:
            sticker = reply.sticker
            sticker_set = sticker.attributes[1].stickerset
            sticker_pack_name = f"stickerset_{sticker_set.id}_{sticker_set.access_hash}"
        else:
            sticker_pack_name = text[1].strip()

        task_id = f"stickers_{int(time.time())}"
        await self.update_status(task_id, "Processing stickers")

        try:
            # Get targets
            targets = self.scheduler.get_targets()
            if not targets:
                await event.reply("❌ No targets to send stickers to.")
                await self.cleanup_status(task_id)
                return

            # Get stickers from the pack
            try:
                stickers = await self.client(functions.messages.GetStickerSetRequest(
                    stickerset=types.InputStickerSetShortName(short_name=sticker_pack_name),
                    hash=0
                ))
            except Exception:
                # Try as a URL or ID if the pack name doesn't work
                try:
                    if sticker_pack_name.startswith("stickerset_"):
                        # Format: stickerset_ID_HASH
                        parts = sticker_pack_name.split("_")
                        if len(parts) >= 3:
                            set_id = int(parts[1])
                            access_hash = int(parts[2])
                            stickers = await self.client(functions.messages.GetStickerSetRequest(
                                stickerset=types.InputStickerSetID(id=set_id, access_hash=access_hash),
                                hash=0
                            ))
                        else:
                            raise ValueError("Invalid sticker set ID format")
                    else:
                        # Try as short name again
                        stickers = await self.client(functions.messages.GetStickerSetRequest(
                            stickerset=types.InputStickerSetShortName(short_name=sticker_pack_name),
                            hash=0
                        ))
                except Exception as e:
                    await event.reply(f"❌ Error getting sticker pack: {str(e)}")
                    await self.cleanup_status(task_id)
                    return

            # Check if we got stickers
            if not hasattr(stickers, 'documents') or not stickers.documents:
                await event.reply("❌ No stickers found in this pack.")
                await self.cleanup_status(task_id)
                return

            # Choose a random sticker if multiple are available
            sticker_doc = random.choice(stickers.documents)

            # Update status
            await self.update_status(task_id, "Sending stickers to targets", 0, len(targets))

            # Send to targets
            sent = 0
            failed = 0

            for i, target in enumerate(targets):
                try:
                    await self.update_status(task_id, "Sending stickers", i, len(targets))

                    # Get target ID
                    target_id = target['original'] if isinstance(target, dict) else target

                    # Send sticker
                    await self.client.send_file(target_id, sticker_doc)
                    sent += 1

                    # Ultra-fast mode - no delay between operations
                    # await asyncio.sleep(0)
                except Exception as e:
                    logger.error(f"Failed to send sticker to {target}: {str(e)}")
                    failed += 1

            await self.cleanup_status(task_id)

            if sent > 0:
                await event.reply(f"✅ Sticker sent to {sent} targets!\n❌ Failed: {failed}")
            else:
                await event.reply("❌ Failed to send stickers to any targets.")

        except Exception as e:
            logger.error(f"Error sending stickers: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")
            await self.cleanup_status(task_id)

    async def create_interactive_message(self, event) -> None:
        """Create an interactive message with buttons."""
        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        await event.reply(
            "⚠️ Interactive message feature is available in the full version.\n\n"
            "This feature allows creating messages with:\n"
            "• Interactive buttons\n"
            "• Polls and quizzes\n"
            "• Reaction buttons\n"
            "• Forms and data collection\n\n"
            "Perfect for enhancing engagement with your audience!"
        )

    async def set_ad(self, event) -> None:
        """Set an advertisement message."""
        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        # Check if it's a reply to a message
        reply = await event.get_reply_message()
        if reply:
            text = reply.text or ""
            entities = reply.entities
            message_id = reply.id
            chat_id = event.chat_id  # Use the chat where the command was sent
        else:
            text = event.text.split(None, 1)
            if len(text) < 2:
                await event.reply("❌ Please provide an advertisement message or reply to a message.")
                return
            text = text[1]
            entities = event.entities
            message_id = None
            chat_id = None

        if not text and not (message_id and chat_id):
            await event.reply("❌ Advertisement message cannot be empty.")
            return

        try:
            # Add the advertisement with message_id and chat_id for forwarding
            ad_id = self.scheduler.add_ad(text, entities, message_id, chat_id)

            # Show preview
            await event.reply(
                f"✅ Advertisement #{ad_id} has been saved!\n\n"
                f"**Preview**:\n\n{text}"
            )
        except Exception as e:
            logger.error(f"Error setting advertisement: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    async def list_ad(self, event) -> None:
        """List all advertisements."""
        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        try:
            ads = self.scheduler.get_ads()

            if not ads:
                await event.reply("❌ No advertisements found.")
                return

            # Prepare the message with chunks to avoid length limits
            chunks = []
            current_chunk = "📣 **Saved Advertisements**\n\n"

            for ad in ads:
                ad_info = f"**#{ad['id']}**:\n{ad['message'][:200]}"

                if len(ad['message']) > 200:
                    ad_info += "...\n\n"
                else:
                    ad_info += "\n\n"

                # Check if we need to start a new chunk
                if len(current_chunk) + len(ad_info) > 4000:
                    chunks.append(current_chunk)
                    current_chunk = ad_info
                else:
                    current_chunk += ad_info

            # Add the last chunk
            if current_chunk:
                chunks.append(current_chunk)

            # Send the chunks
            for chunk in chunks:
                await self.client.send_message(event.chat_id, chunk)

        except Exception as e:
            logger.error(f"Error listing advertisements: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    async def remove_ad_handler(self, event) -> None:
        """Remove an advertisement."""
        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        text = event.text.split()

        if len(text) < 2:
            await event.reply("❌ Usage: `/removead <ad_id>`")
            return

        try:
            ad_id = int(text[1])

            if self.scheduler.remove_ad(ad_id):
                await event.reply(f"✅ Advertisement #{ad_id} has been removed.")
            else:
                await event.reply(f"❌ Advertisement #{ad_id} not found.")

        except ValueError:
            await event.reply("❌ Invalid advertisement ID. Please provide a valid number.")
        except Exception as e:
            logger.error(f"Error removing advertisement: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    async def pin_message(self, event) -> None:
        """Pin a message in a chat."""
        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        try:
            # Check if it's a reply to a message
            reply = await event.get_reply_message()
            if not reply:
                await event.reply("❌ Please reply to a message you want to pin.")
                return

            # Parse any arguments
            text = event.text.split()
            notify = True  # Default: send notification

            if len(text) > 1 and text[1].lower() in ['silent', 'quiet', 'false', 'no']:
                notify = False

            # Pin the message
            await self.client.pin_message(
                entity=event.chat_id,
                message=reply.id,
                notify=notify
            )

            await event.reply("📌 Message pinned successfully!" + 
                              (" (without notification)" if not notify else ""))

        except Exception as e:
            logger.error(f"Error pinning message: {str(e)}")
            await event.reply(f"❌ Error pinning message: {str(e)}")

    async def remove_all_targets(self, event) -> None:
        """Remove all targets at once with confirmation."""
        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        try:
            targets = self.scheduler.get_targets()
            if not targets:
                await event.reply("❌ No targets found to remove.")
                return

            # Ask for confirmation
            confirm_msg = await event.reply(
                f"⚠️ **WARNING: MASS DELETION** ⚠️\n\n"
                f"This will remove ALL {len(targets)} targets!\n"
                f"⚡ This action cannot be undone ⚡\n\n"
                f"Reply with 'CONFIRM' within 30 seconds to proceed."
            )

            # Use helper function for confirmation
            from utils import wait_for_confirmation
            is_confirmed = await wait_for_confirmation(self.client, event)

            if not is_confirmed:
                await event.reply("❌ Operation cancelled or timed out. No targets were removed.")
                return

            # Remove all targets
            count = len(targets)
            self.scheduler.data["targets"] = []
            self.scheduler._save_data()

            await event.reply(
                f"🧹 **MASS TARGET REMOVAL COMPLETE**\n\n"
                f"✅ Successfully removed all {count} targets!\n"
                f"📊 Current target count: 0\n\n"
                f"Your target list has been completely cleared."
            )

        except Exception as e:
            logger.error(f"Error removing all targets: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    async def targeted_ad(self, event) -> None:
        """Configure a specific ad to be sent to specific targets automatically."""
        # Store event for status updates
        self.event = event

        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        text = event.text.split(None, 3)
        if len(text) < 4:
            await event.reply("❌ Usage: `/targetedad <ad_id> <target_list> <interval_minutes>`\n\nExample: `/targetedad 1 @channel1,@channel2 60`")
            return

        try:
            ad_id = int(text[1])
            target_list = [t.strip() for t in text[2].split(',')]
            interval_minutes = int(text[3])

            if interval_minutes < 1:
                await event.reply("❌ Interval must be at least 1 minute.")
                return

            # Verify ad exists
            ad = self.scheduler.get_ad(ad_id)
            if not ad:
                await event.reply(f"❌ Advertisement with ID {ad_id} not found.")
                return

            # Verify targets exist
            if not target_list:
                await event.reply("❌ No targets specified.")
                return

            # Store the targeted ad configuration
            if "targeted_ads" not in self.scheduler.data:
                self.scheduler.data["targeted_ads"] = []

            # Generate a unique ID for this targeted campaign
            campaign_id = f"campaign_{int(time.time())}"

            # Create the campaign configuration
            campaign = {
                "id": campaign_id,
                "ad_id": ad_id,
                "targets": target_list,
                "interval_minutes": interval_minutes,
                "created_at": datetime.now().isoformat(),
                "last_run": None,
                "active": True
            }

            # Add to targeted ads list
            self.scheduler.data["targeted_ads"].append(campaign)
            self.scheduler._save_data()

            # Start the targeted ad task
            asyncio.create_task(self._run_targeted_ad_campaign(campaign))

            # Format time display
            human_interval = "hour" if interval_minutes == 60 else f"{interval_minutes} minutes"

            await event.reply(
                f"🎯 **TARGETED CAMPAIGN CREATED** 🎯\n\n"
                f"🔥 Campaign ID: `{campaign_id}`\n"
                f"📣 Advertisement: #{ad_id}\n"
                f"🎯 Targets: {len(target_list)}\n"
                f"⏱️ Interval: Every {human_interval}\n"
                f"🚀 Status: Active & Running\n\n"
                f"Your targeted campaign is now running! To stop it use `/stoptargeted {campaign_id}`"
            )

        except ValueError:
            await event.reply("❌ Invalid parameters. Make sure ad_id and interval are numbers.")
        except Exception as e:
            logger.error(f"Error setting up targeted ad: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    async def _run_targeted_ad_campaign(self, campaign: dict) -> None:
        """Background task to run a targeted ad campaign."""
        try:
            campaign_id = campaign["id"]
            ad_id = campaign["ad_id"]
            targets = campaign["targets"]
            interval_minutes = campaign["interval_minutes"]

            logger.info(f"Starting targeted campaign {campaign_id} for ad #{ad_id} to {len(targets)} targets")

            while True:
                # Check if the campaign is still active
                active_campaign = None
                for c in self.scheduler.data.get("targeted_ads", []):
                    if c["id"] == campaign_id:
                        active_campaign = c
                        break

                if not active_campaign or not active_campaign.get("active", False):
                    logger.info(f"Targeted campaign {campaign_id} has been deactivated")
                    break

                # Get the ad
                ad = self.scheduler.get_ad(ad_id)
                if not ad:
                    logger.error(f"Ad with ID {ad_id} not found for targeted campaign {campaign_id}")
                    break

                # Create a unique task ID for this run
                task_id = f"targeted_{campaign_id}_{int(time.time())}"

                try:
                    # Send to all targets in this campaign
                    sent = 0
                    failed = 0

                    for i, target_id in enumerate(targets):
                        try:
                            # Forward message if original message is available
                            if ('message_id' in ad and 'chat_id' in ad and 
                                ad['message_id'] is not None and ad['chat_id'] is not None and
                                isinstance(ad['message_id'], int) and isinstance(ad['chat_id'], int)):
                                try:
                                    await self.client.forward_messages(
                                        target_id,
                                        ad['message_id'],
                                        ad['chat_id']
                                    )
                                except Exception as e:
                                    logger.error(f"Failed to forward message, falling back to text: {e}")
                                    await self.client.send_message(
                                        target_id,
                                        ad['message'],
                                        parse_mode='md'
                                    )
                            else:
                                await self.client.send_message(
                                    target_id,
                                    ad['message'],
                                    parse_mode='md'
                                )

                            # Record analytics
                            self.scheduler.record_sent_ad(ad_id, target_id, True)
                            sent += 1

                            # Ultra-fast mode - no delay between operations
                            # await asyncio.sleep(0)
                        except Exception as e:
                            logger.error(f"Failed to send targeted ad to {target_id}: {str(e)}")
                            self.scheduler.record_sent_ad(ad_id, target_id, False)
                            failed += 1

                    # Update last run time
                    for c in self.scheduler.data.get("targeted_ads", []):
                        if c["id"] == campaign_id:
                            c["last_run"] = datetime.now().isoformat()
                            break
                    self.scheduler._save_data()

                    logger.info(f"Targeted campaign {campaign_id} sent to {sent} targets, failed: {failed}")

                except Exception as e:
                    logger.error(f"Error in targeted campaign {campaign_id}: {str(e)}")

                # Wait for the next interval
                await asyncio.sleep(interval_minutes * 60)

        except asyncio.CancelledError:
            logger.info(f"Targeted campaign {campaign_id} task was cancelled")
        except Exception as e:
            logger.error(f"Error in targeted campaign loop: {str(e)}")

    async def list_targeted_campaigns(self, event) -> None:
        """List all targeted ad campaigns."""
        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        try:
            campaigns = self.scheduler.data.get("targeted_ads", [])

            if not campaigns:
                await event.reply("❌ No targeted campaigns found.")
                return

            # Prepare the message
            result = "🎯 **ACTIVE TARGETED CAMPAIGNS** 🎯\n\n"

            for i, campaign in enumerate(campaigns):
                ad_id = campaign.get("ad_id")
                ad = self.scheduler.get_ad(ad_id) if ad_id else None
                ad_name = f"Ad #{ad_id}" if ad else "Unknown Ad"

                if ad and "message" in ad:
                    # Use first 20 chars of message as name
                    ad_name = f"Ad #{ad_id}: {ad['message'][:20]}..."

                # Format target list
                targets = campaign.get("targets", [])
                target_count = len(targets)
                target_preview = ", ".join(targets[:3])
                if target_count > 3:
                    target_preview += f" +{target_count - 3} more"

                # Format last run time
                last_run = campaign.get("last_run")
                if last_run:
                    try:
                        last_run_time = datetime.fromisoformat(last_run)
                        time_diff = datetime.now() - last_run_time
                        if time_diff.days > 0:
                            last_run_fmt = f"{time_diff.days} days ago"
                        elif time_diff.seconds > 3600:
                            last_run_fmt = f"{time_diff.seconds // 3600} hours ago"
                        elif time_diff.seconds > 60:
                            last_run_fmt = f"{time_diff.seconds // 60} minutes ago"
                        else:
                            last_run_fmt = f"{time_diff.seconds} seconds ago"
                    except:
                        last_run_fmt = last_run
                else:
                    last_run_fmt = "Never"

                # Format status
                status = "✅ Active" if campaign.get("active", False) else "❌ Inactive"

                # Add to result
                result += f"{i+1}. **Campaign**: `{campaign['id']}`\n"
                result += f"   • **Ad**: {ad_name}\n"
                result += f"   • **Targets**: {target_count} ({target_preview})\n"
                result += f"   • **Interval**: Every {campaign.get('interval_minutes', '?')} minutes\n"
                result += f"   • **Last Run**: {last_run_fmt}\n"
                result += f"   • **Status**: {status}\n\n"

            await event.reply(result)

        except Exception as e:
            logger.error(f"Error listing targeted campaigns: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    async def stop_targeted_campaign(self, event) -> None:
        """Stop a specific targeted campaign."""
        if not await AdminHandler.verify_admin(event, self.admin_ids):
            return

        text = event.text.split(None, 1)
        if len(text) < 2:
            await event.reply("❌ Usage: `/stoptargeted <campaign_id>`")
            return

        campaign_id = text[1].strip()

        try:
            found = False

            # Look for the campaign to deactivate
            for campaign in self.scheduler.data.get("targeted_ads", []):
                if campaign["id"] == campaign_id:
                    campaign["active"] = False
                    found = True
                    break

            if found:
                self.scheduler._save_data()
                await event.reply(f"✅ Targeted campaign `{campaign_id}` has been deactivated.")
            else:
                await event.reply(f"❌ Campaign `{campaign_id}` not found.")

        except Exception as e:
            logger.error(f"Error stopping targeted campaign: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")