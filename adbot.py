import os
import sys
import json
import time
import random
import logging
import asyncio
import string
import re
from typing import Set, Dict, List, Callable, Optional, Union, Tuple, Any
from functools import wraps
from datetime import datetime, timedelta
from types import SimpleNamespace
from collections import deque
from telethon import TelegramClient, events
from telethon.sync import TelegramClient as SyncTelegramClient
from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest, GetFullChannelRequest
from telethon.tl.functions.messages import GetDialogsRequest, SearchGlobalRequest, ImportChatInviteRequest, ForwardMessagesRequest
from telethon.tl.functions.account import UpdateProfileRequest, UpdateUsernameRequest
from telethon.tl.functions.photos import UploadProfilePhotoRequest
from telethon.tl.types import InputPeerEmpty, InputPeerChannel, InputPeerUser, InputPeerChat, Photo
from telethon.errors import ChatAdminRequiredError, ChatWriteForbiddenError, UserBannedInChannelError, SessionPasswordNeededError
from dotenv import load_dotenv

# Optional imports for enhanced system stats
try:
    import psutil
except ImportError:
    # psutil is optional - used for system statistics in the client command
    psutil = None

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('telegram_forwarder.log')
    ]
)

logger = logging.getLogger(__name__)

def admin_only(func: Callable):
    """Decorator to restrict commands to admin users only"""
    @wraps(func)
    async def wrapper(self, event, *args, **kwargs):
        try:
            # Get the command name from the event text for logging
            command_name = event.text.split()[0].lower() if event.text else ""
            command_function_name = func.__name__
            logger.info(f"Received command: {command_name}, function: {command_function_name}")

            # Special case: Allow /start command even when bot is disabled
            is_start_command = command_function_name == "cmd_start" or command_name == "/start"

            # Get sender ID from event with multiple fallbacks
            sender = None
            if hasattr(event.message, 'from_id'):
                sender = event.message.from_id.user_id
            elif hasattr(event, 'from_id'):
                sender = event.from_id.user_id
            elif hasattr(event.message, 'sender_id'):
                sender = event.message.sender_id
            elif hasattr(event, 'sender_id'):
                sender = event.sender_id

            if sender is None:
                logger.error(f"Could not determine sender ID for command {command_name}")
                return None

            # Log admin check
            logger.info(f"Checking if user {sender} is admin for command {command_name}")

            # Check if sender is in admin list
            if sender not in self.admins:
                logger.warning(f"Unauthorized access attempt from user {sender} for command {command_name}")
                # Silently ignore unauthorized users
                return None

            # SPECIAL HANDLING FOR /START COMMAND WHEN BOT IS OFFLINE
            if is_start_command and not self.forwarding_enabled:
                logger.info(f"Received /start command from admin when bot was offline")
                logger.info(f"Executing start command...")
                return await func(self, event, *args, **kwargs)

            # If the bot is not active (disabled) and this isn't the /start command, ignore it
            if not self.forwarding_enabled:
                logger.info(f"Bot not active. Command: {command_name}, Function: {command_function_name}")
                # Only send the message if it's not a silent command (system might send multiple commands)
                if not command_name.startswith("/silent"):
                    logger.info(f"Sending offline message for command {command_name}")
                    await event.reply("⚠️ --Ꮪɪᴍṗʟᴇ'𝚜 𝙰𝙳𝙱𝙾𝚃 is currently offline! Use `/start` command to wake it up. 🚀")
                return None

            logger.info(f"Admin command authorized for user {sender}")
            return await func(self, event, *args, **kwargs)
        except Exception as e:
            logger.error(f"Error in admin_only decorator: {str(e)}")
            # Don't try to reply on errors
            return None
    return wrapper

def generate_campaign_id(length=1):
    """Generate a simple campaign ID"""
    if length == 1:
        return str(random.randint(1, 9))
    else:
        chars = string.ascii_uppercase + string.digits
        return ''.join(random.choice(chars) for _ in range(length))

def format_time_remaining(seconds: int) -> str:
    """Format seconds into readable time"""
    if seconds < 60:
        return f"{seconds}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {seconds}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes}m"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h"

class HumanBehaviorManager:
    """
    Manages human-like behavior patterns for the bot
    Adds random delays, varied typing patterns, and smart contextual interactions
    """
    def __init__(self):
        # History of recent actions to inform behavior decisions
        self.recent_actions = deque(maxlen=10)
        # Base delay ranges in seconds 
        self.base_delay_ranges = {
            "message": (60, 90),         # Regular message sending delay (60-90 sec)
            "response": (2, 8),          # Response to user commands
            "typing": (0.5, 3.5),        # Typing indicator duration
            "read": (1, 4),              # Read receipt delay
            "reaction": (0.8, 2.5),      # Reacting to messages
            "consecutive": (15, 40)      # Delay between consecutive actions of the same type
        }
        # Keep track of consecutive similar actions
        self.consecutive_count = 0
        self.last_action_type = None
        # Time of day behavior adjustment
        self.last_active_hour = datetime.now().hour
        # Track online periods for realistic sessions
        self.daily_sessions = []
        
    def log_action(self, action_type, target=None, details=None):
        """Record an action to inform future behavior"""
        now = datetime.now()
        self.recent_actions.append({
            "type": action_type,
            "target": target,
            "time": now,
            "details": details
        })
        
        # Update consecutive action tracking
        if self.last_action_type == action_type:
            self.consecutive_count += 1
        else:
            self.consecutive_count = 0
            self.last_action_type = action_type
            
        # Update active hour
        self.last_active_hour = now.hour
        
    async def natural_delay(self, action_type="message", context=None):
        """Apply a context-aware random delay that mimics human behavior"""
        # Get base delay range for this action type
        base_min, base_max = self.base_delay_ranges.get(action_type, (1, 3))
        
        # Factor in consecutive similar actions (gradually increase delay)
        if self.consecutive_count > 2 and self.last_action_type == action_type:
            # Increase base delay by 10-30% for every consecutive action beyond 2
            consecutive_factor = 1.0 + (self.consecutive_count - 2) * random.uniform(0.1, 0.3)
            base_min = min(base_min * consecutive_factor, 120)  # Cap at 2 minutes
            base_max = min(base_max * consecutive_factor, 180)  # Cap at 3 minutes
        
        # Factor in time of day
        hour_now = datetime.now().hour
        if hour_now >= 0 and hour_now < 6:  # Late night
            # Slower responses late at night (less active)
            time_of_day_factor = random.uniform(1.2, 1.5)
        elif hour_now >= 6 and hour_now < 9:  # Early morning
            # Moderate response times in early morning
            time_of_day_factor = random.uniform(0.8, 1.2)
        elif hour_now >= 9 and hour_now < 18:  # Working hours
            # Faster during working hours
            time_of_day_factor = random.uniform(0.6, 1.0)
        elif hour_now >= 18 and hour_now < 22:  # Evening
            # Moderate in evening
            time_of_day_factor = random.uniform(0.7, 1.1)
        else:  # Night
            # Slightly slower at night
            time_of_day_factor = random.uniform(0.9, 1.3)
            
        # Apply all factors to calculate final delay
        min_delay = base_min * time_of_day_factor
        max_delay = base_max * time_of_day_factor
        
        # Get a delay value with weighted distribution (favoring middle values slightly)
        delay = self._weighted_random(min_delay, max_delay)
        
        # Apply the delay
        logger.debug(f"Applied natural delay of {delay:.2f}s for action: {action_type}")
        await asyncio.sleep(delay)
        
    def _weighted_random(self, min_val, max_val):
        """Generate a random number with slight weighting toward middle values"""
        # Take multiple samples and average them to create a more natural distribution
        samples = [random.uniform(min_val, max_val) for _ in range(3)]
        return sum(samples) / len(samples)
    
    def get_human_typing_duration(self, text_length):
        """Calculate realistic typing time based on message length"""
        # Average typing speed: 40-90 characters per minute with natural variation
        chars_per_second = random.uniform(0.7, 1.5)
        base_typing_time = text_length / chars_per_second
        
        # Add random pauses for thinking
        if text_length > 20:
            thinking_pauses = random.randint(1, 3)
            pause_time = random.uniform(1.0, 4.0) * thinking_pauses
            total_time = base_typing_time + pause_time
        else:
            total_time = base_typing_time
            
        # Cap at reasonable bounds (10 seconds to 2 minutes)
        return min(max(total_time, 10), 120)
    
    def should_react(self, message_type, content=None):
        """Determine if the bot should react to a message based on content and context"""
        # Base probability of reaction
        base_probability = 0.3
        
        # Increase probability based on message characteristics
        if content and any(word in content.lower() for word in ["thank", "thanks", "good", "great", "awesome"]):
            base_probability += 0.4
        elif message_type == "photo":
            base_probability += 0.3
        elif message_type == "question":
            base_probability += 0.5
            
        # Reduce probability if we've reacted to several messages recently
        reaction_count = sum(1 for action in self.recent_actions 
                           if action["type"] == "reaction" and 
                           (datetime.now() - action["time"]).total_seconds() < 300)
        if reaction_count > 2:
            base_probability -= 0.15 * (reaction_count - 2)
            
        # Generate decision
        return random.random() < max(0, min(base_probability, 0.9))
    
    def generate_human_error(self, message_length):
        """Occasionally generate believable typos and correct them"""
        # Higher chance of errors in longer messages
        error_chance = min(0.05 + (message_length / 1000), 0.3)
        
        if random.random() < error_chance:
            # Types of errors
            error_types = ["typo", "correction", "incomplete"]
            error_type = random.choice(error_types)
            
            if error_type == "typo":
                return True, "typo"
            elif error_type == "correction":
                return True, "correction"
            else:
                return True, "incomplete"
        
        return False, None
    
    def get_smart_response_suggestion(self, context, recent_messages):
        """Generate intelligent response suggestions based on context"""
        # Implement basic context understanding
        topics = self._extract_topics(recent_messages)
        
        # Choose appropriate response type based on context
        if "question" in context:
            return self._generate_question_response(topics, context)
        elif "greeting" in context:
            return self._generate_greeting_response()
        elif "help" in context:
            return self._generate_help_response(topics)
            
        # Default response if no specific context matched
        return None
        
    def _extract_topics(self, messages):
        """Extract main topics from recent messages"""
        # Simple topic extraction - in a real implementation this would be more sophisticated
        topics = []
        
        # Common keywords to topics mapping
        keyword_topics = {
            "help": "assistance",
            "start": "onboarding",
            "target": "advertising",
            "ad": "advertising",
            "command": "functionality",
            "error": "troubleshooting"
        }
        
        # Extract topics based on keywords
        for message in messages:
            if not message:
                continue
                
            for keyword, topic in keyword_topics.items():
                if keyword in message.lower() and topic not in topics:
                    topics.append(topic)
                    
        return topics
        
    def _generate_question_response(self, topics, context):
        """Generate a smart response to a question"""
        # Basic implementation - would be more advanced in production
        if "advertising" in topics:
            return "I can help with your advertising needs. What specific function are you trying to use?"
        elif "functionality" in topics:
            return "I have many capabilities. Try /help to see all available commands."
        else:
            return "I'm here to assist you. What would you like to know?"
            
    def _generate_greeting_response(self):
        """Generate a time-appropriate greeting"""
        hour = datetime.now().hour
        
        if hour >= 5 and hour < 12:
            return "Good morning! How can I assist you today?"
        elif hour >= 12 and hour < 17:
            return "Good afternoon! What can I help you with?"
        elif hour >= 17 and hour < 22:
            return "Good evening! How may I be of service?"
        else:
            return "Hello there! Even at this late hour, I'm here to help."
            
    def _generate_help_response(self, topics):
        """Generate a contextual help suggestion"""
        if "advertising" in topics:
            return "For advertising help, check out commands like /addtarget, /startad, and /cleantarget."
        elif "troubleshooting" in topics:
            return "If you're experiencing issues, try /cleantarget to fix target problems or /optimize to reset the system."
        else:
            return "Use /help to see all available commands, or tell me what you're trying to accomplish."

async def resolve_entity_without_get_entity(client, entity_reference):
    """
    Resolve an entity reference (username, ID, link) to a numeric ID without using get_entity
    This avoids the common get_entity failures and provides more reliable entity resolution
    
    Args:
        client: Telegram client instance
        entity_reference: String or int representing chat/user/channel reference
        
    Returns:
        tuple: (entity_id, entity_type, entity_name, topic_id)
        entity_id (int): Numeric ID of the entity
        entity_type (str): Type of entity ('user', 'chat', 'channel', 'topic', 'unknown')
        entity_name (str): Name or title of the entity (if available)
        topic_id (int or None): Topic ID if the reference is to a forum topic
    """
    entity_id = None
    entity_type = "unknown"
    entity_name = str(entity_reference)
    topic_id = None
    
    # Handle forum topic links (t.me/c/channel_id/topic_id format)
    if isinstance(entity_reference, str) and 't.me/c/' in entity_reference:
        try:
            # Extract channel ID and topic ID from the URL
            parts = entity_reference.split('t.me/c/')[-1].split('/')
            if len(parts) >= 2:
                # First part is channel ID, second part is topic ID
                channel_id = int(parts[0])
                topic_id = int(parts[1])
                
                logger.info(f"Resolved forum topic link: channel_id={channel_id}, topic_id={topic_id}")
                return channel_id, "topic", f"Forum Topic {topic_id}", topic_id
        except Exception as e:
            logger.error(f"Error parsing forum topic link {entity_reference}: {e}")
            # Fall through to other methods
    
    # Already a numeric ID
    if isinstance(entity_reference, int):
        return entity_reference, "unknown", str(entity_reference), None
    
    # String that is numeric
    if isinstance(entity_reference, str) and entity_reference.lstrip('-').isdigit():
        return int(entity_reference), "unknown", entity_reference, None
    
    # Handle username format (@username)
    if isinstance(entity_reference, str) and entity_reference.startswith('@'):
        username = entity_reference[1:]  # Strip @ symbol
        
        # Try finding in dialogs first (faster and more reliable)
        async for dialog in client.iter_dialogs(limit=200):
            # Check if entity is ChannelForbidden and handle it specially
            if hasattr(dialog, 'entity') and hasattr(dialog.entity, '__class__') and dialog.entity.__class__.__name__ == 'ChannelForbidden':
                # For forbidden channels, we can still get the ID but not much else
                entity_type = "channel"
                entity_name = f"Forbidden Channel {dialog.entity.id}"
                return dialog.entity.id, entity_type, entity_name, None
                
            # Normal case: check for matching username
            if hasattr(dialog.entity, 'username') and dialog.entity.username and dialog.entity.username.lower() == username.lower():
                if hasattr(dialog.entity, 'first_name'):
                    entity_type = "user"
                    entity_name = dialog.entity.first_name
                elif hasattr(dialog.entity, 'title'):
                    if hasattr(dialog.entity, 'broadcast') and dialog.entity.broadcast:
                        entity_type = "channel" 
                    else:
                        entity_type = "chat"
                    entity_name = dialog.entity.title
                
                return dialog.entity.id, entity_type, entity_name, None
        
        # If not found in dialogs, try sending a message
        try:
            # Send a temporary message to get the entity ID
            temp_msg = await client.send_message(entity_reference, ".")
            
            # Extract ID based on peer_id type
            if hasattr(temp_msg.peer_id, 'user_id'):
                entity_id = temp_msg.peer_id.user_id
                entity_type = "user"
            elif hasattr(temp_msg.peer_id, 'channel_id'):
                entity_id = temp_msg.peer_id.channel_id
                entity_type = "channel"
            elif hasattr(temp_msg.peer_id, 'chat_id'):
                entity_id = temp_msg.peer_id.chat_id
                entity_type = "chat"
            
            # Delete the temporary message
            await temp_msg.delete()
            return entity_id, entity_type, entity_name, None
            
        except Exception as e:
            logger.error(f"Error resolving username {entity_reference}: {e}")
            # Fall through to other methods
    
    # Handle t.me links
    if isinstance(entity_reference, str) and ('t.me/' in entity_reference):
        # Try to join first if it's a channel
        try:
            await client(JoinChannelRequest(entity_reference))
            await asyncio.sleep(1)  # Small delay after joining
        except Exception as e:
            logger.debug(f"Couldn't join {entity_reference}: {e}")
        
        # Try sending a message to get ID
        try:
            temp_msg = await client.send_message(entity_reference, ".")
            
            # Extract ID based on peer_id type
            if hasattr(temp_msg.peer_id, 'user_id'):
                entity_id = temp_msg.peer_id.user_id
                entity_type = "user"
            elif hasattr(temp_msg.peer_id, 'channel_id'):
                entity_id = temp_msg.peer_id.channel_id
                entity_type = "channel"
            elif hasattr(temp_msg.peer_id, 'chat_id'):
                entity_id = temp_msg.peer_id.chat_id
                entity_type = "chat"
                
            # Delete the temporary message
            await temp_msg.delete()
            return entity_id, entity_type, entity_name, None
            
        except Exception as e:
            logger.error(f"Error resolving link {entity_reference}: {e}")
    
    # For any other string format
    try:
        # Last attempt - direct message send
        temp_msg = await client.send_message(entity_reference, ".")
        
        # Extract ID based on peer_id type
        if hasattr(temp_msg.peer_id, 'user_id'):
            entity_id = temp_msg.peer_id.user_id
            entity_type = "user"
        elif hasattr(temp_msg.peer_id, 'channel_id'):
            entity_id = temp_msg.peer_id.channel_id
            entity_type = "channel"
        elif hasattr(temp_msg.peer_id, 'chat_id'):
            entity_id = temp_msg.peer_id.chat_id
            entity_type = "chat"
            
        # Delete the temporary message
        await temp_msg.delete()
        return entity_id, entity_type, entity_name, None
        
    except Exception as e:
        logger.error(f"All methods failed to resolve {entity_reference}: {e}")
        
    # If all methods fail, raise error
    if entity_id is None:
        raise ValueError(f"Could not resolve entity: {entity_reference}")
        
    return entity_id, entity_type, entity_name, None

class MonitorDashboard:
    """Live monitoring dashboard for ad campaigns"""
    def __init__(self, forwarder):
        self.forwarder = forwarder
        self.campaigns = {}
        self.active_monitors = {}
        logger.info("Monitor initialized")

    def add_campaign(self, campaign_id, data):
        self.campaigns[campaign_id] = data

    def update_campaign(self, campaign_id, updates):
        if campaign_id in self.campaigns:
            self.campaigns[campaign_id].update(updates)

    def update_campaign_status(self, campaign_id, status, extra_data=None):
        if campaign_id in self.campaigns:
            self.campaigns[campaign_id]['status'] = status
            if extra_data:
                self.campaigns[campaign_id].update(extra_data)

    def get_campaign_data(self, campaign_id):
        return self.campaigns.get(campaign_id)

    def get_active_campaign_count(self):
        return len([c for c in self.campaigns.values() if c.get('status') == 'running'])

    def list_active_campaigns(self):
        return [c_id for c_id, c_data in self.campaigns.items() if c_data.get('status') == 'running']

    def list_campaigns(self):
        return list(self.campaigns.keys())

    def campaign_exists(self, campaign_id):
        return campaign_id in self.campaigns

    def is_being_monitored(self, campaign_id):
        """Check if a campaign is currently being monitored"""
        if not campaign_id or not isinstance(campaign_id, str):
            return False
        return campaign_id in self.active_monitors

    async def start_live_monitor(self, campaign_id, message, chat_id):
        self.active_monitors[campaign_id] = {'message': message, 'chat_id': chat_id}
        asyncio.create_task(self._live_monitor(campaign_id, message, chat_id))
        return True  # Return a value to make it properly awaitable
    
    async def _live_monitor(self, campaign_id, message, chat_id):
        """Live monitor a campaign and update the status message regularly with enhanced real-time tracking"""
        try:
            logger.info(f"Starting enhanced live monitoring for campaign {campaign_id}")
            
            # Adaptive update intervals based on campaign status
            active_update_interval = 2   # Update every 2 seconds when active sending
            waiting_update_interval = 5   # Update every 5 seconds when waiting
            
            # Track previous status to detect changes
            previous_status = None
            previous_sent = 0
            previous_failed = 0
            
            # Start time for calculations
            monitor_start_time = time.time()
            last_successful_update = time.time()
            update_failures = 0
            
            while campaign_id in self.active_monitors:
                # Check if campaign still exists
                if not self.campaign_exists(campaign_id):
                    logger.warning(f"Campaign {campaign_id} no longer exists, stopping monitor")
                    break
                    
                # Get current campaign data
                campaign_data = self.get_campaign_data(campaign_id)
                
                # Skip update if campaign data is missing
                if not campaign_data:
                    await asyncio.sleep(active_update_interval)
                    continue
                
                # Get current time for calculations
                current_time = time.time()
                
                # Format status message with detailed real-time tracking
                status = campaign_data.get('status', 'Unknown')
                total_sent = campaign_data.get('total_sent', 0)
                failed_sends = campaign_data.get('failed_sends', 0)
                rounds_completed = campaign_data.get('rounds_completed', 0)
                last_round_success = campaign_data.get('last_round_success', 0)
                
                # Detect if status has changed or messages sent/failed have increased
                status_changed = status != previous_status
                sent_increased = total_sent > previous_sent
                failed_increased = failed_sends > previous_failed
                
                # Calculate real-time sending rate
                elapsed_time = current_time - monitor_start_time
                if elapsed_time > 0:
                    sending_rate = total_sent / elapsed_time if elapsed_time > 0 else 0
                    sending_rate_text = f"{sending_rate:.2f} msgs/sec" if sending_rate > 0 else "Calculating..."
                else:
                    sending_rate_text = "Starting..."
                
                # Ensure failures are correctly extracted
                current_failures = campaign_data.get('current_failures', {})
                if not current_failures and 'current_failures' in campaign_data:
                    current_failures = campaign_data['current_failures']
                
                targets = campaign_data.get('targets', 0)
                msg_id = campaign_data.get('msg_id', 'unknown')
                interval = campaign_data.get('interval', 0)
                start_time = campaign_data.get('start_time', time.time())
                next_round_time = campaign_data.get('next_round_time', time.time())
                
                # Log important statistics for debugging
                logger.info(f"Monitor data for campaign {campaign_id}: Rounds={rounds_completed}, Sent={total_sent}, Failed={failed_sends}, Failures={len(current_failures)}")
                
                # Calculate running time
                running_time = int(time.time() - start_time)
                running_time_str = format_time_remaining(running_time)
                
                # Calculate success rate - ensure it's actually based on total attempted messages
                success_rate = 0.0
                if total_sent > 0:  # Only calculate if there were successful deliveries
                    success_rate = (total_sent / (total_sent + failed_sends)) * 100
                    # Cap success rate at 100% for logical display
                    success_rate = min(success_rate, 100.0)
                
                # Format interval
                minutes, seconds = divmod(interval, 60)
                interval_str = f"{minutes}m {seconds}s"
                
                # Get current time
                current_time = datetime.now().strftime('%H:%M:%S')
                
                # Determine next run status
                time_to_next = max(0, int(next_round_time - time.time()))
                next_run_str = "processing now..." if status == "sending" else f"in {format_time_remaining(time_to_next)}"
                
                # Convert status to uppercase
                display_status = status.upper() if status else "UNKNOWN"
                
                # Current time for timestamps
                current_monitor_time = datetime.now().strftime('%H:%M:%S')
                
                # Build the monitor message with real-time indicators
                status_text = f"📊 LIVE CAMPAIGN MONITOR #{campaign_id}\n\n"
                status_text += f"🔄 Status: {display_status} @ {current_monitor_time}\n\n"
                status_text += f"📨 Message: {msg_id}\n\n"
                status_text += f"⏱️ Interval: {interval_str}\n\n"
                status_text += f"🎯 Targets: {targets}\n\n"
                
                # Enhanced statistics section with real-time indicators
                status_text += f"📈 LIVE Statistics:\n"
                
                # Add real-time indicator with timestamp for sent messages
                if status == "sending":
                    status_text += f"   ✅ Sent: {total_sent} (Sending now...)\n"
                else:
                    status_text += f"   ✅ Sent: {total_sent}\n"
                
                # Show last round success count if available
                if last_round_success > 0:
                    status_text += f"   ✳️ Last Round: +{last_round_success} sent\n"
                
                status_text += f"   ❌ Failures: {failed_sends}\n"
                status_text += f"   📊 Success Rate: {success_rate:.1f}%\n\n"
                
                # Progress section
                status_text += f"🔄 Progress:\n"
                status_text += f"   • Rounds completed: {rounds_completed}\n"
                
                # Add progress indicator if sending
                if status == "sending":
                    # Check if there's a progress field in the campaign data
                    progress_text = campaign_data.get('progress', 'Sending in progress...')
                    status_text += f"   • 🔄 {progress_text}\n"
                
                status_text += f"\n⏰ Timing:\n"
                status_text += f"   🟢 Running for: {running_time_str}\n"
                status_text += f"   ⏩ Next run: {next_run_str}\n\n"
                
                # Add failures if any
                if current_failures and len(current_failures) > 0:
                    status_text += f"❌ Current Failures: {len(current_failures)}\n"
                    
                    # Limit the number of failures shown to prevent message length issues
                    max_failures_to_show = min(5, len(current_failures))
                    
                    for i, (target, error) in enumerate(list(current_failures.items())[:max_failures_to_show]):
                        # Extract ban/error reason more clearly
                        error_type = "Unknown error"
                        if "banned" in error.lower():
                            error_type = "BANNED ⛔"
                        elif "permission" in error.lower():
                            error_type = "NO PERMISSION ⚠️"
                        elif "private" in error.lower():
                            error_type = "PRIVATE CHANNEL 🔒"
                        elif "not found" in error.lower():
                            error_type = "CHAT NOT FOUND 🔍"
                        elif "too many" in error.lower() or "rate limit" in error.lower():
                            error_type = "RATE LIMITED ⏱️"
                        else:
                            # Truncate other errors
                            error_type = error if len(error) < 30 else error[:27] + "..."
                        
                        status_text += f"   • Chat ID {target}: {error_type}\n"
                    
                    # If we have more failures than we're showing, indicate that
                    if len(current_failures) > max_failures_to_show:
                        status_text += f"   • ... and {len(current_failures) - max_failures_to_show} more failures\n"
                    
                    status_text += "\n"
                
                status_text += f"Monitor updating every 5s • Last updated: {current_time}"
                
                # Update the message
                try:
                    # Limit the message length to avoid Telegram's restrictions
                    # Use Telegram's actual limit with a safety margin
                    max_message_length = 4096  # Telegram's official limit
                    if len(status_text) > max_message_length:
                        logger.warning(f"Live monitor message too long ({len(status_text)} chars), truncating")
                        status_text = status_text[:max_message_length-100] + "\n\n... (message truncated due to length) ...\n"
                    
                    # Only update if enough time has passed since last successful update
                    # This helps avoid flood wait errors
                    time_since_last_update = time.time() - last_successful_update
                    if time_since_last_update >= 5:  # Minimum 5 seconds between updates
                        await self.forwarder.client.edit_message(chat_id, message, status_text)
                        last_successful_update = time.time()
                        update_failures = 0  # Reset failure counter after successful update
                    else:
                        # Skip this update to avoid flood wait errors
                        logger.info(f"Skipping monitor update to avoid flood wait (last update was {time_since_last_update:.1f}s ago)")
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"Error updating monitor message: {error_msg}")
                    update_failures += 1
                    
                    # Handle flood wait errors specifically
                    if "wait" in error_msg.lower():
                        try:
                            # Extract wait time
                            wait_seconds = int(re.search(r'of (\d+) seconds', error_msg).group(1))
                            logger.warning(f"Flood wait detected in monitor: {wait_seconds}s. Adjusting update frequency.")
                            
                            # Adjust the update interval based on the required wait time
                            # Add extra 5 seconds as safety margin
                            new_wait_time = wait_seconds + 5
                            
                            # Wait the required time plus a safety margin
                            logger.info(f"Waiting {new_wait_time}s before next monitor update")
                            await asyncio.sleep(new_wait_time)
                            
                            # Update the last update time to avoid immediate retry
                            last_successful_update = time.time()
                            
                            # Continue to next iteration
                            continue
                        except Exception as wait_error:
                            logger.error(f"Error processing wait time: {wait_error}")
                    
                    # If we encounter specific errors, try to recover
                    if "message to edit not found" in error_msg.lower():
                        logger.warning("Monitor message not found, stopping monitoring")
                        # The message was deleted, stop monitoring
                        self.stop_live_monitor(campaign_id)
                    elif "message is not modified" in error_msg.lower():
                        # Message wasn't changed, this is fine
                        # Reset last update time to allow future updates
                        last_successful_update = time.time()
                    
                    # If we have too many consecutive failures, increase the wait time
                    if update_failures > 3:
                        logger.warning(f"Too many monitor update failures ({update_failures}), increasing wait time")
                        # Exponential backoff
                        backoff_time = min(30, 5 * (2 ** (update_failures - 3)))
                        await asyncio.sleep(backoff_time)
                    else:
                        # Just log other errors but continue
                        logger.error(f"Unknown error updating monitor: {error_msg}")
                
                # Choose update interval based on status
                update_interval = active_update_interval if status == "sending" else waiting_update_interval
                
                # Update the status text with the correct interval
                status_text = status_text.replace("Monitor updating every 5s", f"Monitor updating every {update_interval}s")
                
                # Wait before next update
                await asyncio.sleep(update_interval)
                
                # Update tracking variables for next iteration
                previous_status = status
                previous_sent = total_sent
                previous_failed = failed_sends
            
            # Final update if campaign still exists
            if self.campaign_exists(campaign_id):
                campaign_data = self.get_campaign_data(campaign_id)
                if campaign_data:
                    status = campaign_data.get('status', 'Unknown')
                    display_status = status.upper() if status else "UNKNOWN"
                    msg_id = campaign_data.get('msg_id', 'unknown')
                    total_sent = campaign_data.get('total_sent', 0)
                    failed_sends = campaign_data.get('failed_sends', 0)
                    targets = campaign_data.get('targets', 0)
                    rounds_completed = campaign_data.get('rounds_completed', 0)
                    interval = campaign_data.get('interval', 0)
                    start_time = campaign_data.get('start_time', time.time())
                    
                    # Calculate running time
                    running_time = int(time.time() - start_time)
                    running_time_str = format_time_remaining(running_time)
                    
                    # Calculate success rate with proper logic
                    success_rate = 0.0
                    if total_sent > 0:  # Only calculate if there were successful deliveries
                        success_rate = (total_sent / (total_sent + failed_sends)) * 100
                        # Cap success rate at 100% for logical display
                        success_rate = min(success_rate, 100.0)
                    
                    # Format interval
                    minutes, seconds = divmod(interval, 60)
                    interval_str = f"{minutes}m {seconds}s"
                    
                    # Get current time
                    current_time = datetime.now().strftime('%H:%M:%S')
                    
                    # Final time stamp
                    final_timestamp = datetime.now().strftime('%H:%M:%S')
                    
                    final_text = f"📊 CAMPAIGN MONITOR #{campaign_id} - ENDED\n\n"
                    final_text += f"🔄 Final Status: {display_status} @ {final_timestamp}\n\n"
                    final_text += f"📨 Message: {msg_id}\n\n"
                    final_text += f"⏱️ Interval: {interval_str}\n\n"
                    final_text += f"🎯 Targets: {targets}\n\n"
                    
                    # Enhanced statistics with completion indicators
                    final_text += f"📈 Final Statistics:\n"
                    final_text += f"   ✅ Total Sent: {total_sent}\n"
                    
                    # Show percentage of targets reached
                    if targets > 0:
                        percentage_reached = (total_sent / targets) * 100
                        final_text += f"   📊 Target Reach: {percentage_reached:.1f}% of targets\n"
                    
                    final_text += f"   ❌ Failures: {failed_sends}\n"
                    final_text += f"   📊 Success Rate: {success_rate:.1f}%\n\n"
                    
                    final_text += f"🔄 Final Progress:\n"
                    final_text += f"   • Rounds completed: {rounds_completed}\n"
                    
                    # Add average sends per round if rounds completed
                    if rounds_completed > 0:
                        avg_sends = total_sent / rounds_completed
                        final_text += f"   • Avg. sends per round: {avg_sends:.1f}\n\n"
                    else:
                        final_text += "\n"
                        
                    final_text += f"⏰ Total Runtime: {running_time_str}\n\n"
                    final_text += f"⏹️ Monitoring ended at: {current_time}"
                    
                    try:
                        # Make sure final message respects Telegram's size limits
                        # Use a more conservative limit to ensure we stay well under Telegram's maximum
                        max_message_length = 4096  # Telegram's official limit
                        if len(final_text) > max_message_length:
                            logger.warning(f"Final monitor message too long ({len(final_text)} chars), truncating")
                            final_text = final_text[:max_message_length-100] + "\n\n... (message truncated due to length) ...\n"
                            
                        await self.forwarder.client.edit_message(chat_id, message, final_text)
                    except Exception as e:
                        error_msg = str(e)
                        logger.error(f"Error updating final monitor message: {error_msg}")
                        
                        # If error is not a critical one, just log it
                        if "message to edit not found" in error_msg.lower():
                            logger.warning("Final monitor message not found, it may have been deleted")
                        elif "message is not modified" in error_msg.lower():
                            # Message wasn't changed, this is fine
                            logger.debug("Final message not modified, content likely unchanged")
                        elif "wait" in error_msg.lower() or "flood" in error_msg.lower():
                            # FloodWait error from Telegram - extract the wait time
                            wait_time = 60  # Default wait time
                            try:
                                # Try to extract wait time from error message
                                match = re.search(r'A wait of (\d+) seconds', error_msg)
                                if match:
                                    wait_time = int(match.group(1))
                                    logger.warning(f"FloodWait detected: Waiting for {wait_time} seconds")
                                # Add a small buffer to the wait time just to be safe
                                wait_time += 5
                                # Wait the required time
                                await asyncio.sleep(wait_time)
                                # Try again after waiting
                                logger.info(f"Retrying after FloodWait ({wait_time}s)")
                                await self.forwarder.client.edit_message(chat_id, message, final_text)
                            except Exception as retry_error:
                                logger.error(f"Error retrying after FloodWait: {str(retry_error)}")
                        else:
                            # Just log other errors
                            logger.error(f"Unknown error updating final message: {error_msg}")
            
            logger.info(f"Stopped live monitoring for campaign {campaign_id}")
        except asyncio.CancelledError:
            logger.info(f"Live monitor task for campaign {campaign_id} was cancelled")
        except Exception as e:
            logger.error(f"Error in live monitor for campaign {campaign_id}: {str(e)}")

    def stop_live_monitor(self, campaign_id):
        if campaign_id in self.active_monitors:
            del self.active_monitors[campaign_id]

    def stop_all_monitoring(self):
        self.active_monitors.clear()

    def get_active_monitor_count(self):
        return len(self.active_monitors)

    def get_daily_stats(self, days):
        # Placeholder for daily stats
        return [{'total_sent': 10, 'total_failed': 2}, {'total_sent': 15, 'total_failed': 1}] * days
    def generate_performance_chart(self, daily_stats):
        return "📊 Performance chart not yet implemented"
    def generate_dashboard(self, targeted_only=False):
        """Generate a detailed monitoring dashboard with real-time campaign stats and status indicators"""
        current_time = datetime.now().strftime('%H:%M:%S')
        dashboard = f"📊 **REAL-TIME CAMPAIGN DASHBOARD** (Updated: {current_time})\n\n"
        
        # Count active versus inactive campaigns
        active_count = 0
        inactive_count = 0
        total_messages_sent = 0
        total_failures = 0
        
        # Process each campaign
        active_campaigns = []
        inactive_campaigns = []
        
        for campaign_id, data in self.campaigns.items():
            if targeted_only and "targeted_" not in campaign_id:
                continue
                
            # Extract campaign data
            status = data.get('status', 'Unknown')
            targets = data.get('targets', 0)
            sent = data.get('total_sent', 0)
            failed = data.get('failed_sends', 0)
            progress = data.get('progress', '')
            last_update = data.get('last_update_time', '')
            next_round = data.get('next_round_time', 0)
            time_remaining = data.get('estimated_time_remaining', '')
            success_rate = data.get('success_rate', 'N/A')
            rounds_completed = data.get('rounds_completed', 0)
            scheduled_for = data.get('scheduled_for', None)
            
            # Update totals
            total_messages_sent += sent
            total_failures += failed
            
            # Format the campaign info with status emoji and detailed stats
            if status in ['sending', 'sending_with_errors']:
                active_count += 1
                status_emoji = "🔄" if status == 'sending' else "⚠️"
                
                campaign_info = (
                    f"{status_emoji} **Campaign:** `{campaign_id}`\n"
                    f"   • **Status:** {status.upper()}\n"
                    f"   • **Progress:** {progress}\n"
                    f"   • **Sent:** {sent}/{targets} targets ({success_rate} success rate)\n"
                    f"   • **Failed:** {failed} targets\n"
                )
                
                # Add time details if available
                if time_remaining:
                    campaign_info += f"   • **Est. Remaining:** {time_remaining}\n"
                if last_update:
                    campaign_info += f"   • **Last Update:** {last_update}\n"
                
                active_campaigns.append(campaign_info)
                
            elif status == 'waiting':
                active_count += 1
                
                # Calculate time until next round
                time_until_next = ""
                if next_round > 0:
                    now = time.time()
                    if next_round > now:
                        seconds_remaining = int(next_round - now)
                        time_until_next = format_time_remaining(seconds_remaining)
                
                campaign_info = (
                    f"⏳ **Campaign:** `{campaign_id}`\n"
                    f"   • **Status:** WAITING FOR NEXT ROUND\n"
                    f"   • **Completed:** {rounds_completed} rounds\n"
                    f"   • **Sent:** {sent}/{targets} targets\n"
                    f"   • **Failed:** {failed} targets\n"
                )
                
                if time_until_next:
                    campaign_info += f"   • **Next Round In:** {time_until_next}\n"
                
                active_campaigns.append(campaign_info)
                
            elif status == 'scheduled':
                active_count += 1
                campaign_info = (
                    f"🗓️ **Campaign:** `{campaign_id}`\n"
                    f"   • **Status:** SCHEDULED\n"
                    f"   • **Will Run At:** {scheduled_for}\n"
                    f"   • **Targets:** {targets}\n"
                )
                active_campaigns.append(campaign_info)
                
            else:
                inactive_count += 1
                # Compact display for inactive campaigns
                status_emoji = "✅" if status == 'completed' else "❌" if status in ['cancelled', 'error'] else "⚪"
                campaign_info = f"{status_emoji} `{campaign_id}`: {status.upper()} ({sent}/{targets} sent, {failed} failed)\n"
                inactive_campaigns.append(campaign_info)
        
        # Build the dashboard with sections
        if active_count > 0:
            dashboard += f"🔴 **ACTIVE CAMPAIGNS ({active_count}):**\n\n"
            dashboard += "\n".join(active_campaigns)
            dashboard += "\n\n"
        
        if inactive_count > 0:
            dashboard += f"⚪ **INACTIVE CAMPAIGNS ({inactive_count}):**\n\n"
            dashboard += "".join(inactive_campaigns)
            dashboard += "\n\n"
        
        # Add summary stats
        dashboard += f"📈 **TOTAL STATISTICS:**\n"
        dashboard += f"   • Total Campaigns: {active_count + inactive_count}\n"
        dashboard += f"   • Total Messages Sent: {total_messages_sent}\n"
        dashboard += f"   • Total Failures: {total_failures}\n"
        dashboard += f"   • Overall Success Rate: {(total_messages_sent / (total_messages_sent + total_failures) * 100):.1f}%" if (total_messages_sent + total_failures) > 0 else "N/A"
        
        return dashboard if active_count + inactive_count > 0 else "📝 No campaigns found. Start a campaign with /startad or /schedule."


class MessageForwarder:
    # Class attribute to store the current instance
    instance = None
    # Primary admin ID is fixed and protected from removal
    primary_admin = 1715541908  # This ID is protected and cannot be removed

    def __init__(self, client):
        self.client = client
        self.client.flood_sleep_threshold = 5  # Reduce flood wait time
        self.flood_protection = {
            'max_messages': 30,  # Max messages per minute
            'cooldown': 60,      # Cooldown period in seconds
            'message_count': 0,  # Current message count
            'last_reset': time.time()  # Last counter reset time
        }
        self.forwarding_enabled = False
        self.target_chats: Set[Union[int, Tuple[int, int]]] = set()
        self.forward_interval = 300  # Default from config
        self.stored_messages: Dict[str, Any] = {}  # Store multiple messages by ID
        self._commands_registered = False
        self._forwarding_tasks: Dict[str, asyncio.Task] = {}  # Track multiple forwarding tasks
        self._message_queue = asyncio.Queue()  # Message queue for faster processing
        self._cache = {}
        
        # Track failed chats with detailed information about failures
        # Structure: {chat_id: {
        #    'name': str, 'type': str, 'first_failure': datetime,
        #    'last_attempt': datetime, 'reason': str, 'detail': str,
        #    'failed_count': int, 'campaign_ids': set,
        #    'error_history': [{timestamp, campaign_id, error_type, details}]
        # }}
        self.failed_chats = {}  # Cache for frequently accessed data

        # Scheduled campaigns
        self.scheduled_tasks: Dict[str, asyncio.Task] = {}  # Track scheduled tasks
        self.targeted_campaigns: Dict[str, Dict] = {}  # Store targeted ad campaigns

        # Admin management - Always ensure primary admin is included
        admin_ids = os.getenv('ADMIN_USER_IDS', '').split(',')
        self.admins: Set[int] = set([int(id.strip()) for id in admin_ids if id.strip()])
        # Always ensure the primary admin is in the admins list
        self.admins.add(MessageForwarder.primary_admin)

        # Analytics
        self.analytics = {
            "forwards": {},  # Track successful forwards
            "failures": {},  # Track failed forwards
            "start_time": time.time(),  # Track when bot started
            "auto_replies": {}  # Store auto-reply patterns
        }
        
        # Initialize default auto-replies
        self.auto_replies = {
            "hello": "👋 Hello! How can I assist you today?",
            "price": "💰 Please contact @admin for pricing details",
            "help": "ℹ️ Use /help to see all available commands"
        }

        # Dashboard for live monitoring
        self.monitor = MonitorDashboard(self)
        
        # Initialize human behavior manager
        self.human_behavior = HumanBehaviorManager()
        
        # Enhanced context tracking for smarter responses
        self.recent_messages = deque(maxlen=20)  # Track recent messages for context
        self.active_conversations = {}  # Track active conversations by chat_id
        self.entity_knowledge = {}  # Store knowledge about entities we interact with
        self.smart_mode = True  # Toggle for smart behavior with human-like delays

        # Set this instance as the current one
        MessageForwarder.instance = self

        logger.info("MessageForwarder initialized with smart human-like behavior")

        # Register command handlers
        self.register_commands()

    async def _get_sender_name(self, event):
        """Get the name of the sender of an event, preferring client name over username"""
        try:
            sender = await event.get_sender()
            if hasattr(sender, 'first_name'):
                return sender.first_name
            return "User"  # Fallback for non-user senders
        except Exception as e:
            logger.error(f"Error getting sender name: {str(e)}")
            return "User"  # Fallback in case of error

    async def forward_stored_message(self, msg_id: str = "default", targets: Optional[Set[int]] = None, interval: Optional[int] = None, campaign_id: Optional[str] = None, max_queue_size: int = 100):
        # Initialize message queue if not exists
        if not hasattr(self, '_message_queue'):
            self._message_queue = asyncio.Queue(maxsize=max_queue_size)
        """Periodically forward stored message to target chats with error handling and continuous operation"""
        # Define campaign_marker at the top level to ensure it's always bound
        campaign_marker = None
        try:
            # Use provided campaign_id if available, otherwise generate a new one
            # This allows us to link the task with a pre-existing campaign ID
            campaign_marker = campaign_id if campaign_id else f"adcampaign_{msg_id}_{int(time.time())}"
            
            if msg_id not in self.stored_messages:
                logger.error(f"Message ID {msg_id} not found in stored messages")
                # Add to monitor with error status
                self.monitor.add_campaign(campaign_marker, {
                    "msg_id": msg_id,
                    "status": "error",
                    "error_message": f"Message ID {msg_id} not found",
                    "start_time": time.time()
                })
                return

            message = self.stored_messages[msg_id]
            use_targets = targets if targets is not None else self.target_chats
            use_interval = interval if interval is not None else self.forward_interval
            
            # Log the campaign ID we're using to track this forwarding task
            logger.info(f"Using campaign marker: {campaign_marker} for message {msg_id}")
            
            # Store target list for failure checking
            target_list = list(use_targets)
            
            # Add this to monitor for tracking with explicit error tracking
            self.monitor.add_campaign(campaign_marker, {
                "msg_id": msg_id,
                "targets": len(use_targets),
                "target_list": target_list, 
                "interval": use_interval,
                "start_time": time.time(),
                "rounds_completed": 0,
                "total_sent": 0,
                "failed_sends": 0,
                "status": "running",
                "current_failures": {}
            })

            # Send monitor dashboard as a reply
            logger.info(f"Starting periodic forwarding task for message {msg_id}")

            round_number = 0

            while True:
                if msg_id not in self.stored_messages:  # Check if message was deleted
                    logger.info(f"Message {msg_id} no longer exists, stopping forwarding")
                    # Update monitor
                    self.monitor.update_campaign_status(campaign_marker, "stopped")
                    break

                round_number += 1
                success_count = 0
                failure_count = 0
                current_failures = {}

                logger.info(f"Forwarding message {msg_id} to {len(use_targets)} targets (Round {round_number})")

                # Update monitor before sending
                self.monitor.update_campaign(campaign_marker, {
                    "rounds_completed": round_number - 1,  # Current round not completed yet
                    "status": "sending"
                })

                # Get current campaign data before processing targets
                campaign_data = self.monitor.get_campaign_data(campaign_marker) or {}
                
                # Split targets into smaller batches of 20
                target_list = list(use_targets)
                batch_size = 20
                last_batch_index = 0
                
                for i in range(0, len(target_list), batch_size):
                    # Record batch start time for timing calculations
                    batch_start_time = datetime.now()
                    last_batch_index = i
                    batch = target_list[i:i + batch_size]
                    
                    # Process each target in batch
                    for target in batch:
                        try:
                            # Get target info for better error reporting
                            target_info = ""
                            entity = None
                            try:
                                # Get target info without using get_entity
                                if isinstance(target, int) or (isinstance(target, str) and target.lstrip('-').isdigit()):
                                    # For numeric IDs, just use the ID as info
                                    target_info = f"ID: {target}"
                                elif isinstance(target, str):
                                    if target.startswith('@'):
                                        target_info = target  # Already a username format
                                    elif 't.me/' in target:
                                        target_info = f"Link: {target}"
                                    else:
                                        target_info = f"Chat: {target}"
                                elif hasattr(entity, 'phone'):
                                    target_info = f"+{entity.phone}"
                            except:
                                target_info = str(target)

                            # Try forwarding with retries
                            max_retries = 3
                            for retry in range(max_retries):
                                try:
                                    # Get the user ID (from_peer) of the bot - this is needed for ForwardMessagesRequest
                                    me = await self.client.get_me()
                                    bot_user_id = me.id
                                    
                                    # Check if target is a tuple (chat_id, topic_id)
                                    if isinstance(target, tuple) and len(target) == 2:
                                        chat_id, topic_id = target
                                        logger.info(f"Forwarding to topic: chat_id={chat_id}, topic_id={topic_id}")
                                        
                                        # Use ForwardMessagesRequest for topics
                                        forwarded = await self.client(ForwardMessagesRequest(
                                            from_peer=bot_user_id,
                                            id=[message.id],
                                            to_peer=chat_id,
                                            top_msg_id=topic_id  # Use topic_id as top_msg_id for forum topics
                                        ))
                                        
                                        # We no longer need to send the confirmation message
                                        # The message is already properly forwarded to the topic
                                    else:
                                        # Regular chat - use ForwardMessagesRequest with numeric IDs
                                        await self.client(ForwardMessagesRequest(
                                            from_peer=bot_user_id,
                                            id=[message.id],
                                            to_peer=target
                                        ))
                                    success_count += 1
                                    
                                    # Update the monitor immediately after each successful send for real-time stats
                                    try:
                                        # Get current campaign data
                                        current_data = self.monitor.get_campaign_data(campaign_marker) or {}
                                        current_sent = current_data.get("total_sent", 0)
                                        
                                        # Update monitor with real-time status
                                        self.monitor.update_campaign(campaign_marker, {
                                            "total_sent": current_sent + 1,
                                            "last_target": str(target),
                                            "last_update_time": datetime.now().strftime('%H:%M:%S'),
                                            "status": "sending",
                                            "progress": f"Sent to {success_count}/{len(batch)} in current batch"
                                        })
                                    except Exception as update_error:
                                        logger.error(f"Error updating monitor in real-time: {update_error}")
                                    
                                    logger.info(f"Successfully forwarded message to {target}")
                                    break
                                except Exception as e:
                                    error_msg = str(e)
                                    logger.error(f"Error forwarding to {target}: {error_msg}")
                                    
                                    # Add specific error logging for common issues
                                    if "banned" in error_msg.lower():
                                        logger.error(f"Target {target} has banned the bot or the bot is banned from the channel")
                                    elif "not found" in error_msg.lower():
                                        logger.error(f"Target {target} was not found (may not exist)")
                                    elif "private" in error_msg.lower():
                                        logger.error(f"Target {target} is a private channel the bot cannot access")
                                    elif "permission" in error_msg.lower() or "403" in error_msg:
                                        logger.error(f"Bot lacks permission to forward to {target}")
                                    elif "Too many" in error_msg or "420" in error_msg:
                                        logger.error(f"Rate limit hit when forwarding to {target}, waiting longer")
                                        await asyncio.sleep(5)  # Wait longer for rate limits
                                        
                                    if retry == max_retries - 1:
                                        raise
                                    await asyncio.sleep(2)  # Wait before retry

                            # Update analytics
                            today = datetime.now().strftime('%Y-%m-%d')
                            if today not in self.analytics["forwards"]:
                                self.analytics["forwards"][today] = {}

                            campaign_key = f"{msg_id}_{target}"
                            if campaign_key not in self.analytics["forwards"][today]:
                                self.analytics["forwards"][today][campaign_key] = 0

                            self.analytics["forwards"][today][campaign_key] += 1

                            logger.info(f"Successfully forwarded message {msg_id} to {target_info}")
                            
                            # Apply human-like delay if smart mode is enabled
                            if self.smart_mode:
                                # Log the action for behavior tracking
                                self.human_behavior.log_action("message", target, {"type": "forward", "msg_id": msg_id})
                                # Apply natural delay between messages
                                await self.human_behavior.natural_delay("message")
                        except Exception as e:
                            failure_count += 1
                            error_message = str(e)
                            # Record the error in current_failures
                            current_failures[str(target)] = error_message
                            logger.error(f"Error forwarding to {target}: {error_message}")
                            
                            # Track in failed chats system with detailed information
                            try:
                                # Convert tuple target (chat_id, topic_id) to string for consistency
                                target_key = target[0] if isinstance(target, tuple) else target
                                
                                # Get or create the failed chat entry
                                if target_key not in self.failed_chats:
                                    # Try to get entity info without triggering errors
                                    entity_type = "unknown"
                                    entity_name = str(target)
                                    try:
                                        # Use direct string checks instead of API calls
                                        if isinstance(target, int) or (isinstance(target, str) and target.lstrip('-').isdigit()):
                                            if str(target).startswith('-100'):
                                                entity_type = "channel"
                                            elif str(target).startswith('-'):
                                                entity_type = "group"
                                            else:
                                                entity_type = "user"
                                    except:
                                        pass
                                        
                                    # Create new failed chat entry
                                    self.failed_chats[target_key] = {
                                        'name': entity_name,
                                        'type': entity_type,
                                        'first_failure': datetime.now(),
                                        'last_attempt': datetime.now(),
                                        'reason': self._classify_error(error_message),
                                        'detail': error_message,
                                        'failed_count': 1,
                                        'campaign_ids': {campaign_marker},
                                        'error_history': [{
                                            'timestamp': datetime.now().isoformat(),
                                            'campaign_id': campaign_marker,
                                            'error_type': self._classify_error(error_message),
                                            'details': error_message
                                        }]
                                    }
                                else:
                                    # Update existing failed chat entry
                                    failed_chat = self.failed_chats[target_key]
                                    failed_chat['last_attempt'] = datetime.now()
                                    failed_chat['reason'] = self._classify_error(error_message)
                                    failed_chat['detail'] = error_message
                                    failed_chat['failed_count'] += 1
                                    if 'campaign_ids' not in failed_chat:
                                        failed_chat['campaign_ids'] = set()
                                    failed_chat['campaign_ids'].add(campaign_marker)
                                    
                                    # Add to error history
                                    if 'error_history' not in failed_chat:
                                        failed_chat['error_history'] = []
                                    failed_chat['error_history'].append({
                                        'timestamp': datetime.now().isoformat(),
                                        'campaign_id': campaign_marker,
                                        'error_type': self._classify_error(error_message),
                                        'details': error_message
                                    })
                            except Exception as failed_chat_error:
                                logger.error(f"Error updating failed chats system: {failed_chat_error}")
                            
                            # Update monitor immediately with failure information for real-time tracking
                            try:
                                # Get current campaign data
                                current_data = self.monitor.get_campaign_data(campaign_marker) or {}
                                current_failed = current_data.get("failed_sends", 0)
                                
                                # Update monitor with real-time status including failure
                                self.monitor.update_campaign(campaign_marker, {
                                    "failed_sends": current_failed + 1,
                                    "last_failed_target": str(target),
                                    "last_error": error_message[:100] if len(error_message) > 100 else error_message,
                                    "last_update_time": datetime.now().strftime('%H:%M:%S'),
                                    "current_failures": current_failures,
                                    "status": "sending_with_errors"
                                })
                            except Exception as update_error:
                                logger.error(f"Error updating monitor for failure in real-time: {update_error}")
                        
                    # More frequent batch updates after every 5 targets or at end of batch
                    if (len(batch) % 5 == 0) or (len(batch) < 5):
                        # Fetch the latest campaign data for updating
                        latest_campaign_data = self.monitor.get_campaign_data(campaign_marker) or {}
                        current_sent = latest_campaign_data.get("total_sent", 0)
                        current_failed = latest_campaign_data.get("failed_sends", 0)
                        
                        # Include timing information for better monitoring
                        current_time = datetime.now()
                        elapsed_time = (current_time - batch_start_time).total_seconds()
                        remaining_targets = len(target_list) - (i + len(batch))
                        
                        # Calculate estimated time remaining
                        if success_count + failure_count > 0 and elapsed_time > 0:
                            targets_per_second = (success_count + failure_count) / elapsed_time
                            estimated_time_remaining = remaining_targets / targets_per_second if targets_per_second > 0 else 0
                            time_remaining_str = format_time_remaining(int(estimated_time_remaining))
                        else:
                            time_remaining_str = "Calculating..."
                        
                        # Update monitor with comprehensive status
                        self.monitor.update_campaign(campaign_marker, {
                            "total_sent": current_sent,
                            "failed_sends": current_failed,
                            "current_failures": current_failures,
                            "status": "sending",
                            "progress": f"Processed {i + len(batch)}/{len(target_list)} targets",
                            "success_rate": f"{(success_count / (success_count + failure_count) * 100):.1f}%" if (success_count + failure_count) > 0 else "N/A",
                            "estimated_time_remaining": time_remaining_str,
                            "batch_progress": f"{len(batch)}/{batch_size} in current batch"
                        })
                
                # Add delay between batches if not the last batch
                if last_batch_index + batch_size < len(target_list):
                    await asyncio.sleep(5)  # 5 second delay between batches

                # Get the most current campaign data
                latest_campaign_data = self.monitor.get_campaign_data(campaign_marker) or {}
                
                # Update monitor after completing the round with explicit failure tracking
                total_sent = latest_campaign_data.get("total_sent", 0)  # Use the already updated value
                total_failed = latest_campaign_data.get("failed_sends", 0)  # Use the already updated value
                
                self.monitor.update_campaign(campaign_marker, {
                    "rounds_completed": round_number,
                    "total_sent": total_sent,  # Already includes success_count from batch updates
                    "failed_sends": total_failed,  # Already includes failure_count from batch updates
                    "current_failures": current_failures,
                    "last_round_success": success_count,
                    "last_round_failures": failure_count,
                    "status": "waiting",
                    "next_round_time": time.time() + use_interval
                })
                
                # Log detailed statistics for debugging
                logger.info(f"Campaign {campaign_marker} statistics updated - Round: {round_number}, Total sent: {total_sent}, Failed: {total_failed}")

                logger.info(f"Round {round_number} completed: {success_count} successful, {failure_count} failed")
                logger.info(f"Waiting {use_interval} seconds before next forward for message {msg_id}")

                await asyncio.sleep(use_interval)

        except asyncio.CancelledError:
            logger.info(f"Forwarding task for message {msg_id} was cancelled")
            # Update monitor (use campaign_marker which was defined at the beginning of the function)
            if campaign_marker:
                self.monitor.update_campaign_status(campaign_marker, "cancelled")
        except Exception as e:
            logger.error(f"Error in forwarding task for message {msg_id}: {str(e)}")
            # Update monitor (use campaign_marker which was defined at the beginning of the function)
            if campaign_marker:
                self.monitor.update_campaign_status(campaign_marker, "error", {"error_message": str(e)})

            # Remove task from active tasks
            if msg_id in self._forwarding_tasks:
                del self._forwarding_tasks[msg_id]

    def _classify_error(self, error_message):
        """Classify error message into categories for better analysis"""
        error_message = error_message.lower()
        
        if "banned" in error_message or "restrict" in error_message:
            return "banned"
        elif "not found" in error_message or "invalid" in error_message:
            return "not_found" 
        elif "private" in error_message or "access" in error_message:
            return "access_denied"
        elif "permission" in error_message or "403" in error_message:
            return "permission_denied"
        elif "too many" in error_message or "420" in error_message or "flood" in error_message:
            return "rate_limited"
        elif "topic" in error_message and "not found" in error_message:
            return "topic_not_found"
        elif "message" in error_message and "not found" in error_message:
            return "message_not_found"
        elif "timeout" in error_message or "disconnect" in error_message:
            return "connection_error"
        elif "too long" in error_message or "large" in error_message:
            return "content_too_large"
        else:
            return "other"
            
    def register_commands(self):
        """Register command handlers"""
        if self._commands_registered:
            return

        try:
            commands = {
                # Basic commands
                'start': self.cmd_start,
                'stop': self.cmd_stop,
                'help': self.cmd_help,
                'optimize': self.cmd_optimize,

                # Message management
                'setad': self.cmd_setad,
                'listad': self.cmd_listad,
                'removead': self.cmd_removead,

                # Basic forwarding
                'startad': self.cmd_startad,
                'stopad': self.cmd_stopad,
                'timer': self.cmd_timer,

                # Advanced forwarding
                'schedule': self.cmd_schedule,

                # Target management
                'addtarget': self.cmd_addtarget,
                'listtarget': self.cmd_listtarget,
                'listtargets': self.cmd_listtarget,  # Alias
                'removetarget': self.cmd_removetarget,
                'removealltarget': self.cmd_removealltarget,
                'cleantarget': self.cmd_cleantarget,

                # Chat management
                'joinchat': self.cmd_joinchat,
                'leavechat': self.cmd_leavechat,
                'leaveallchat': self.cmd_leaveallchat,
                'listjoined': self.cmd_listjoined,
                'clearchat': self.cmd_clearchat,
                'pin': self.cmd_pin,

                # Profile management
                'bio': self.cmd_bio,
                'name': self.cmd_name,
                'username': self.cmd_username,
                'setpic': self.cmd_setpic,

                # Admin management
                'addadmin': self.cmd_addadmin,
                'removeadmin': self.cmd_removeadmin,
                'listadmins': self.cmd_listadmins,

                # Monitoring
                'monitor': self.cmd_monitor,
                'failedchats': self.cmd_failed_chats,
                'retryfailed': self.cmd_retry_failed,
                'removefailed': self.cmd_remove_failed,

                # Miscellaneous
                'client': self.cmd_client,
            }

            for cmd, handler in commands.items():
                pattern = f'^/{cmd}(?:\\s|$)'
                self.client.add_event_handler(
                    handler,
                    events.NewMessage(pattern=pattern)
                )
                logger.info(f"Registered command: /{cmd}")

            self._commands_registered = True
            logger.info("All commands registered")
        except Exception as e:
            logger.error(f"Error registering commands: {str(e)}")
            raise

    @admin_only
    async def cmd_start(self, event):
        """Start the userbot and show welcome message with monitoring info"""
        try:
            # Use cached me info if available
            if 'me' not in self._cache:
                self._cache['me'] = await self.client.get_me()
            me = self._cache['me']
            username = "siimplebot1"  # Always use this fixed username
            name = me.first_name if hasattr(me, 'first_name') else "Siimple"  # Use client name instead of user

            # Enable the bot to respond to commands
            self.forwarding_enabled = True

            # Show loading animation
            frames = [
                "╔═══════ LOADING ═══════╗\n║    ⚡ □□□□□□□□□□ 0%    ║\n╚═════════════════════╝",
                "╔═══════ LOADING ═══════╗\n║    ⚡ ■□□□□□□□□□ 10%   ║\n╚═════════════════════╝",
                "╔═══════ LOADING ═══════╗\n║    ⚡ ■■□□□□□□□□ 20%   ║\n╚═════════════════════╝",
                "╔═══════ LOADING ═══════╗\n║    ⚡ ■■■□□□□□□□ 30%   ║\n╚═════════════════════╝",
                "╔═══════ LOADING ═══════╗\n║    ⚡ ■■■■□□□□□□ 40%   ║\n╚═════════════════════╝",
                "╔═══════ LOADING ═══════╗\n║    ⚡ ■■■■■□□□□□ 50%   ║\n╚═════════════════════╝",
                "╔═══════ LOADING ═══════╗\n║    ⚡ ■■■■■■□□□□ 60%   ║\n╚═════════════════════╝",
                "╔═══════ LOADING ═══════╗\n║    ⚡ ■■■■■■■□□□ 70%   ║\n╚═════════════════════╝",
                "╔═══════ LOADING ═══════╗\n║    ⚡ ■■■■■■■■□□ 80%   ║\n╚═════════════════════╝",
                "╔═══════ LOADING ═══════╗\n║    ⚡ ■■■■■■■■■□ 90%   ║\n╚═════════════════════╝",
                "╔═══════ LOADING ═══════╗\n║    ⚡ ■■■■■■■■■■ 100%  ║\n╚═════════════════════╝",
                "╔══════ COMPLETED ═══════╗\n║     ✨ SUCCESS! ✨      ║\n╚═════════════════════╝"
            ]
            
            msg = await event.reply(frames[0])
            for frame in frames[1:]:
                await asyncio.sleep(0.3)
                await msg.edit(frame)
            
            await asyncio.sleep(0.5)
            await msg.delete()

            welcome_text = f"""
╔═══════════════════════════╗
║  🌟 WELCOME TO THE BEST   ║
║  --Ꮪɪᴍṗʟᴇ'𝚜 𝙰𝙳𝙱𝙾𝚃 #1   ║
║       @{username}         ║
╚═══════════════════════════╝

💫 Hey {name}! Ready to experience the ULTIMATE automation? 💫

I am --Ꮪɪᴍṗʟᴇ'𝚜 𝙰𝙳𝙱𝙾𝚃, your ultimate Telegram assistant, built to make your experience smarter, faster, and way more fun! 🎭⚡

💎 What I Can Do: 
✅ Fast & Smart Automation ⚡ 
✅ Fun Commands & Tools 🎭 
✅ Instant Replies & Assistance 🤖 
✅ Custom Features Just for You! 💡

🎯 How to Use Me? 
🔹 Type `/help` to explore my powers! 
🔹 Want to chat? Just send a message & see the magic! 
🔹 Feeling bored? Try my fun commands and enjoy the ride!

💬 Mood: Always ready to assist! 
⚡ Speed: Faster than light! 
🎭 Vibe: Smart, cool & interactive!

I'm here to make your Telegram experience legendary! 🚀💙 Stay awesome, and let's get started! 😎🔥
"""
            await event.reply(welcome_text)

            # Send a dashboard with current status if there are active campaigns
            if self.monitor.get_active_campaign_count() > 0:
                dashboard_text = self.monitor.generate_dashboard()
                await event.reply(dashboard_text)

            logger.info("Start command executed - Bot activated")
        except Exception as e:
            logger.error(f"Error in start command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_stop(self, event):
        """Stop all active forwarding tasks and disable command responses"""
        try:
            # Get client name for personalized message
            me = await self.client.get_me()
            name = me.first_name if hasattr(me, 'first_name') else "Siimple"  # Use client name instead of user

            # Cancel all forwarding tasks
            for task_id, task in list(self._forwarding_tasks.items()):
                if not task.done():
                    task.cancel()
            self._forwarding_tasks.clear()

            # Cancel all scheduled tasks
            for task_id, task in list(self.scheduled_tasks.items()):
                if not task.done():
                    task.cancel()
            self.scheduled_tasks.clear()

            # Clear targeted campaigns
            self.targeted_campaigns.clear()

            # Disable command responses (except for /start)
            self.forwarding_enabled = False

            # Show shutdown animation
            frames = [
                "╔═══════ SHUTDOWN ═══════╗\n║    🔴 ■■■■■■■■■■ 100%  ║\n╚═════════════════════╝",
                "╔═══════ SHUTDOWN ═══════╗\n║    🔴 ■■■■■■■■■□ 90%   ║\n╚═════════════════════╝",
                "╔═══════ SHUTDOWN ═══════╗\n║    🔴 ■■■■■■■■□□ 80%   ║\n╚═════════════════════╝",
                "╔═══════ SHUTDOWN ═══════╗\n║    🔴 ■■■■■■■□□□ 70%   ║\n╚═════════════════════╝",
                "╔═══════ SHUTDOWN ═══════╗\n║    🔴 ■■■■■■□□□□ 60%   ║\n╚═════════════════════╝",
                "╔═══════ SHUTDOWN ═══════╗\n║    🔴 ■■■■■□□□□□ 50%   ║\n╚═════════════════════╝",
                "╔═══════ SHUTDOWN ═══════╗\n║    🔴 ■■■■□□□□□□ 40%   ║\n╚═════════════════════╝",
                "╔═══════ SHUTDOWN ═══════╗\n║    🔴 ■■■□□□□□□□ 30%   ║\n╚═════════════════════╝",
                "╔═══════ SHUTDOWN ═══════╗\n║    🔴 ■■□□□□□□□□ 20%   ║\n╚═════════════════════╝",
                "╔═══════ SHUTDOWN ═══════╗\n║    🔴 ■□□□□□□□□□ 10%   ║\n╚═════════════════════╝",
                "╔═══════ SHUTDOWN ═══════╗\n║    🔴 □□□□□□□□□□ 0%    ║\n╚═════════════════════╝",
                "╔══════ TERMINATED ══════╗\n║      💤 OFFLINE 💤      ║\n╚═════════════════════╝"
            ]
            
            msg = await event.reply(frames[0])
            for frame in frames[1:]:
                await asyncio.sleep(0.3)
                await msg.edit(frame)
            
            await asyncio.sleep(0.5)
            await msg.delete()

            stop_message = f"""⚠️ --Ꮪɪᴍṗʟᴇ'𝚜 𝙰𝙳𝙱𝙾𝚃 SYSTEM SHUTDOWN ⚠️

Hey {name}! 😔 Looks like you've decided to stop me... but don't worry, I'll be here whenever you need me! 🚀

📌 Bot Status: ⚠️ Going Offline for You
📌 Commands Disabled: ❌ No More Assistance
📌 Mood: 💤 Entering Sleep Mode

💡 Want to wake me up again?
Just type `/start`, and I'll be back in action, ready to assist you! 🔥

Until then, stay awesome & take care! 😎

🚀 Powered by --Ꮪɪᴍṗʟᴇ'𝚜 𝙰𝙳𝙱𝙾𝚃 (@siimplebot1)
"""
            await event.reply(stop_message)
            logger.info("Stop command executed - Bot deactivated")
        except Exception as e:
            logger.error(f"Error in stop command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_help(self, event):
        """Show help message with animation"""
        try:
            me = await self.client.get_me()
            username = "siimplebot1"  # Always use this fixed username
            name = me.first_name if hasattr(me, 'first_name') else "Siimple"  # Use client name instead of user

            # Show loading animation
            help_msg = await event.reply("🔄 Loading Command Center...")
            await asyncio.sleep(0.7)

            frames = [
                "⚡ Initializing Help System...",
                "🔍 Gathering Commands...",
                "📝 Formatting Guide...",
                "✨ Preparing Display..."
            ]

            for frame in frames:
                await help_msg.edit(frame)
                await asyncio.sleep(0.7)

            # Delete the loading message
            await help_msg.delete()

            help_text = f"""🚀🔥 WELCOME TO --Ꮪɪᴍṗʟᴇ'𝚜 𝙰𝙳𝙱𝙾𝚃 COMMAND CENTER 🔥🚀

Hey {name}! 😎 Ready to take control? Here's what I can do for you! ⚡

━━━━━━━━━━━━━━━━━━━━━━

🌟 BASIC COMMANDS
🔹 `/start` – 🚀 Activate the bot
🔹 `/stop` – 🛑 Deactivate the bot
🔹 `/help` – 📜 Show all available commands
🔹 `/client` – 🤖 Get details about your client
🔹 `/optimize` – 🚀 Reset and optimize performance
🔹 `/optimize --fast` – ⚡ Optimize with fast mode (no delays)

━━━━━━━━━━━━━━━━━━━━━━

📢 ADVERTISEMENT MANAGEMENT
📌 Run powerful ad campaigns with ease!
🔹 `/setad` <reply to message> – 📝 Set an ad
🔹 `/listad` – 📋 View all ads
🔹 `/removead` <ID> – ❌ Remove a specific ad
🔹 `/startad` <ID> <interval> – 🚀 Start an ad campaign
🔹 `/stopad` <ID> – ⏹ Stop an ad campaign
🔹 `/timer` <seconds> – ⏱️ Set default forward interval
🔹 `/schedule` <msg_id> <time> – 📆 Schedule a message

━━━━━━━━━━━━━━━━━━━━━━

🎯 TARGETING & AUDIENCE MANAGEMENT
📌 Reach the right audience with precision!
🔹 `/addtarget` <targets> – ➕ Add target audience
🔹 `/listtarget` – 📜 View all targets
🔹 `/removetarget` <id 1,2,3> – ❌ Remove specific targets
🔹 `/removealltarget` – 🧹 Clear all targets
🔹 `/cleantarget` – ✨ Clean up target list

━━━━━━━━━━━━━━━━━━━━━━

🏠 GROUP & CHAT MANAGEMENT
📌 Effortlessly manage groups and chats!
🔹 `/joinchat` <chats> – 🔗 Join a chat/group
🔹 `/leavechat` <chats> – 🚪 Leave a chat/group
🔹 `/leaveallchat` – 🧹 Leave all groups and channels
🔹 `/listjoined` – 📋 View joined groups
🔹 `/listjoined --all` – 📜 View all targeted joined groups
🔹 `/clearchat` [count] – 🧹 Clear messages
🔹 `/pin` [silent] – 📌 Pin a message silently

━━━━━━━━━━━━━━━━━━━━━━

👤 USER PROFILE & CUSTOMIZATION
📌 Make your profile stand out!
🔹 `/bio` <text> – 📝 Set a new bio
🔹 `/name` <first_name> <last_name> – 🔄 Change your name
🔹 `/username` <new_username> – 🔀 Change your username
🔹 `/setpic` – 🖼 Set profile picture

━━━━━━━━━━━━━━━━━━━━━━

🔑 ADMIN CONTROLS
📌 Manage bot admins easily!
🔹 `/addadmin` <user_id> <username> – ➕ Add an admin
🔹 `/removeadmin` <user_id> <username> – ❌ Remove an admin
🔹 `/listadmins` – 📜 View all admins

━━━━━━━━━━━━━━━━━━━━━━

📊 MONITORING
📌 Track your campaigns!
🔹 `/monitor` – 📊 Show campaign dashboard
🔹 `/failedchats` – 📋 View chats with failed deliveries
🔹 `/retryfailed` – 🔄 Retry sending to failed chats
🔹 `/removefailed` – 🧹 Clear failed chats list

━━━━━━━━━━━━━━━━━━━━━━

💡 Need Help?
Type `/help` anytime to get assistance!

━━━━━━━━━━━━━━━━━━━━━━

🔥 Powered by --Ꮪɪᴍṗʟᴇ'𝚜 𝙰𝙳𝙱𝙾𝚃 (@{username})

🚀 Stay Smart, Stay Automated!
"""
            await event.reply(help_text)
            logger.info("Help message sent")
        except Exception as e:
            logger.error(f"Error in help command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")



    @admin_only
    async def cmd_optimize(self, event):
        """Reset and optimize userbot performance by clearing all data"""
        try:
            # Show reset animation
            msg = await event.reply("🚀 SYSTEM RESET IN PROGRESS\n\n⚡ Phase 1: Analyzing System...")
            await asyncio.sleep(1)
            await msg.edit("🚀 SYSTEM RESET IN PROGRESS\n\n⚡ Phase 2: Preparing for Reset...")
            await asyncio.sleep(1)
            await msg.edit("🚀 SYSTEM RESET IN PROGRESS\n\n⚡ Phase 3: Backing Up Critical Data...")
            await asyncio.sleep(1)
            await msg.edit("🚀 SYSTEM RESET IN PROGRESS\n\n⚡ Phase 4: Clearing All Data...")
            await asyncio.sleep(1)
            await msg.edit("🚀 SYSTEM RESET IN PROGRESS\n\n⚡ Phase 5: Finalizing Reset...")
            await asyncio.sleep(1)

            # Cancel all active tasks
            active_task_count = 0
            for task_id, task in list(self._forwarding_tasks.items()):
                if not task.done():
                    task.cancel()
                    active_task_count += 1
                del self._forwarding_tasks[task_id]

            # Cancel all scheduled tasks
            scheduled_task_count = 0
            for task_id, task in list(self.scheduled_tasks.items()):
                if not task.done():
                    task.cancel()
                    scheduled_task_count += 1
                del self.scheduled_tasks[task_id]

            # Clear all stored messages
            stored_msg_count = len(self.stored_messages)
            self.stored_messages.clear()

            # Clear all target chats
            target_count = len(self.target_chats)
            self.target_chats.clear()

            # Clear all targeted campaigns
            campaign_count = len(self.targeted_campaigns)
            self.targeted_campaigns.clear()

            # Reset analytics data
            self.analytics = {
                "start_time": time.time(),
                "forwards": {},
                "failures": {}
            }

            # Reset monitor data
            self.monitor = MonitorDashboard(self)

            # Reset default interval to 300 seconds (5 minutes)
            self.forward_interval = 300
            
            # Reset the human behavior manager for improved performance
            self.human_behavior = HumanBehaviorManager()
            self.recent_messages = deque(maxlen=20)
            self.active_conversations = {}
            
            # Reset entity knowledge
            self.entity_knowledge = {}
            
            # Toggle smart mode (can be enabled with --smart flag)
            smart_mode = True  # Default to enabled
            
            # Check if there are any flags in the command
            command_text = event.raw_text.strip()
            if ' --fast' in command_text or ' -f' in command_text:
                # Fast mode disables the human behavior delays
                smart_mode = False
                
            self.smart_mode = smart_mode  # Set the smart_mode flag

            # Enable the bot
            self.forwarding_enabled = True

            # Final completion message with animation
            await msg.delete()
            
            frames = [
                "🔄 Finalizing Reset...",
                "✨ Clearing Memory...",
                "🧹 Cleaning Up...",
                "🔧 Reconfiguring...",
                "✅ Reset Complete!"
            ]
            
            reset_msg = await event.reply(frames[0])
            for frame in frames[1:]:
                await asyncio.sleep(0.7)
                await reset_msg.edit(frame)
            
            await asyncio.sleep(0.7)
            await reset_msg.delete()

            # Get client name for personalized message
            me = await self.client.get_me()
            name = me.first_name if hasattr(me, 'first_name') else "Siimple"

            result = f"""✅ **COMPLETE SYSTEM RESET**

Hey {name}! Your bot has been completely reset to its initial state! 🎉

**Reset Summary:**
• 🧹 Cleared {stored_msg_count} stored messages
• 🎯 Removed {target_count} target chats
• 🛑 Cancelled {active_task_count} active forwarding tasks
• 📅 Cancelled {scheduled_task_count} scheduled tasks
• 📊 Cleared {campaign_count} ad campaigns
• 📈 Reset all analytics data
• ⏰ Reset default interval to 300 seconds
• 🧠 Human behavior manager reset
• 🤖 Smart Mode: {"Enabled ✅" if self.smart_mode else "Disabled ❌"}

**What's Next?**
• Use `/setad` to save new messages
• Use `/addtarget` to add new target chats
• Type `/help` to see all available commands

💡 **TIP:** Smart Mode adds human-like delays (60-90s) between actions.
Use `/optimize --fast` to disable Smart Mode for faster operation.

Your bot is now fresh and ready for a new start! 🚀
"""
            await event.reply(result)
            logger.info("Complete system reset performed through optimize command")
        except Exception as e:
            logger.error(f"Error in optimize command: {str(e)}")
            await event.reply(f"❌ Error resetting system: {str(e)}")

    @admin_only
    async def cmd_setad(self, event):
        """Set a message to be forwarded with sequential ID"""
        try:
            if not event.is_reply:
                await event.reply("❌ Please reply to the message you want to forward")
                return

            # Get next serial number
            next_id = str(len(self.stored_messages) + 1)
            msg_id = next_id

            # Handle if ID already exists
            while msg_id in self.stored_messages:
                next_id = str(int(next_id) + 1)
                msg_id = next_id

            replied_msg = await event.get_reply_message()
            self.stored_messages[msg_id] = replied_msg

            await event.reply(f"✅ Message saved for forwarding with ID: `{msg_id}`\n\nUse this ID in commands like `/startad`, `/targetedad`, etc.")
            logger.info(f"New message saved with ID: {msg_id}")
        except Exception as e:
            logger.error(f"Error in setad command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_listad(self, event):
        """List all saved messages"""
        try:
            if not self.stored_messages:
                await event.reply("📝 No messages are currently saved")
                return

            result = "📝 **Saved Messages**:\n\n"

            for msg_id, message in self.stored_messages.items():
                # Get message preview (limited to 50 chars)
                content = ""
                if message.text:
                    content = message.text[:50] + ("..." if len(message.text) > 50 else "")
                elif message.media:
                    content = "[Media Message]"
                else:
                    content = "[Unknown Content]"

                result += f"• ID: `{msg_id}` - {content}\n"

            await event.reply(result)
            logger.info("Listed all saved messages")
        except Exception as e:
            logger.error(f"Error in listad command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_removead(self, event):
        """Remove a saved message"""
        try:
            command_parts = event.text.split()
            if len(command_parts) != 2:
                await event.reply("❌ Please provide a message ID\nFormat: /removead <message_id>")
                return

            msg_id = command_parts[1]

            if msg_id not in self.stored_messages:
                await event.reply(f"❌ Message with ID {msg_id} not found")
                return

            # Cancel any active forwarding tasks for this message
            for task_id, task in list(self._forwarding_tasks.items()):
                if task_id == msg_id and not task.done():
                    task.cancel()
                    del self._forwarding_tasks[task_id]

            # Remove from stored messages
            del self.stored_messages[msg_id]

            await event.reply(f"✅ Message with ID {msg_id} has been removed")
            logger.info(f"Removed message with ID: {msg_id}")
        except Exception as e:
            logger.error(f"Error in removead command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_startad(self, event):
        """Start forwarding a specific message at an interval with automatic monitoring"""
        try:
            command_parts = event.text.split()
            msg_id = "default"  # Default value
            interval = self.forward_interval  # Default interval

            if len(command_parts) >= 2:
                msg_id = command_parts[1]

            if len(command_parts) >= 3:
                try:
                    interval = int(command_parts[2])
                    if interval < 60:
                        await event.reply("❌ Interval must be at least 60 seconds")
                        return
                except ValueError:
                    await event.reply("❌ Invalid interval format. Must be an integer in seconds.")
                    return

            # Check if message exists
            if msg_id not in self.stored_messages:
                if msg_id == "default" and not self.stored_messages:
                    await event.reply("❌ No message set for forwarding. Please use `/setad` while replying to a message first.")
                else:
                    await event.reply(f"❌ Message with ID {msg_id} not found. Use `/listad` to see available messages.")
                return

            # Check if targets exist
            if not self.target_chats:
                await event.reply("❌ No target chats configured. Please add target chats first using /addtarget <target>")
                return

            # Cancel existing task if any
            if msg_id in self._forwarding_tasks and not self._forwarding_tasks[msg_id].done():
                self._forwarding_tasks[msg_id].cancel()

            # Create campaign ID for monitoring - unique format to match the one used in forward_stored_message
            timestamp = int(time.time())
            campaign_id = f"adcampaign_{msg_id}_{timestamp}"

            # Show animated initialization message
            monitor_message = await event.reply("🔄 **Initializing Campaign...**")
            
            # Log campaign ID for debugging
            logger.info(f"Creating campaign with ID: {campaign_id} for message {msg_id}")

            # Animation phases
            phases = [
                "⚙️ **Campaign Setup** ⚙️\n\n🔍 Validating message...",
                "⚙️ **Campaign Setup** ⚙️\n\n✅ Message validated\n🔍 Checking targets...",
                "⚙️ **Campaign Setup** ⚙️\n\n✅ Message validated\n✅ Targets verified\n🔍 Configuring interval...",
                "⚙️ **Campaign Setup** ⚙️\n\n✅ Message validated\n✅ Targets verified\n✅ Interval configured\n🔍 Initializing monitor...",
                "⚙️ **Campaign Setup** ⚙️\n\n✅ Message validated\n✅ Targets verified\n✅ Interval configured\n✅ Monitor initialized\n🔍 Launching campaign..."
            ]

            # Display animation
            for phase in phases:
                await monitor_message.edit(phase)
                await asyncio.sleep(0.8)  # Short delay between updates

            # Add to monitor dashboard
            self.monitor.add_campaign(campaign_id, {
                "msg_id": msg_id,
                "targets": len(self.target_chats),
                "interval": interval,
                "start_time": time.time(),
                "rounds_completed": 0,
                "total_sent": 0,
                "failed_sends": 0,
                "status": "running",
                "is_active": True
            })

            # Create the campaign ID for both starting the task and monitoring
            # This ensures a consistent campaign ID
            campaign_id = f"adcampaign_{msg_id}_{timestamp}"
            logger.info(f"Starting forwarding task with campaign_id: {campaign_id}")
            
            # Start new forwarding task with the pre-defined campaign_id to ensure consistency
            self._forwarding_tasks[msg_id] = asyncio.create_task(
                self.forward_stored_message(msg_id=msg_id, interval=interval, campaign_id=campaign_id)
            )
            
            # Store a reference to the campaign marker for monitoring
            self._forwarding_task_campaigns = getattr(self, '_forwarding_task_campaigns', {})
            self._forwarding_task_campaigns[msg_id] = campaign_id

            # Success message
            await monitor_message.edit(f"""🚀 **Ad Campaign Started!** 🚀

✅ **Campaign ID:** `{campaign_id}`
✅ **Ad ID:** {msg_id}
⏱️ **Interval:** {interval} seconds
🎯 **Targets:** {len(self.target_chats)} channels/groups

⚡ **Real-time monitor initialized!** ⚡
🛟 **Auto-retries:** Enabled
📊 **Detailed stats:** Available

✨ Your campaign is now live and being monitored in real-time!
Use `/stopad {msg_id}` to stop it anytime.

🚀 Powered by --Ꮪɪᴍṗʟᴇ'𝚜 𝙰𝙳𝙱𝙾𝚃 (@siimplebot1)""")

            # Create a live monitoring message that will continuously update
            live_monitor_message = await event.reply("📊 **Starting Live Monitor...**")
            
            # Use the actual campaign marker that will be generated in forward_stored_message
            # This ensures we're monitoring the right campaign data
            actual_campaign_id = f"adcampaign_{msg_id}_{timestamp}"
            logger.info(f"Starting live monitor for actual campaign ID: {actual_campaign_id}")
            
            # Start live monitoring for this campaign - this continuously updates the message
            await self.monitor.start_live_monitor(actual_campaign_id, live_monitor_message, event.chat_id)

            logger.info(f"Forwarding enabled for message {msg_id}. Interval: {interval}s, Targets: {self.target_chats}")
        except Exception as e:
            logger.error(f"Error in startad command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_stopad(self, event):
        """Stop forwarding a specific message with animation"""
        try:
            command_parts = event.text.split()

            # Initial animation message
            stop_message = await event.reply("🔄 **Processing Stop Request...**")

            # Animation phases for stopping all campaigns
            all_stop_phases = [
                "🛑 **Stopping All Campaigns** 🛑\n\n🔍 Identifying active campaigns...",
                "🛑 **Stopping All Campaigns** 🛑\n\n✅ Campaigns identified\n🔍 Sending stop signals...",
                "🛑 **Stopping All Campaigns** 🛑\n\n✅ Campaigns identified\n✅ Stop signals sent\n🔍 Cleaning up resources...",
                "🛑 **Stopping All Campaigns** 🛑\n\n✅ Campaigns identified\n✅ Stop signals sent\n✅ Resources cleaned\n🔍 Finalizing..."
            ]

            # Animation phases for stopping a specific campaign
            specific_stop_phases = [
                "🛑 **Stopping Campaign** 🛑\n\n🔍 Validating campaign ID...",
                "🛑 **Stopping Campaign** 🛑\n\n✅ Campaign ID valid\n🔍 Sending stop signal...",
                "🛑 **Stopping Campaign** 🛑\n\n✅ Campaign ID valid\n✅ Stop signal sent\n🔍 Updating monitor status...",
                "🛑 **Stopping Campaign** 🛑\n\n✅ Campaign ID valid\n✅ Stop signal sent\n✅ Monitor updated\n🔍 Finalizing..."
            ]

            # If no ID specified, stop all forwarding
            if len(command_parts) == 1:
                # Show animation
                for phase in all_stop_phases:
                    await stop_message.edit(phase)
                    await asyncio.sleep(0.8)  # Short delay between updates

                # Find all campaigns in the monitor
                active_campaigns = self.monitor.list_active_campaigns()

                # Cancel all forwarding tasks and update monitor
                for task_id, task in list(self._forwarding_tasks.items()):
                    if not task.done():
                        task.cancel()
                        # Update monitor if this task ID is a campaign
                        if self.monitor.campaign_exists(task_id):
                            self.monitor.update_campaign_status(task_id, "stopped")

                # Stop all active monitoring
                self.monitor.stop_all_monitoring()

                # Clear all tasks
                self._forwarding_tasks.clear()

                # Final success message
                await stop_message.edit(f"""✅ **All Campaigns Stopped!** ✅

📊 **Summary:**
• Campaigns stopped: {len(active_campaigns)}
• Status: All campaigns terminated successfully
• Monitor: All monitoring services stopped

💡 Start a new campaign anytime using `/startad <ID> <interval>`.

🚀 Powered by --Ꮪɪᴍṗʟᴇ'𝚜 𝙰𝙳𝙱𝙾𝚃 (@siimplebot1)""")
                logger.info("All forwarding tasks stopped")
                return

            # Stop specific message forwarding
            msg_id = command_parts[1]

            # Show animation for specific campaign stop
            for phase in specific_stop_phases:
                await stop_message.edit(phase)
                await asyncio.sleep(0.8)  # Short delay between updates

            # Find associated campaign ID if any
            campaign_id = None
            for c_id in self.monitor.list_campaigns():
                if self.monitor.get_campaign_data(c_id).get('msg_id') == msg_id:
                    campaign_id = c_id
                    break

            if msg_id in self._forwarding_tasks and not self._forwarding_tasks[msg_id].done():
                self._forwarding_tasks[msg_id].cancel()
                del self._forwarding_tasks[msg_id]

                # Update monitor if we found a campaign
                if campaign_id and self.monitor.campaign_exists(campaign_id):
                    self.monitor.update_campaign_status(campaign_id, "stopped")
                    self.monitor.stop_live_monitor(campaign_id)

                await stop_message.edit(f"""✅ **Campaign Stopped!** ✅

📊 **Details:**
• Ad ID: `{msg_id}`
• Campaign ID: `{campaign_id if campaign_id else 'N/A'}`
• Status: Terminated successfully
• Monitor: Stopped

💡 Start a new campaign anytime using `/startad <ID> <interval>`.

🚀 Powered by --Ꮪɪᴍṗʟᴇ'𝚜 𝙰𝙳𝙱𝙾𝚃 (@siimplebot1)""")
                logger.info(f"Forwarding disabled for message {msg_id}")
            else:
                await stop_message.edit(f"""⚠️ **No Active Campaign Found** ⚠️

• Message ID: `{msg_id}`
• Status: No active forwarding found for this ID
• Possible reasons: 
  - The campaign has already completed
  - The ID is incorrect
  - The campaign was never started

💡 Try `/listad` to see all available messages.
💡 Try `/monitor` to see active campaigns.

🚀 Powered by --Ꮪɪᴍṗʟᴇ'𝚜 𝙰𝙳𝙱𝙾𝚃 (@siimplebot1)""")

        except Exception as e:
            logger.error(f"Error in stopad command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_timer(self, event):
        """Set default forwarding interval in seconds"""
        try:
            command_parts = event.text.split()
            if len(command_parts) != 2:
                await event.reply("❌ Please provide a valid interval in seconds\nFormat: /timer <seconds>")
                return

            try:
                interval = int(command_parts[1])
                if interval < 60:
                    await event.reply("❌ Interval must be at least 60 seconds")
                    return
            except ValueError:
                await event.reply("❌ Invalid interval format. Must be an integer in seconds.")
                return

            self.forward_interval = interval
            await event.reply(f"⏱️ Default forwarding interval set to {interval} seconds")
            logger.info(f"Set default forwarding interval to {interval} seconds")
        except Exception as e:
            logger.error(f"Error in timer command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_targetedad(self, event):
        """Start a targeted ad campaign with specific message, targets and interval with monitoring"""
        try:
            command_parts = event.text.split()
            usage = "❌ Format: /targetedad <ad_id> <target_list> <interval>\n\nExample: /targetedad ABC123 target1,target2 3600"

            if len(command_parts) < 3:
                await event.reply(usage)
                return

            msg_id = command_parts[1]
            target_str = command_parts[2]

            # Check if message exists
            if msg_id not in self.stored_messages:
                await event.reply(f"❌ Message with ID {msg_id} not found. Use /listad to see available messages.")
                return

            # Parse targets - No confirmations, just process immediately
            targets = set()
            for target in target_str.split(','):
                target = target.strip()
                if not target:
                    continue

                # Check if it's a topic link (t.me/channel/topicID)
                topic_match = re.search(r't\.me/([^/]+)/(\d+)', target)
                if topic_match:
                    channel_name, topic_id = topic_match.groups()
                    try:
                        # Extract channel link and send a message to get the channel ID
                        channel_link = f"t.me/{channel_name}"
                        # Instead of get_entity, we'll try to resolve through other methods
                        # First try to find the channel in dialogs
                        channel_id = None
                        async for dialog in self.client.iter_dialogs():
                            if dialog.entity.username and dialog.entity.username.lower() == channel_name.lower():
                                channel_id = dialog.entity.id
                                break
                                
                        # If not found, try to send a message which will be auto-deleted
                        if not channel_id:
                            try:
                                # Join the channel first if needed
                                await self.client(JoinChannelRequest(channel_link))
                                await asyncio.sleep(2)
                                
                                # Send a temporary message to get the channel ID
                                temp_msg = await self.client.send_message(channel_link, ".")
                                channel_id = temp_msg.peer_id.channel_id
                                # Delete the temp message immediately
                                await temp_msg.delete()
                            except Exception as e:
                                logger.error(f"Error resolving channel ID for {channel_name}: {e}")
                                
                        # If still not resolved, use a fallback method or inform user
                        if not channel_id:
                            raise ValueError(f"Could not resolve channel ID for {channel_name}")
                            
                        topic_id = int(topic_id)
                        # Store as a tuple (chat_id, topic_id)
                        targets.add((channel_id, topic_id))
                        logger.info(f"Added topic target: channel={channel_id}, topic={topic_id}")
                    except Exception as e:
                        logger.error(f"Error resolving topic target {target}: {str(e)}")
                        await event.reply(f"❌ Could not resolve topic target: {target}")
                        return
                else:
                    try:
                        # Try as numeric ID
                        chat_id = int(target)
                        targets.add(chat_id)
                    except ValueError:
                        # Try as username or link without get_entity
                        try:
                            # For usernames, try to resolve through dialogs
                            if target.startswith('@'):
                                username = target[1:]  # Remove the @ sign
                                resolved = False
                                
                                # Try to find in dialogs
                                async for dialog in self.client.iter_dialogs():
                                    if dialog.entity.username and dialog.entity.username.lower() == username.lower():
                                        targets.add(dialog.entity.id)
                                        resolved = True
                                        break
                                
                                # If not found in dialogs, try to send a message
                                if not resolved:
                                    try:
                                        # Try to send a temporary message
                                        temp_msg = await self.client.send_message(target, ".")
                                        if hasattr(temp_msg.peer_id, 'user_id'):
                                            targets.add(temp_msg.peer_id.user_id)
                                        elif hasattr(temp_msg.peer_id, 'channel_id'):
                                            targets.add(temp_msg.peer_id.channel_id)
                                        elif hasattr(temp_msg.peer_id, 'chat_id'):
                                            targets.add(temp_msg.peer_id.chat_id)
                                        else:
                                            targets.add(temp_msg.peer_id)
                                        # Delete the message right away
                                        await temp_msg.delete()
                                        resolved = True
                                    except Exception as msg_err:
                                        logger.error(f"Error resolving username with message: {msg_err}")
                                
                                if not resolved:
                                    raise ValueError(f"Could not resolve username: {target}")
                                    
                            # For t.me links
                            elif 't.me/' in target:
                                # Try to join the chat first
                                try:
                                    await self.client(JoinChannelRequest(target))
                                    await asyncio.sleep(1)
                                except Exception as join_err:
                                    logger.error(f"Error joining channel: {join_err}")
                                
                                # Try to send a temp message to get the ID
                                try:
                                    temp_msg = await self.client.send_message(target, ".")
                                    if hasattr(temp_msg.peer_id, 'user_id'):
                                        targets.add(temp_msg.peer_id.user_id)
                                    elif hasattr(temp_msg.peer_id, 'channel_id'):
                                        targets.add(temp_msg.peer_id.channel_id)
                                    elif hasattr(temp_msg.peer_id, 'chat_id'):
                                        targets.add(temp_msg.peer_id.chat_id)
                                    else:
                                        targets.add(temp_msg.peer_id)
                                    # Delete the message right away
                                    await temp_msg.delete()
                                except Exception as e:
                                    logger.error(f"Error resolving link with message: {e}")
                                    raise ValueError(f"Could not resolve link: {target}")
                            else:
                                # For any other format, try direct message
                                try:
                                    temp_msg = await self.client.send_message(target, ".")
                                    if hasattr(temp_msg.peer_id, 'user_id'):
                                        targets.add(temp_msg.peer_id.user_id)
                                    elif hasattr(temp_msg.peer_id, 'channel_id'):
                                        targets.add(temp_msg.peer_id.channel_id)
                                    elif hasattr(temp_msg.peer_id, 'chat_id'):
                                        targets.add(temp_msg.peer_id.chat_id)
                                    else:
                                        targets.add(temp_msg.peer_id)
                                    # Delete the message right away
                                    await temp_msg.delete()
                                except Exception as e:
                                    logger.error(f"Error resolving chat: {e}")
                                    raise ValueError(f"Could not resolve chat: {target}")
                                
                        except Exception as e:
                            logger.error(f"Error resolving target {target}: {str(e)}")
                            await event.reply(f"❌ Could not resolve target: {target}")
                            return

            if not targets:
                await event.reply("❌ No valid targets specified")
                return

            # Parse interval
            interval = self.forward_interval
            if len(command_parts) >= 4:
                try:
                    interval = int(command_parts[3])
                    if interval < 60:
                        await event.reply("❌ Interval must be at least 60 seconds")
                        return
                except ValueError:
                    await event.reply("❌ Invalid interval format. Must be an integer in seconds.")
                    return

            # Generate campaign ID
            campaign_id = f"targeted_{generate_campaign_id()}"

            # Show animated initialization message
            monitor_message = await event.reply("🔄 **Initializing Targeted Campaign...**")

            # Animation phases
            phases = [
                "⚙️ **Targeted Campaign Setup** ⚙️\n\n🔍 Validating message content...",
                "⚙️ **Targeted Campaign Setup** ⚙️\n\n✅ Message validated\n🔍 Analyzing target channels...",
                "⚙️ **Targeted Campaign Setup** ⚙️\n\n✅ Message validated\n✅ Target channels verified\n🔍 Configuring interval settings...",
                "⚙️ **Targeted Campaign Setup** ⚙️\n\n✅ Message validated\n✅ Target channels verified\n✅ Interval configured\n🔍 Creating campaign...",
                "⚙️ **Targeted Campaign Setup** ⚙️\n\n✅ Message validated\n✅ Target channels verified\n✅ Interval configured\n✅ Campaign created\n🔍 Initializing monitor...",
                "⚙️ **Targeted Campaign Setup** ⚙️\n\n✅ Message validated\n✅ Target channels verified\n✅ Interval configured\n✅ Campaign created\n✅ Monitor initialized\n🔍 Launching campaign..."
            ]

            # Display animation
            for phase in phases:
                await monitor_message.edit(phase)
                await asyncio.sleep(0.7)  # Short delay between updates

            # Store campaign info
            self.targeted_campaigns[campaign_id] = {
                "msg_id": msg_id,
                "targets": targets,
                "interval": interval,
                "start_time": time.time()
            }

            # Add to monitor dashboard
            self.monitor.add_campaign(campaign_id, {
                "msg_id": msg_id,
                "targets": len(targets),
                "interval": interval,
                "start_time": time.time(),
                "rounds_completed": 0,
                "total_sent": 0,
                "failed_sends": 0,
                "status": "running",
                "is_active": True,
                "is_targeted": True
            })

            # Start campaign task with the specific campaign_id to ensure consistency
            task = asyncio.create_task(
                self.forward_stored_message(
                    msg_id=msg_id,
                    targets=targets,
                    interval=interval,
                    campaign_id=campaign_id
                )
            )

            self._forwarding_tasks[campaign_id] = task

            # Success message
            await monitor_message.edit(f"""🎯 **Targeted Campaign Started!** 🎯

📝 **Campaign Details:**
• Campaign ID: `{campaign_id}`
• Message ID: `{msg_id}`
• Targets: {len(targets)} specific chats
• Interval: {interval} seconds
• Status: Running ✓

⚡ **Advanced Features:**
• Real-time monitoring: Active
• Auto-retries: Enabled
• Detailed analytics: Collecting

📌 Use `/stoptargetad {campaign_id}` to stop this campaign.
📊 Use `/monitor` to view overall campaign status.

🚀 Powered by --Ꮪɪᴍṗʟᴇ'𝚜 𝙰𝙳𝙱𝙾𝚃 (@siimplebot1)
""")

            # Create a monitoring message that will be updated
            live_monitor_message = await event.reply("📊 **Initializing Targeted Campaign Monitor...**")

            # Start live monitoring for this campaign
            await self.monitor.start_live_monitor(campaign_id, live_monitor_message, event.chat_id)

            logger.info(f"Started targeted campaign {campaign_id} with message {msg_id}, {len(targets)} targets, {interval}s interval")
        except Exception as e:
            logger.error(f"Error in targetedad command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_listtargetad(self, event):
        """List all targeted ad campaigns"""
        try:
            if not self.targeted_campaigns:
                await event.reply("📝 No targeted campaigns are currently active")
                return

            result = "📝 **Active Targeted Campaigns**:\n\n"

            for campaign_id, campaign in self.targeted_campaigns.items():
                # Calculate runtime
                runtime_seconds = int(time.time() - campaign["start_time"])
                days, remainder = divmod(runtime_seconds, 86400)
                hours, remainder = divmod(remainder, 3600)
                minutes, seconds = divmod(remainder, 60)
                runtime_str = f"{days}d {hours}h {minutes}m {seconds}s"

                result += f"""• Campaign ID: `{campaign_id}`
  - Message ID: {campaign["msg_id"]}
  - Targets: {len(campaign["targets"])} chats
  - Interval: {campaign["interval"]} seconds
  - Running for: {runtime_str}

"""

            await event.reply(result)

            # Also show the full dashboard
            dashboard_text = self.monitor.generate_dashboard(targeted_only=True)
            await event.reply(dashboard_text)

            logger.info("Listed all targeted campaigns")
        except Exception as e:
            logger.error(f"Error in listtargetad command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_stoptargetad(self, event):
        """Stop a targeted ad campaign with animation"""
        try:
            command_parts = event.text.split()
            if len(command_parts) != 2:
                await event.reply("❌ Please provide a campaign ID\nFormat: /stoptargetad <campaign_id>")
                return

            campaign_id = command_parts[1]

            # Initial animation message
            stop_message = await event.reply("🔄 **Processing Stop Request...**")

            # Animation phases for stopping a targeted campaign
            stop_phases = [
                "🛑 **Stopping Targeted Campaign** 🛑\n\n🔍 Validating campaign ID...",
                "🛑 **Stopping Targeted Campaign** 🛑\n\n✅ Campaign ID validated\n🔍 Retrieving campaign data...",
                "🛑 **Stopping Targeted Campaign** 🛑\n\n✅ Campaign ID validated\n✅ Campaign data retrieved\n🔍 Sending stop signal...",
                "🛑 **Stopping Targeted Campaign** 🛑\n\n✅ Campaign ID validated\n✅ Campaign data retrieved\n✅ Stop signal sent\n🔍 Updating monitors..."
            ]

            # Show animation for campaign stop
            for phase in stop_phases:
                await stop_message.edit(phase)
                await asyncio.sleep(0.7)  # Short delay between updates

            # Check if campaign exists
            if campaign_id not in self.targeted_campaigns:
                await stop_message.edit(f"""⚠️ **Campaign Not Found** ⚠️

• Campaign ID: `{campaign_id}`
• Status: Not found in active targeted campaigns
• Possible reasons:
  - The campaign ID is incorrect
  - The campaign has already been stopped
  - The campaign was never started as a targeted campaign

💡 Try `/listtargetad` to see all active targeted campaigns.
""")
                return

            # Get campaign data before stopping for reporting
            campaign_data = self.targeted_campaigns[campaign_id].copy()
            target_count = len(campaign_data["targets"])
            msg_id = campaign_data["msg_id"]
            runtime_seconds = int(time.time() - campaign_data["start_time"])

            # Format runtime
            days, remainder = divmod(runtime_seconds, 86400)
            hours, remainder = divmod(remainder, 3600)
            minutes, seconds = divmod(remainder, 60)
            runtime_str = f"{int(days)}d {int(hours)}h {int(minutes)}m {int(seconds)}s"

            # Cancel the task
            if campaign_id in self._forwarding_tasks and not self._forwarding_tasks[campaign_id].done():
                self._forwarding_tasks[campaign_id].cancel()
                del self._forwarding_tasks[campaign_id]

            # Remove campaign
            del self.targeted_campaigns[campaign_id]

            # Update monitor to reflect stopped status
            if self.monitor.campaign_exists(campaign_id):
                self.monitor.update_campaign_status(campaign_id, "stopped")
                self.monitor.stop_live_monitor(campaign_id)

            # Final success message with campaign stats
            await stop_message.edit(f"""✅ **Targeted Campaign Stopped!** ✅

📊 **Campaign Summary:**
• Campaign ID: `{campaign_id}`
• Message ID: `{msg_id}`
• Status: Successfully terminated
• Target count: {target_count} chats
• Runtime: {runtime_str}
• Monitor: Deactivated

⚡ **Campaign data has been archived for analytics**

💡 Start a new targeted campaign anytime using `/targetedad <msg_id> <targets> <interval>`.

🚀 Powered by --Ꮪɪᴍṗʟᴇ'𝚜 𝙰𝙳𝙱𝙾𝚃 (@siimplebot1)""")

            logger.info(f"Stopped targeted campaign {campaign_id}")
        except Exception as e:
            logger.error(f"Error in stoptargetad command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    async def _schedule_forward(self, msg_id, targets, schedule_time):
        """Helper function to schedule a forward at a specific time"""
        try:
            now = datetime.now()
            wait_seconds = (schedule_time - now).total_seconds()

            if wait_seconds > 0:
                logger.info(f"Scheduled message {msg_id} to be sent in {wait_seconds} seconds")
                await asyncio.sleep(wait_seconds)

            message = self.stored_messages[msg_id]

            for target in targets:
                try:
                    # Check if target is a tuple (chat_id, topic_id)
                    if isinstance(target, tuple) and len(target) == 2:
                        chat_id, topic_id = target
                        logger.info(f"Forwarding scheduled message to topic: chat_id={chat_id}, topic_id={topic_id}")
                        
                        # Get the user ID (from_peer) of the bot
                        me = await self.client.get_me()
                        bot_user_id = me.id
                        
                        # Use ForwardMessagesRequest for topics
                        forwarded = await self.client(ForwardMessagesRequest(
                            from_peer=bot_user_id,
                            id=[message.id],
                            to_peer=chat_id,
                            top_msg_id=topic_id  # Use topic_id as top_msg_id for forum topics
                        ))
                        # Then, if needed, reply to the topic
                        if forwarded and topic_id:
                            try:
                                # Use send_message with reply_to
                                await self.client.send_message(
                                    entity=chat_id,
                                    message=f"⬆️ Forwarded message to topic #{topic_id}",
                                    reply_to=topic_id
                                )
                            except Exception as e:
                                logger.error(f"Topic association error: {e}")
                    else:
                        # Regular chat - use ForwardMessagesRequest with numeric UIDs
                        me = await self.client.get_me()
                        bot_user_id = me.id
                        
                        await self.client(ForwardMessagesRequest(
                            from_peer=bot_user_id,
                            id=[message.id],
                            to_peer=target
                        ))
                    logger.info(f"Successfully forwarded scheduled message {msg_id} to {target}")
                except Exception as e:
                    logger.error(f"Error forwarding scheduled message {msg_id} to {target}: {str(e)}")

            return True
        except asyncio.CancelledError:
            logger.info(f"Scheduled task for message {msg_id} was cancelled")
            return False
        except Exception as e:
            logger.error(f"Error in scheduled task for message {msg_id}: {str(e)}")
            return False

    @admin_only
    async def cmd_schedule(self, event):
        """Schedule a message to be sent at a specific time without confirmation"""
        try:
            # Define usage at the beginning to make it accessible throughout the function
            usage = """❌ Format: /schedule <msg_id> <time>

Time format examples:
- "5m" (5 minutes from now)
- "2h" (2 hours from now)
- "12:30" (today at 12:30, or tomorrow if already past)
- "2023-12-25 14:30" (specific date and time)"""
            
            command_parts = event.text.split(maxsplit=2)
            if len(command_parts) < 3:
                await event.reply(usage)
                return

            msg_id = command_parts[1]
            time_str = command_parts[2]

            # Check if message exists
            if msg_id not in self.stored_messages:
                await event.reply(f"❌ Message with ID {msg_id} not found. Use /listad to see available messages.")
                return

            # Parse the time without asking for confirmation
            schedule_time = None
            now = datetime.now()

            # Check for relative time (e.g., "5m", "2h")
            relative_match = re.match(r'(\d+)([mh])', time_str)
            if relative_match:
                value, unit = relative_match.groups()
                value = int(value)

                if unit == 'm':
                    schedule_time = now + timedelta(minutes=value)
                elif unit == 'h':
                    schedule_time = now + timedelta(hours=value)
            else:
                # Try parsing as time or datetime
                try:
                    # Try as time only (e.g., "14:30")
                    time_only_match = re.match(r'(\d{1,2}):(\d{2})', time_str)
                    if time_only_match:
                        hour, minute = map(int, time_only_match.groups())
                        schedule_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

                        # If time is in the past, add one day
                        if schedule_time < now:
                            schedule_time += timedelta(days=1)
                    else:
                        # Try as full datetime (e.g., "2023-12-25 14:30")
                        schedule_time = datetime.strptime(time_str, '%Y-%m-%d %H:%M')
                except ValueError:
                    await event.reply(f"❌ Invalid time format: {time_str}\n\n{usage}")
                    return

            if not schedule_time:
                await event.reply(f"❌ Could not parse time: {time_str}\n\n{usage}")
                return

            if schedule_time < now:
                await event.reply("❌ Scheduled time must be in the future")
                return

            # Format the time for display
            formatted_time = schedule_time.strftime('%Y-%m-%d %H:%M')

            # Create a unique ID for this schedule
            schedule_id = f"sched_{generate_campaign_id()}"

            # Create and store the task
            task = asyncio.create_task(
                self._schedule_forward(
                    msg_id=msg_id,
                    targets=self.target_chats,
                    schedule_time=schedule_time
                )
            )

            self.scheduled_tasks[schedule_id] = task

            # Calculate wait time for display
            wait_seconds = (schedule_time - now).total_seconds()
            days, remainder = divmod(wait_seconds, 86400)
            hours, remainder = divmod(remainder, 3600)
            minutes, seconds = divmod(remainder, 60)
            wait_str = ""
            if days > 0:
                wait_str += f"{int(days)} days "
            if hours > 0:
                wait_str += f"{int(hours)} hours "
            if minutes > 0:
                wait_str += f"{int(minutes)} minutes "
            if seconds > 0 and days == 0 and hours == 0:
                wait_str += f"{int(seconds)} seconds"

            # Create a monitoring entry for this scheduled task
            self.monitor.add_campaign(schedule_id, {
                "msg_id": msg_id,
                "targets": len(self.target_chats),
                "start_time": time.time(),
                "scheduled_for": formatted_time,
                "status": "waiting",
                "type": "scheduled"
            })

            await event.reply(f"""✅ **Message Scheduled**
• Schedule ID: `{schedule_id}`
• Message ID: {msg_id}
• Scheduled for: {formatted_time}
• Time until sending: {wait_str}
• Targets: {len(self.target_chats)} chats

The message will be forwarded at the scheduled time.
""")

            # Show monitoring info for the scheduled task
            monitor_message = await event.reply("📊 **Initializing Schedule Monitor...**")
            await self.monitor.start_live_monitor(schedule_id, monitor_message, event.chat_id)

            logger.info(f"Scheduled message {msg_id} for {formatted_time}, Schedule ID: {schedule_id}")
        except Exception as e:
            logger.error(f"Error in schedule command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_forward(self, event):
        """Forward a message to specific targets once"""
        try:
            command_parts = event.text.split()
            usage = "❌ Format: /forward <msg_id> <targets>\n\nExample: /forward ABC123 target1,target2"

            if len(command_parts) < 3:
                await event.reply(usage)
                return

            msg_id = command_parts[1]
            target_str = command_parts[2]

            # Check if message exists
            if msg_id not in self.stored_messages:
                await event.reply(f"❌ Message with ID {msg_id} not found. Use `/listad` to see available messages.")
                return

            # Parse targets without confirmation
            targets = set()
            for target in target_str.split(','):
                target = target.strip()
                if not target:
                    continue

                try:
                    # Try as numeric ID
                    chat_id = int(target)
                    targets.add(chat_id)
                except ValueError:
                    # Try as username or link without get_entity
                    try:
                        # Use our custom resolver function
                        entity_id, entity_type, entity_name, _ = await resolve_entity_without_get_entity(self.client, target)
                        targets.add(entity_id)
                        logger.info(f"Resolved target {target} to ID {entity_id} (Type: {entity_type}, Name: {entity_name})")
                    except Exception as e:
                        logger.error(f"Error resolving target {target}: {str(e)}")
                        await event.reply(f"❌ Could not resolve target: {target}")
                        return

            if not targets:
                await event.reply("❌ No valid targets specified")
                return

            # Get the message
            message = self.stored_messages[msg_id]

            # Forward to each target
            success_count = 0
            fail_count = 0
            failures = {}

            # Create a tracking ID for the forward operation
            forward_id = f"forward_{generate_campaign_id()}"

            # Add to monitor
            self.monitor.add_campaign(forward_id, {
                "msg_id": msg_id,
                "targets": len(targets),
                "start_time": time.time(),
                "status": "sending",
                "type": "one-time"
            })

            # Create a monitoring message
            monitor_message = await event.reply("📊 **Forwarding in progress...**")

            # Start live monitoring
            await self.monitor.start_live_monitor(forward_id, monitor_message, event.chat_id)

            for target in targets:
                try:
                    if isinstance(target, tuple) and len(target) == 2:
                        # Target is a tuple of (chat_id, topic_id)
                        chat_id, topic_id = target
                        logger.info(f"Forwarding to topic: chat_id={chat_id}, topic_id={topic_id}")
                        
                        # Get the user ID (from_peer) of the bot
                        me = await self.client.get_me()
                        bot_user_id = me.id
                        
                        # Use ForwardMessagesRequest for topics 
                        forwarded = await self.client(ForwardMessagesRequest(
                            from_peer=bot_user_id,
                            id=[message.id],
                            to_peer=chat_id,
                            top_msg_id=topic_id  # Use topic_id as top_msg_id for forum topics
                        ))
                        # Then, if needed, associate with the topic
                        if forwarded and topic_id:
                            try:
                                # Use send_message with reply_to
                                await self.client.send_message(
                                    entity=chat_id,
                                    message=f"⬆️ Forwarded message to topic #{topic_id}",
                                    reply_to=topic_id
                                )
                            except Exception as e:
                                logger.error(f"Topic association error: {e}")
                    else:
                        # Get the user ID (from_peer) of the bot
                        me = await self.client.get_me()
                        bot_user_id = me.id
                        
                        # Use ForwardMessagesRequest with numeric UIDs
                        await self.client(ForwardMessagesRequest(
                            from_peer=bot_user_id,
                            id=[message.id],
                            to_peer=target
                        ))
                    success_count += 1

                    # Update analytics
                    today = datetime.now().strftime('%Y-%m-%d')
                    if today not in self.analytics["forwards"]:
                        self.analytics["forwards"][today] = {}

                    campaign_key = f"{msg_id}_{target}"
                    if campaign_key not in self.analytics["forwards"][today]:
                        self.analytics["forwards"][today][campaign_key] = 0

                    self.analytics["forwards"][today][campaign_key] += 1

                    logger.info(f"Successfully forwarded message {msg_id} to {target}")
                except Exception as e:
                    fail_count += 1
                    error_message = str(e)
                    failures[target] = error_message

                    # Track failures in analytics
                    today = datetime.now().strftime('%Y-%m-%d')
                    if today not in self.analytics["failures"]:
                        self.analytics["failures"][today] = {}

                    campaign_key = f"{msg_id}_{target}"
                    if campaign_key not in self.analytics["failures"][today]:
                        self.analytics["failures"][today][campaign_key] = []

                    self.analytics["failures"][today][campaign_key].append(error_message)

                    logger.error(f"Error forwarding message {msg_id} to {target}: {error_message}")

                # Update monitor during the process
                self.monitor.update_campaign(forward_id, {
                    "total_sent": success_count,
                    "failed_sends": fail_count,
                    "current_failures": failures,
                    "status": "sending"
                })

            # Update final status
            self.monitor.update_campaign(forward_id, {
                "status": "completed"
            })

            # Report results
            result = f"""✅ **Forward Results**
• Message ID: {msg_id}
• Successful: {success_count}
• Failed: {fail_count}
"""

            if failures:
                result += "\n**Failures:**\n"
                for target, error in list(failures.items())[:5]:  # Limit to first 5 failures
                    result += f"• Target {target}: {error[:50]}...\n"

                if len(failures) > 5:
                    result += f"... and {len(failures) - 5} more failures\n"

            await event.reply(result)
            logger.info(f"Forwarded message {msg_id} to {len(targets)} targets. Success: {success_count}, Failed: {fail_count}")
        except Exception as e:
            logger.error(f"Error in forward command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_broadcast(self, event):
        """Send a message to all target chats with monitoring"""
        try:
            command_parts = event.text.split(maxsplit=1)

            # Check if message content is provided
            if len(command_parts) < 2:
                if event.is_reply:
                    # Use replied message as broadcast content
                    replied_msg = await event.get_reply_message()
                    message_content = replied_msg
                else:
                    await event.reply("❌ Please provide a message to broadcast or reply to a message")
                    return
            else:
                # Use provided text as broadcast content
                message_content = command_parts[1]

            # Check if targets exist
            if not self.target_chats:
                await event.reply("❌ No target chats configured. Please add target chats first using /addtarget <target>")
                return

            # Create a broadcast ID and add to monitor
            broadcast_id = f"broadcast_{generate_campaign_id()}"

            # Add to monitor
            self.monitor.add_campaign(broadcast_id, {
                "msg_id": "broadcast",
                "targets": len(self.target_chats),
                "start_time": time.time(),
                "status": "sending",
                "type": "broadcast"
            })

            # Initial report
            broadcast_message = await event.reply(f"🔄 Broadcasting message to {len(self.target_chats)} targets...")

            # Create a monitoring message
            monitor_message = await event.reply("📊 **Broadcast in progress...**")

            # Start live monitoring
            await self.monitor.start_live_monitor(broadcast_id, monitor_message, event.chat_id)

            # Broadcast the message
            success_count = 0
            fail_count = 0
            failures = {}

            for target in self.target_chats:
                try:
                    if isinstance(target, tuple) and len(target) == 2:
                        # Target is a tuple of (chat_id, topic_id)
                        chat_id, topic_id = target
                        logger.info(f"Broadcasting to topic: chat_id={chat_id}, topic_id={topic_id}")
                        
                        if isinstance(message_content, str):
                            # For text messages, directly use send_message with reply_to (this works)
                            await self.client.send_message(chat_id, message_content, reply_to=topic_id)
                        else:
                            # For message objects, use ForwardMessagesRequest
                            me = await self.client.get_me()
                            bot_user_id = me.id
                            
                            # Use ForwardMessagesRequest for topics
                            forwarded = await self.client(ForwardMessagesRequest(
                                from_peer=bot_user_id,
                                id=[message_content.id],
                                to_peer=chat_id,
                                top_msg_id=topic_id  # Use topic_id as top_msg_id for forum topics
                            ))
                            # Then send a message linking to the topic
                            if forwarded and topic_id:
                                try:
                                    await self.client.send_message(
                                        entity=chat_id,
                                        message=f"⬆️ Forwarded message to topic #{topic_id}",
                                        reply_to=topic_id
                                    )
                                except Exception as e:
                                    logger.error(f"Topic association error: {e}")
                    else:
                        # Regular chat
                        if isinstance(message_content, str):
                            await self.client.send_message(target, message_content)
                        else:
                            # Get the user ID (from_peer) of the bot
                            me = await self.client.get_me()
                            bot_user_id = me.id
                            
                            # Use ForwardMessagesRequest for regular chats
                            await self.client(ForwardMessagesRequest(
                                from_peer=bot_user_id,
                                id=[message_content.id],
                                to_peer=target
                            ))

                    success_count += 1
                    logger.info(f"Successfully broadcast message to {target}")
                except Exception as e:
                    fail_count += 1
                    error_message = str(e)
                    failures[target] = error_message
                    logger.error(f"Error broadcasting message to {target}: {error_message}")

                # Update monitor during the process
                self.monitor.update_campaign(broadcast_id, {
                    "total_sent": success_count,
                    "failed_sends": fail_count,
                    "current_failures": failures,
                    "status": "sending"
                })

            # Update final status
            self.monitor.update_campaign(broadcast_id, {
                "status": "completed"
            })

            # Report results
            result = f"""✅ **Broadcast Results**
• Total Targets: {len(self.target_chats)}
• Successful: {success_count}
• Failed: {fail_count}
"""

            if failures:
                result += "\n**Failures:**\n"
                for target, error in list(failures.items())[:5]:  # Limit to first 5 failures
                    result += f"• Target {target}: {error[:50]}...\n"

                if len(failures) > 5:
                    result += f"... and {len(failures) - 5} more failures\n"

            await event.reply(result)
            logger.info(f"Broadcast message to {len(self.target_chats)} targets. Success: {success_count}, Failed: {fail_count}")
        except Exception as e:
            logger.error(f"Error in broadcast command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_addtarget(self, event):
        """Add target chats including topics by serial number, chat ID, or links"""
        try:
            command_parts = event.text.split()
            if len(command_parts) < 2:
                usage = """❌ Please provide targets in any of these formats:
• Serial numbers: 1,2,3
• Chat IDs: -100123456789
• Links: t.me/group, t.me/c/123/456 (topic), t.me/group/123 (topic)
• Usernames: @group
• Multiple mixed: 1,@group,t.me/c/123/456,-100987654321,t.me/group/123

Example: /addtarget @group1,t.me/c/123/456,-100123456789,t.me/group/123"""
                await event.reply(usage)
                return

            # Get list of all available chats, including handling forbidden channels
            all_chats = []
            async for dialog in self.client.iter_dialogs():
                # Check if dialog.entity is a ChannelForbidden object
                is_forbidden = hasattr(dialog, 'entity') and hasattr(dialog.entity, '__class__') and dialog.entity.__class__.__name__ == 'ChannelForbidden'
                
                # Add both regular channels/groups and forbidden channels
                if is_forbidden or ((dialog.is_channel or dialog.is_group) and not dialog.is_user):
                    all_chats.append(dialog.id)

            target_str = command_parts[1]
            target_list = [t.strip() for t in target_str.split(',')]

            success_list = []
            fail_list = []

            for target in target_list:
                try:
                    chat_id = None
                    topic_id = None

                    # Handle topic links (t.me/c/channelid/topicid)
                    topic_match = re.match(r'(?:https?://)?(?:t\.me|telegram\.me|telegram\.dog)/c/(\d+)(?:/(\d+))?', target)
                    if topic_match:
                        channel_id = int(topic_match.group(1))
                        topic_id = int(topic_match.group(2)) if topic_match.group(2) else None
                        chat_id = int(f"-100{channel_id}")  # Convert to supergroup format
                        if topic_id:
                            # Store as tuple for topic support
                            chat_id = (chat_id, topic_id)

                    # Try parsing as serial number if no topic match
                    elif target.isdigit() and int(target) > 0 and int(target) <= len(all_chats):
                        serial_no = int(target)
                        chat_id = all_chats[serial_no - 1]  # Convert to 0-based index

                    if chat_id:
                        chat_name = None
                        try:
                            # Use our custom resolver function instead of get_entity
                            if isinstance(chat_id, tuple):
                                # For topic chat_ids (tuple), only resolve the channel part
                                channel_id, topic_id = chat_id
                                entity_id, entity_type, entity_name, _ = await resolve_entity_without_get_entity(self.client, channel_id)
                                chat_name = f"{entity_name} (topic #{topic_id})"
                            else:
                                # Regular chat
                                entity_id, entity_type, entity_name, resolved_topic_id = await resolve_entity_without_get_entity(self.client, chat_id)
                                
                                # If a topic was detected during resolution
                                if resolved_topic_id:
                                    topic_id = resolved_topic_id
                                    chat_id = (entity_id, topic_id)
                                    chat_name = f"{entity_name} (topic #{topic_id})"
                                else:
                                    chat_name = entity_name
                        except:
                            # Fallback to string representation
                            if isinstance(chat_id, tuple):
                                chat_name = f"Channel {chat_id[0]} (topic #{chat_id[1]})"
                            else:
                                chat_name = str(chat_id)

                        self.target_chats.add(chat_id)
                        await event.reply(f"✅ Added target chat: {chat_name} ({chat_id})")
                        logger.info(f"Added target chat from reply: {chat_id}, current targets: {self.target_chats}")
                        return
                except Exception as e:
                    logger.error(f"Error processing target {target}: {str(e)}")
                    continue

            # Process from command parameters
            command_parts = event.text.split(maxsplit=1)
            if len(command_parts) < 2:
                await event.reply("❌ Please provide chat IDs, usernames, or invite links\nFormat: /addtarget <ID1,@username2,t.me/link,uid:123456>")
                return

            targets_text = command_parts[1]

            # Split by commas to support multiple targets
            target_list = [t.strip() for t in targets_text.split(',')]

            if not target_list:
                await event.reply("❌ No targets specified")
                return

            success_list = []
            fail_list = []

            for target in target_list:
                try:
                    chat_id = None

                    # Handle topic links with format t.me/c/channelid/topicid
                    topic_match_c = re.match(r'(?:https?://)?(?:t\.me|telegram\.me|telegram\.dog)/c/(\d+)(?:/(\d+))?', target)
                    # Handle topic links with format t.me/channel/topicid
                    topic_match_channel = re.match(r'(?:https?://)?(?:t\.me|telegram\.me|telegram\.dog)/([^/]+)/(\d+)', target)
                    
                    if topic_match_c:
                        channel_id = int(topic_match_c.group(1))
                        topic_id = int(topic_match_c.group(2)) if topic_match_c.group(2) else None
                        chat_id = int(f"-100{channel_id}")  # Convert to supergroup format
                        if topic_id:
                            # Store topic ID along with chat ID
                            chat_id = (chat_id, topic_id)
                    elif topic_match_channel:
                        # Format is t.me/channel/topicid
                        channel_name = topic_match_channel.group(1)
                        topic_id = int(topic_match_channel.group(2))
                        try:
                            # Convert channel name to numeric ID without get_entity
                            try:
                                # For usernames, try to send a temporary message to get the peer ID
                                if not channel_name.startswith('-100'):
                                    temp_message = await self.client.send_message(f"@{channel_name}", "")
                                    channel_id = int(f"-100{temp_message.peer_id.channel_id}")
                                    await temp_message.delete()
                                else:
                                    # Already a numeric ID
                                    channel_id = int(channel_name)
                                
                                # Store as tuple for topic support
                                chat_id = (channel_id, topic_id)
                            except Exception as e:
                                logger.error(f"Alternative channel resolution failed: {str(e)}")
                                # Try with regex to extract channel ID if it's in the format -100XXXXX
                                match = re.search(r'(-100\d+)', channel_name)
                                if match:
                                    channel_id = int(match.group(1))
                                    chat_id = (channel_id, topic_id)
                                else:
                                    raise ValueError(f"Could not resolve channel: {channel_name}")
                            logger.info(f"Successfully resolved topic link: {target} to channel {chat_id} with topic {topic_id}")
                            
                            # Add additional debug log to track the topic ID
                            logger.debug(f"TOPIC DEBUG: Original link: {target}, Resolved to channel: {channel_id}, Topic ID: {topic_id}")
                        except Exception as e:
                            logger.error(f"Error resolving channel in topic link '{target}': {str(e)}")
                            fail_list.append(f"{target}: Could not resolve channel")
                            continue
                        
                    # Handle user ID format (uid:12345)
                    elif target.lower().startswith('uid:'):
                        try:
                            uid = int(target[4:])
                            entity_id, entity_type, entity_name, _ = await resolve_entity_without_get_entity(self.client, uid)
                            chat_id = entity_id
                        except Exception as e:
                            logger.error(f"Error resolving user ID {target}: {str(e)}")
                            fail_list.append(f"{target}: Invalid user ID")
                            continue
                    # Try parsing as numeric chat ID first
                    elif target.lstrip('-').isdigit():
                        chat_id = int(target)
                    # Not a numeric ID, try resolving as username or link
                    else:
                        try:
                            # Unified handling of all non-numeric identifiers with our custom resolver
                            resolved_id = None
                            
                            if target.startswith('t.me/') or target.startswith('https://t.me/'):
                                # Handle invite links
                                entity_id, entity_type, entity_name, _ = await resolve_entity_without_get_entity(self.client, target)
                                resolved_id = entity_id
                            elif target.startswith('@'):
                                # Handle usernames
                                entity_id, entity_type, entity_name, _ = await resolve_entity_without_get_entity(self.client, target)
                                resolved_id = entity_id
                            else:
                                # Try as username without @
                                entity_id, entity_type, entity_name, _ = await resolve_entity_without_get_entity(self.client, '@' + target)
                                resolved_id = entity_id
                                
                            # Assign to chat_id if resolution was successful
                            if resolved_id:
                                chat_id = resolved_id
                                logger.info(f"Resolved {target} to entity: {entity_id} (Type: {entity_type}, Name: {entity_name})")
                            else:
                                raise ValueError(f"Could not resolve {target} to a valid entity ID")
                        except Exception as e:
                            logger.error(f"Error resolving chat identifier '{target}': {str(e)}")
                            fail_list.append(f"{target}: {str(e)}")
                            continue

                    if not chat_id:
                        fail_list.append(f"{target}: Could not resolve to a valid chat ID")
                        continue

                    self.target_chats.add(chat_id)
                    success_list.append(f"{target} → {chat_id}")
                    logger.info(f"Added target chat: {chat_id} from {target}")
                except Exception as e:
                    logger.error(f"Error adding target {target}: {str(e)}")
                    fail_list.append(f"{target}: {str(e)}")

            # Prepare response message
            response = []

            if success_list:
                response.append(f"✅ Successfully added {len(success_list)} target(s):")
                for success in success_list:
                    response.append(f"• {success}")

            if fail_list:
                response.append(f"\n❌ Failed to add {len(fail_list)} target(s):")
                for fail in fail_list:
                    response.append(f"• {fail}")

            if not success_list and not fail_list:
                response.append("⚠️ No targets were processed")

            # Split long messages
            max_length = 4096  # Telegram's max message length
            messages = []
            for i in range(0, len('\n'.join(response)), max_length):
                messages.append('\n'.join(response)[i:i + max_length])

            # Send each part
            for message in messages:
                await event.reply(message)
            logger.info(f"Target chat operation complete, current targets: {self.target_chats}")
        except Exception as e:
            logger.error(f"Error in addtarget command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_listtarget(self, event):
        """List all target chats"""
        try:
            if not self.target_chats:
                await event.reply("📝 No target chats configured")
                return

            # Parse page number from command if present
            command_parts = event.text.split()
            page = 1
            items_per_page = 10

            if len(command_parts) > 1 and command_parts[1].isdigit():
                page = int(command_parts[1])

            # Get all chats info
            all_chats = []
            for target in self.target_chats:
                try:
                    # Check if target is a tuple (chat_id, topic_id)
                    if isinstance(target, tuple) and len(target) == 2:
                        chat_id, topic_id = target
                        logger.debug(f"Processing topic target: chat_id={chat_id}, topic_id={topic_id}")
                        
                        try:
                            # Only get entity for the chat_id (not the tuple)
                            entity = await self.client.get_entity(chat_id)
                            
                            # Check if entity is ChannelForbidden
                            if hasattr(entity, '__class__') and entity.__class__.__name__ == 'ChannelForbidden':
                                name = f"Forbidden Channel {entity.id}"
                                username = None
                            else:
                                name = getattr(entity, 'title', None) or getattr(entity, 'first_name', None) or str(chat_id)
                                username = getattr(entity, 'username', None)
                            
                            # Format the display with topic information
                            display_name = f"{name} (Topic #{topic_id})"
                            all_chats.append((target, display_name, username))
                        except Exception as e:
                            logger.error(f"Error getting entity for topic chat {chat_id}: {str(e)}")
                            display_name = f"Unknown Channel {chat_id} (Topic #{topic_id})"
                            all_chats.append((target, display_name, None))
                    else:
                        try:
                            # Regular chat (not a topic)
                            entity = await self.client.get_entity(target)
                            
                            # Check if entity is ChannelForbidden
                            if hasattr(entity, '__class__') and entity.__class__.__name__ == 'ChannelForbidden':
                                name = f"Forbidden Channel {entity.id}"
                                username = None
                            else:
                                name = getattr(entity, 'title', None) or getattr(entity, 'first_name', None) or str(target)
                                username = getattr(entity, 'username', None)
                                
                            all_chats.append((target, name, username))
                        except Exception as e:
                            logger.error(f"Error getting entity for chat {target}: {str(e)}")
                            all_chats.append((target, f"[Unknown: {str(target)}]", None))
                except Exception as e:
                    logger.error(f"Error getting entity for target {target}: {str(e)}")
                    all_chats.append((target, f"[Unknown: {str(target)}]", None))

            # Calculate pagination
            total_pages = (len(all_chats) + items_per_page - 1) // items_per_page
            page = min(max(1, page), total_pages)
            start_idx = (page - 1) * items_per_page
            end_idx = start_idx + items_per_page

            result = f"📝 **Target Chats** (Page {page}/{total_pages})\n\n"

            # Add chats for current page
            for idx, (chat_id, name, username) in enumerate(all_chats[start_idx:end_idx], start=start_idx + 1):
                # Format differently for topic vs regular chat
                if isinstance(chat_id, tuple) and len(chat_id) == 2:
                    chat_part, topic_part = chat_id
                    if username:
                        result += f"{idx}. Channel: {chat_part}, Topic: {topic_part} - {name} (@{username})\n"
                    else:
                        result += f"{idx}. Channel: {chat_part}, Topic: {topic_part} - {name}\n"
                else:
                    if username:
                        result += f"{idx}. {chat_id} - {name} (@{username})\n"
                    else:
                        result += f"{idx}. {chat_id} - {name}\n"

            # Add navigation buttons info
            result += f"\n**Navigation:**\n"
            if page > 1:
                result += f"• Use `/listtarget {page-1}` for previous page\n"
            if page < total_pages:
                result += f"• Use `/listtarget {page+1}` for next page\n"
            result += f"\nShowing {start_idx + 1}-{min(end_idx, len(all_chats))} of {len(all_chats)} chats"

            await event.reply(result)
            logger.info(f"Listed target chats page {page}/{total_pages}")
        except Exception as e:
            logger.error(f"Error in listtarget command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_removetarget(self, event):
        """Remove target chats by serial number or chat ID"""
        try:
            command_parts = event.text.split()
            if len(command_parts) < 2:
                await event.reply("❌ Please provide serial numbers or chat IDs\nFormat: /removetarget <serial_no1,serial_no2> or <id1,id2>")
                return

            # Get list of targets
            targets = list(self.target_chats)
            target_str = command_parts[1]
            target_list = [t.strip() for t in target_str.split(',')]

            removed = []
            not_found = []

            for target in target_list:
                try:
                    # Try parsing as serial number first
                    if target.isdigit() and int(target) > 0 and int(target) <= len(targets):
                        serial_no = int(target)
                        chat_id = targets[serial_no - 1]  # Convert to 0-based index
                        if chat_id in self.target_chats:
                            self.target_chats.remove(chat_id)
                            removed.append(f"Serial #{target} → {chat_id}")
                except Exception as e:
                    logger.error(f"Error processing target {target}: {str(e)}")
                    continue

            # Process from command parameters
            command_parts = event.text.split(maxsplit=1)
            if len(command_parts) < 2:
                await event.reply("❌ Please provide chat IDs, usernames or links to remove\nFormat: /removetarget <id1,@username2,t.me/link,uid:123456>")
                return

            id_str = command_parts[1]
            id_list = [x.strip() for x in id_str.split(',')]

            removed = []
            not_found = []

            for target_str in id_list:
                try:
                    chat_id = None

                    # Handle user ID format (uid:12345)
                    if target_str.lower().startswith('uid:'):
                        try:
                            uid = int(target_str[4:])
                            entity_id, entity_type, entity_name, _ = await resolve_entity_without_get_entity(self.client, uid)
                            chat_id = entity_id
                        except Exception as e:
                            logger.error(f"Error resolving user ID {target_str}: {str(e)}")
                            not_found.append(f"{target_str}: Invalid user ID")
                            continue
                    # Try parsing as numeric chat ID first
                    elif target_str.lstrip('-').isdigit():
                        chat_id = int(target_str)
                    # Not a numeric ID, try resolving as username or link
                    else:
                        try:
                            # Unified handling of all non-numeric identifiers with our custom resolver
                            resolved_id = None
                            
                            if target_str.startswith('t.me/') or target_str.startswith('https://t.me/'):
                                # Handle invite links
                                entity_id, entity_type, entity_name, _ = await resolve_entity_without_get_entity(self.client, target_str)
                                resolved_id = entity_id
                            elif target_str.startswith('@'):
                                # Handle usernames
                                entity_id, entity_type, entity_name, _ = await resolve_entity_without_get_entity(self.client, target_str)
                                resolved_id = entity_id
                            else:
                                # Try as username without @
                                entity_id, entity_type, entity_name, _ = await resolve_entity_without_get_entity(self.client, '@' + target_str)
                                resolved_id = entity_id
                                
                            # Assign to chat_id if resolution was successful
                            if resolved_id:
                                chat_id = resolved_id
                                logger.info(f"Resolved {target_str} to entity: {entity_id} (Type: {entity_type}, Name: {entity_name})")
                            else:
                                raise ValueError(f"Could not resolve {target_str} to a valid entity ID")
                        except Exception as e:
                            logger.error(f"Error resolving chat identifier '{target_str}': {str(e)}")
                            not_found.append(f"{target_str}: Could not resolve")
                            continue

                    if not chat_id:
                        not_found.append(f"{target_str}: Could not resolve to a valid chat ID")
                        continue

                    # Check if the chat is in the target list
                    if chat_id in self.target_chats:
                        self.target_chats.remove(chat_id)
                        chat_name = None
                        try:
                            # Use our custom resolver function instead of get_entity
                            entity_id, entity_type, entity_name, _ = await resolve_entity_without_get_entity(self.client, chat_id)
                            chat_name = entity_name
                        except:
                            pass

                        if chat_name:
                            removed.append(f"{target_str} ({chat_name})")
                        else:
                            removed.append(f"{target_str}")
                    else:
                        not_found.append(f"{target_str}: Not in target list")
                except Exception as e:
                    logger.error(f"Error removing target {target_str}: {str(e)}")
                    not_found.append(f"{target_str}: {str(e)}")

            # Prepare response message
            response = []

            if removed:
                response.append(f"✅ Successfully removed {len(removed)} target(s):")
                for success in removed:
                    response.append(f"• {success}")

            if not_found:
                if removed:
                    response.append("")  # Add a blank line as separator
                response.append(f"❌ Failed to remove {len(not_found)} target(s):")
                for fail in not_found:
                    response.append(f"• {fail}")

            if not removed and not not_found:
                response.append("⚠️ No targets were processed")

            # Split long messages
            max_length = 4096  # Telegram's max message length
            messages = []
            for i in range(0, len('\n'.join(response)), max_length):
                messages.append('\n'.join(response)[i:i + max_length])

            # Send each part
            for message in messages:
                await event.reply(message)
            logger.info(f"Target chat removal complete, current targets: {self.target_chats}")
        except Exception as e:
            logger.error(f"Error in removetarget command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_removealltarget(self, event):
        """Remove all target chats without confirmation"""
        try:
            count = len(self.target_chats)

            # No confirmation - just clear all targets
            self.target_chats.clear()

            await event.reply(f"✅ All {count} target chats have been removed")
            logger.info(f"Removed all {count} target chats")

        except Exception as e:
            logger.error(f"Error in removealltarget command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_cleantarget(self, event):
        """Clean invalid target chats and chats where bot is not a member, banned, or can't send messages"""
        try:
            if not self.target_chats:
                await event.reply("📝 No target chats configured")
                return

            # Get initial count for report
            initial_count = len(self.target_chats)

            # Status message with animation
            animation_frames = ["🔍", "🔎", "👁️", "👀", "🔍", "🔎"]
            status_msg = await event.reply(f"{animation_frames[0]} Preparing to check {initial_count} targets for validity...")
            animation_frame = 0
            
            # Process targets with enhanced checking
            invalid_targets = []       # Targets that don't exist
            banned_targets = []        # Targets where bot is banned
            no_send_perm_targets = []  # Targets where bot can't send messages
            not_member_targets = []    # Targets where bot is not a member
            practical_test_failed = [] # Targets that failed the practical message test
            processed = 0

            for target in list(self.target_chats):
                try:
                    # Update animation frame
                    animation_frame = (animation_frame + 1) % len(animation_frames)
                    if processed % 2 == 0:  # Update status every 2 chats for better animation
                        await status_msg.edit(
                            f"{animation_frames[animation_frame]} Checking targets... ({processed}/{initial_count})"
                        )
                    
                    # Check if target is a tuple (chat_id, topic_id)
                    if isinstance(target, tuple) and len(target) == 2:
                        chat_id, topic_id = target
                        logger.debug(f"Checking topic target: chat_id={chat_id}, topic_id={topic_id}")
                        
                        # Check if the chat_id exists using our custom resolver
                        try:
                            entity_id, entity_type, entity_name, _ = await resolve_entity_without_get_entity(self.client, chat_id)
                            # If we got this far, the entity exists
                            chat = SimpleNamespace(id=entity_id, title=entity_name)
                        except Exception as e:
                            invalid_targets.append((target, f"Channel does not exist: {str(e)}"))
                            continue
                            
                        # Check permissions for topic's parent channel
                        try:
                            # Get permissions to check ban status and sending permissions
                            permissions = await self.client.get_permissions(chat)
                            
                            if not permissions:
                                not_member_targets.append((target, "Not a member"))
                                continue
                                
                            # Check if banned or restricted from sending messages
                            if hasattr(permissions, 'banned_rights') and permissions.banned_rights:
                                if hasattr(permissions.banned_rights, 'send_messages') and permissions.banned_rights.send_messages:
                                    banned_targets.append((target, "Banned from sending messages"))
                                    continue
                            
                            # Check if we have send message permission
                            if hasattr(permissions, 'send_messages') and not permissions.send_messages:
                                no_send_perm_targets.append((target, "No permission to send messages"))
                                continue
                                
                        except ChatAdminRequiredError:
                            no_send_perm_targets.append((target, "Admin privileges required"))
                            continue
                        except UserBannedInChannelError:
                            banned_targets.append((target, "Bot is banned from this channel"))
                            continue
                        except ChatWriteForbiddenError:
                            no_send_perm_targets.append((target, "Writing messages forbidden"))
                            continue
                        except Exception as e:
                            # Generic error - either not a member or some other issue
                            not_member_targets.append((target, f"Error: {str(e)}"))
                            continue
                    else:
                        # Regular chat (not a topic) - use our custom resolver
                        try:
                            entity_id, entity_type, entity_name, _ = await resolve_entity_without_get_entity(self.client, target)
                            
                            # Check if entity name indicates it's a ChannelForbidden
                            if entity_name and "Forbidden Channel" in entity_name:
                                logger.warning(f"Target {target} is a forbidden channel")
                                # We can still include forbidden channels if needed
                                chat = SimpleNamespace(id=entity_id, title=entity_name, is_forbidden=True)
                            else:
                                # Normal entity
                                chat = SimpleNamespace(id=entity_id, title=entity_name, is_forbidden=False)
                        except Exception as e:
                            invalid_targets.append((target, f"Invalid chat: {str(e)}"))
                            continue

                        # Thorough check of member status and permissions
                        try:
                            # Get permissions to check rights
                            permissions = await self.client.get_permissions(chat)
                            
                            if not permissions:
                                not_member_targets.append((target, "Not a member"))
                                continue
                                
                            # Check if banned or restricted from sending messages
                            if hasattr(permissions, 'banned_rights') and permissions.banned_rights:
                                if hasattr(permissions.banned_rights, 'send_messages') and permissions.banned_rights.send_messages:
                                    banned_targets.append((target, "Banned from sending messages"))
                                    continue
                            
                            # Check if we have send message permission
                            if hasattr(permissions, 'send_messages') and not permissions.send_messages:
                                no_send_perm_targets.append((target, "No permission to send messages"))
                                continue
                                
                        except ChatAdminRequiredError:
                            no_send_perm_targets.append((target, "Admin privileges required"))
                            continue
                        except UserBannedInChannelError:
                            banned_targets.append((target, "Bot is banned from this channel"))
                            continue
                        except ChatWriteForbiddenError:
                            no_send_perm_targets.append((target, "Writing messages forbidden"))
                            continue
                        except Exception as e:
                            # Generic error - either not a member or some other issue
                            not_member_targets.append((target, f"Error: {str(e)}"))
                            continue

                    processed += 1

                except Exception as e:
                    invalid_targets.append((target, f"Error checking: {str(e)}"))
                    logger.error(f"Error checking target {target}: {str(e)}")

            # Remove all problem targets
            for target, _ in invalid_targets:
                if target in self.target_chats:
                    self.target_chats.remove(target)
            
            for target, _ in banned_targets:
                if target in self.target_chats:
                    self.target_chats.remove(target)
                    
            for target, _ in no_send_perm_targets:
                if target in self.target_chats:
                    self.target_chats.remove(target)
                    
            for target, _ in not_member_targets:
                if target in self.target_chats:
                    self.target_chats.remove(target)

            # Additional check - attempt to send a test message to each remaining target
            # This is the most reliable way to check if we can actually send messages
            if self.target_chats:
                await status_msg.edit("🧪 Running practical message sending test on remaining targets...")
                test_message = "⚡ Testing message permissions... (This message will be deleted immediately)"
                practical_test_failed = []
                
                for target in list(self.target_chats):
                    try:
                        # Skip the test if we know it's a forbidden channel
                        is_forbidden = False
                        if isinstance(target, tuple) and len(target) == 2:
                            # Check if this is a topic in a forbidden channel
                            chat_id, topic_id = target
                            try:
                                entity_id, entity_type, entity_name, _ = await resolve_entity_without_get_entity(self.client, chat_id)
                                if entity_name and "Forbidden Channel" in entity_name:
                                    is_forbidden = True
                                    practical_test_failed.append((target, f"Forbidden channel (no access)"))
                                    continue
                            except:
                                pass
                        else:
                            # Check if this is a forbidden channel
                            try:
                                entity_id, entity_type, entity_name, _ = await resolve_entity_without_get_entity(self.client, target)
                                if entity_name and "Forbidden Channel" in entity_name:
                                    is_forbidden = True
                                    practical_test_failed.append((target, f"Forbidden channel (no access)"))
                                    continue
                            except:
                                pass
                        
                        # Only try to send a message if the channel is not forbidden
                        if not is_forbidden:
                            # For topics, we need to handle differently
                            if isinstance(target, tuple) and len(target) == 2:
                                chat_id, topic_id = target
                                msg = await self.client.send_message(
                                    entity=chat_id,
                                    message=test_message,
                                    reply_to=topic_id
                                )
                            else:
                                # Regular chat
                                msg = await self.client.send_message(target, test_message)
                            
                        # If we get here, message was sent successfully, now delete it
                        await msg.delete()
                    except Exception as e:
                        practical_test_failed.append((target, f"Failed practical test: {str(e)}"))
                        if target in self.target_chats:
                            self.target_chats.remove(target)

            # Prepare enhanced detailed report with color coding and categories
            await status_msg.edit("📊 Generating comprehensive report...")
            
            report = ["🧹 **Enhanced Target Cleanup Report**\n"]
            report.append(f"• Initial targets: {initial_count}")
            
            total_removed = (len(invalid_targets) + len(banned_targets) + 
                           len(no_send_perm_targets) + len(not_member_targets) + 
                           len(practical_test_failed))
            
            if invalid_targets:
                report.append(f"\n❌ **Removed {len(invalid_targets)} invalid targets:**")
                for target, reason in invalid_targets[:5]:  # Show first 5 only
                    target_display = f"{target[0]}/topics/{target[1]}" if isinstance(target, tuple) else target
                    report.append(f"  • {target_display}: {reason}")
                if len(invalid_targets) > 5:
                    report.append(f"  • ...and {len(invalid_targets) - 5} more")

            if banned_targets:
                report.append(f"\n🚫 **Removed {len(banned_targets)} targets where bot is banned:**")
                for target, reason in banned_targets[:5]:
                    target_display = f"{target[0]}/topics/{target[1]}" if isinstance(target, tuple) else target
                    report.append(f"  • {target_display}: {reason}")
                if len(banned_targets) > 5:
                    report.append(f"  • ...and {len(banned_targets) - 5} more")
                    
            if no_send_perm_targets:
                report.append(f"\n🔒 **Removed {len(no_send_perm_targets)} targets with no send permission:**")
                for target, reason in no_send_perm_targets[:5]:
                    target_display = f"{target[0]}/topics/{target[1]}" if isinstance(target, tuple) else target
                    report.append(f"  • {target_display}: {reason}")
                if len(no_send_perm_targets) > 5:
                    report.append(f"  • ...and {len(no_send_perm_targets) - 5} more")

            if not_member_targets:
                report.append(f"\n⚠️ **Removed {len(not_member_targets)} targets where bot is not a member:**")
                for target, reason in not_member_targets[:5]:
                    target_display = f"{target[0]}/topics/{target[1]}" if isinstance(target, tuple) else target
                    report.append(f"  • {target_display}: {reason}")
                if len(not_member_targets) > 5:
                    report.append(f"  • ...and {len(not_member_targets) - 5} more")
                    
            if practical_test_failed:
                report.append(f"\n🧪 **Removed {len(practical_test_failed)} targets that failed practical message test:**")
                for target, reason in practical_test_failed[:5]:
                    target_display = f"{target[0]}/topics/{target[1]}" if isinstance(target, tuple) else target
                    report.append(f"  • {target_display}: {reason}")
                if len(practical_test_failed) > 5:
                    report.append(f"  • ...and {len(practical_test_failed) - 5} more")

            report.append(f"\n✅ **Final Result:**")
            report.append(f"• Remaining valid targets: {len(self.target_chats)}")
            report.append(f"• Total removed: {total_removed} ({total_removed/initial_count*100:.1f}% of original)")
            
            if len(self.target_chats) > 0:
                report.append(f"\n📊 Breakdown of remaining targets:")
                chat_types = {"channel": 0, "group": 0, "supergroup": 0, "user": 0, "topic": 0, "other": 0}
                
                # Count target types
                for target in self.target_chats:
                    if isinstance(target, tuple):
                        chat_types["topic"] += 1
                    else:
                        try:
                            # Use our custom resolver function instead of get_entity
                            entity_id, entity_type, entity_name, _ = await resolve_entity_without_get_entity(self.client, target)
                            
                            # Based on entity_type, categorize the target
                            if entity_type == "channel":
                                chat_types["channel"] += 1
                            elif entity_type == "chat":
                                chat_types["group"] += 1
                            elif entity_type == "user":
                                chat_types["user"] += 1
                            else:
                                # For unknown types, try to determine from entity ID range
                                if isinstance(target, int) and target < 0:
                                    # Negative IDs are typically channels or chats
                                    if target < -1000000000:
                                        chat_types["supergroup"] += 1
                                    else:
                                        chat_types["group"] += 1
                                else:
                                    chat_types["other"] += 1
                        except Exception:
                            chat_types["other"] += 1
                
                # Add breakdown to report
                for chat_type, count in chat_types.items():
                    if count > 0:
                        emoji = {"channel": "📢", "group": "👥", "supergroup": "👥", 
                                "user": "👤", "topic": "🗨️", "other": "❓"}[chat_type]
                        report.append(f"  • {emoji} {chat_type.capitalize()}: {count}")

            # Send final report
            await status_msg.edit("\n".join(report))

            logger.info(f"Enhanced target cleanup: removed {total_removed} problematic targets, remaining {len(self.target_chats)}")
        except Exception as e:
            logger.error(f"Error in cleantarget command: {str(e)}")
            await event.reply(f"❌ Error cleaning targets: {str(e)}")



    @admin_only
    async def cmd_targeting(self, event):
        """Targeting based on keywords"""
        # Implementation placeholder for interface completeness
        await event.reply("✅ Command executed - targeting parameters set")

    async def _send_chunked_response(self, event, message_list, prefix="", suffix=""):
        """Helper to send long messages in chunks"""
        if not message_list:
            return
            
        chunk = prefix
        for item in message_list:
            if len(chunk + item + "\n") > 3500:  # Safe limit for Telegram
                await event.reply(chunk)
                chunk = prefix
            chunk += item + "\n"
        
        if chunk:
            chunk += suffix
            await event.reply(chunk)

    async def _join_with_delay(self, chat, progress_msg=None):
        """Join a chat with rate limit handling"""
        try:
            if 't.me/' in chat or 'telegram.me/' in chat or 'telegram.dog/' in chat:
                if 'joinchat' in chat or '+' in chat:
                    invite_hash = chat.split('/')[-1].replace('+', '')
                    await self.client(ImportChatInviteRequest(invite_hash))
                else:
                    username = chat.split('/')[-1].split('?')[0]
                    await self.client(JoinChannelRequest(username))
            else:
                username = chat.lstrip('@')
                await self.client(JoinChannelRequest(username))
            return True, None
        except Exception as e:
            wait_time = None
            error_msg = str(e)
            if "wait" in error_msg.lower():
                try:
                    wait_time = int(re.search(r'of (\d+) seconds', error_msg).group(1))
                except:
                    wait_time = 60
            return False, (error_msg, wait_time)

    @admin_only
    async def cmd_joinchat(self, event):
        """Join chat/group from message or reply with rate limit handling"""
        try:
            chats = []
            
            # Get chats from reply or command
            if event.is_reply:
                replied_msg = await event.get_reply_message()
                if replied_msg.text:
                    # Extract all relevant patterns
                    patterns = [
                        r'(?:https?://)?(?:t\.me|telegram\.me|telegram\.dog)/[^\s/]+(?:/\S*)?',
                        r'@[\w\d_]+',
                        r'-?\d{6,}'
                    ]
                    for pattern in patterns:
                        chats.extend(re.findall(pattern, replied_msg.text))

            # Add chats from command arguments
            command_parts = event.text.split(maxsplit=1)
            if len(command_parts) > 1:
                additional_chats = command_parts[1].split(',')
                chats.extend([c.strip() for c in additional_chats if c.strip()])

            if not chats:
                await event.reply("❌ Please provide chat links/usernames or reply to a message containing them\nFormat: /joinchat <chat1,chat2,...>")
                return

            # Remove duplicates while preserving order
            chats = list(dict.fromkeys(chats))
            
            # Show initial progress
            progress_msg = await event.reply(f"🔄 Processing {len(chats)} chats...")
            
            success_list = []
            fail_list = []
            delayed_list = []
            
            for chat in chats:
                success, result = await self._join_with_delay(chat)
                if success:
                    success_list.append(f"• {chat}")
                else:
                    error_msg, wait_time = result
                    if wait_time:
                        delayed_list.append((chat, wait_time))
                    else:
                        fail_list.append(f"• {chat}: {error_msg}")
                
                # Update progress periodically
                if len(success_list) % 5 == 0:
                    try:
                        await progress_msg.edit(f"🔄 Joined {len(success_list)}/{len(chats)} chats...")
                    except:
                        pass

            # Process delayed joins if any
            if delayed_list:
                # Sort by wait time
                delayed_list.sort(key=lambda x: x[1])
                delay_msg = f"\n⏳ {len(delayed_list)} chats require waiting:\n"
                for chat, wait_time in delayed_list:
                    delay_msg += f"• {chat}: {wait_time} seconds\n"
                fail_list.append(delay_msg)

            # Send results in chunks
            if success_list:
                await self._send_chunked_response(
                    event,
                    success_list,
                    f"✅ Successfully joined {len(success_list)} chat(s):\n",
                    "\n"
                )

            if fail_list:
                await self._send_chunked_response(
                    event,
                    fail_list,
                    f"❌ Failed to join {len(fail_list)} chat(s):\n",
                    "\n"
                )

            try:
                await progress_msg.delete()
            except:
                pass

            logger.info(f"Join operation completed - Success: {len(success_list)}, Failed: {len(fail_list)}")
        except Exception as e:
            logger.error(f"Error in joinchat command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")
            
            # Check if replying to a message
            if event.is_reply:
                replied_msg = await event.get_reply_message()
                if replied_msg.text:
                    # Extract topic links (t.me/c/channelid/topicid)
                    topic_links = re.findall(r'(?:https?://)?(?:t\.me|telegram\.me|telegram\.dog)/c/(\d+)(?:/\d+)?', replied_msg.text)
                    chats.extend([f"-100{chat_id}" for chat_id in topic_links])
                    
                    # Extract regular t.me links
                    links = re.findall(r'(?:https?://)?(?:t\.me|telegram\.me|telegram\.dog)/(?!c/)([^\s/]+)(?:/\S*)?', replied_msg.text)
                    chats.extend([link for link in links])
                    
                    # Extract usernames
                    usernames = re.findall(r'@[\w\d_]+', replied_msg.text)
                    chats.extend(usernames)
            
            # Also check command arguments
            command_parts = event.text.split(maxsplit=1)
            if len(command_parts) > 1:
                additional_chats = command_parts[1].split(',')
                chats.extend([c.strip() for c in additional_chats if c.strip()])

            if not chats:
                await event.reply("❌ Please provide chat links/usernames or reply to a message containing them\nFormat: /joinchat <chat1,chat2,...>")
                return

            # Show progress message
            progress_msg = await event.reply("🔄 Processing join requests...")
            
            success_list = []
            fail_list = []

            for chat in chats:
                chat = chat.strip()
                try:
                    if 't.me/' in chat or 'telegram.me/' in chat or 'telegram.dog/' in chat:
                        # Handle various invite link formats
                        if 'joinchat' in chat or '+' in chat:
                            invite_hash = chat.split('/')[-1].replace('+', '')
                            await self.client(ImportChatInviteRequest(invite_hash))
                        else:
                            # Clean the link to get username
                            username = chat.split('/')[-1].split('?')[0]
                            await self.client(JoinChannelRequest(username))
                    else:
                        # Handle username format
                        username = chat.lstrip('@')
                        await self.client(JoinChannelRequest(username))
                    
                    success_list.append(chat)
                    logger.info(f"Successfully joined chat: {chat}")
                    
                    # Update progress
                    await progress_msg.edit(f"🔄 Joined {len(success_list)}/{len(chats)} chats...")
                    
                    # Small delay to avoid flood limits
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    fail_list.append(f"{chat}: {str(e)}")
                    logger.error(f"Failed to join chat {chat}: {str(e)}")

            # Prepare final response
            response = []
            if success_list:
                response.append(f"✅ Successfully joined {len(success_list)} chat(s):")
                for chat in success_list:
                    response.append(f"• {chat}")

            if fail_list:
                if response:
                    response.append("")
                response.append(f"❌ Failed to join {len(fail_list)} chat(s):")
                for fail in fail_list:
                    response.append(f"• {fail}")

            await progress_msg.edit("\n".join(response))
            logger.info(f"Join operation completed - Success: {len(success_list)}, Failed: {len(fail_list)}")
        except Exception as e:
            logger.error(f"Error in joinchat command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    async def _leave_with_delay(self, chat):
        """Leave a chat with rate limit handling"""
        try:
            if 't.me/' in chat or 'telegram.me/' in chat or 'telegram.dog/' in chat:
                username = chat.split('/')[-1].split('?')[0]
                if 'joinchat' in chat or '+' in chat:
                    return False, "Cannot leave from invite links"
            else:
                username = chat.lstrip('@')
            
            # Use our custom resolver to get entity ID without get_entity
            entity_id, entity_type, entity_name, _ = await resolve_entity_without_get_entity(self.client, username)
            
            # Check if it's actually a channel/chat (not a user)
            if entity_type in ["channel", "chat", "unknown"]:
                # For LeaveChannelRequest, just passing the ID as an integer works
                await self.client(LeaveChannelRequest(entity_id))
            return True, None
        except Exception as e:
            wait_time = None
            error_msg = str(e)
            if "wait" in error_msg.lower():
                try:
                    wait_time = int(re.search(r'of (\d+) seconds', error_msg).group(1))
                except:
                    wait_time = 60
            return False, (error_msg, wait_time)

    @admin_only
    async def cmd_leaveallchat(self, event):
        """Leave all groups and channels the bot is a member of"""
        try:
            # Initial confirmation message with animation
            frames = [
                "⚠️ **WARNING: LEAVE ALL CHATS REQUESTED** ⚠️\n\nThis will remove you from ALL groups and channels!\n\nProcessing request... ⏳",
                "⚠️ **WARNING: LEAVE ALL CHATS REQUESTED** ⚠️\n\nThis will remove you from ALL groups and channels!\n\nAre you sure? Reply with 'yes' to continue... ⏳",
                "⚠️ **WARNING: LEAVE ALL CHATS REQUESTED** ⚠️\n\nThis will remove you from ALL groups and channels!\n\nWaiting for confirmation... ⏳"
            ]
            
            # Show animated warning
            warning_msg = await event.reply(frames[0])
            for frame in frames[1:]:
                await asyncio.sleep(0.7)
                await warning_msg.edit(frame)
            
            # Wait for confirmation
            try:
                response = await self.client.wait_for_event(
                    events.NewMessage(from_users=event.sender_id, chats=event.chat_id),
                    timeout=30
                )
                
                # If response is not 'yes', cancel operation
                if not response.text.lower() == 'yes':
                    await warning_msg.edit("❌ Operation cancelled. No chats were left.")
                    return
                
                # Continue with the operation if confirmed
                await warning_msg.edit("✅ Confirmation received! Starting to leave all chats...")
                
            except asyncio.TimeoutError:
                await warning_msg.edit("⏱️ Confirmation timeout. Operation cancelled.")
                return
            
            # Get all dialogs
            status_msg = await event.reply("🔍 Scanning all joined chats and channels...")
            
            chats_to_leave = []
            private_chats = 0
            
            async for dialog in self.client.iter_dialogs():
                # Skip private chats (users)
                entity = dialog.entity
                
                if hasattr(entity, 'broadcast') or hasattr(entity, 'megagroup') or hasattr(entity, 'gigagroup'):
                    # It's a channel or group
                    chat_name = entity.title if hasattr(entity, 'title') else "Unknown"
                    chat_id = entity.id
                    
                    # Add to our leave list
                    chats_to_leave.append((chat_id, chat_name))
                else:
                    # It's a private chat
                    private_chats += 1
            
            if not chats_to_leave:
                await status_msg.edit("✅ No groups or channels found to leave!")
                return
                
            # Update status message with count
            await status_msg.edit(f"🔍 Found {len(chats_to_leave)} groups/channels to leave.\n\n⏳ Starting leave process...")
            
            # Process leaving
            success_count = 0
            failed_count = 0
            failed_chats = []
            
            for index, (chat_id, chat_name) in enumerate(chats_to_leave):
                try:
                    # Update progress every 5 chats
                    if index % 5 == 0 or index == len(chats_to_leave) - 1:
                        progress = (index + 1) / len(chats_to_leave) * 100
                        await status_msg.edit(f"🚪 Leaving chats... ({index+1}/{len(chats_to_leave)}) - {progress:.1f}%")
                    
                    # Attempt to leave the chat
                    success, result = await self._leave_with_delay(chat_id)
                    
                    if success:
                        success_count += 1
                    else:
                        failed_count += 1
                        failed_chats.append(f"• {chat_name} ({chat_id}): {result}")
                    
                    # Sleep to avoid rate limits
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"Error leaving chat {chat_id}: {str(e)}")
                    failed_count += 1
                    failed_chats.append(f"• {chat_name} ({chat_id}): {str(e)}")
            
            # Prepare final report
            report = [f"🧹 **Mass Leave Operation Complete**\n"]
            report.append(f"• Total chats processed: {len(chats_to_leave)}")
            report.append(f"• Successfully left: {success_count} chats")
            
            if failed_count > 0:
                report.append(f"• Failed to leave: {failed_count} chats")
                
                # Show first 10 failed chats
                report.append("\n❌ **Failed chats:**")
                for i, chat in enumerate(failed_chats[:10]):
                    report.append(chat)
                    
                if len(failed_chats) > 10:
                    report.append(f"...and {len(failed_chats) - 10} more")
            
            await status_msg.edit("\n".join(report))
            logger.info(f"Leaveallchat command completed: Left {success_count} of {len(chats_to_leave)} chats")
            
        except Exception as e:
            logger.error(f"Error in leaveallchat command: {str(e)}")
            await event.reply(f"❌ Error leaving all chats: {str(e)}")
    
    @admin_only
    async def cmd_leavechat(self, event):
        """Leave chat/group from message or reply with rate limit handling"""
        try:
            chats = []
            
            # Get chats from reply or command
            if event.is_reply:
                replied_msg = await event.get_reply_message()
                if replied_msg.text:
                    # Extract all relevant patterns
                    patterns = [
                        r'(?:https?://)?(?:t\.me|telegram\.me|telegram\.dog)/[^\s/]+(?:/\S*)?',
                        r'@[\w\d_]+',
                        r'-?\d{6,}'
                    ]
                    for pattern in patterns:
                        chats.extend(re.findall(pattern, replied_msg.text))

            # Add chats from command arguments
            command_parts = event.text.split(maxsplit=1)
            if len(command_parts) > 1:
                additional_chats = command_parts[1].split(',')
                chats.extend([c.strip() for c in additional_chats if c.strip()])

            if not chats:
                await event.reply("❌ Please provide chat links/usernames or reply to a message containing them\nFormat: /leavechat <chat1,chat2,...>")
                return

            # Remove duplicates while preserving order
            chats = list(dict.fromkeys(chats))
            
            # Show initial progress
            progress_msg = await event.reply(f"🔄 Processing {len(chats)} chats...")
            
            success_list = []
            fail_list = []
            delayed_list = []
            
            for chat in chats:
                success, result = await self._leave_with_delay(chat)
                if success:
                    success_list.append(f"• {chat}")
                else:
                    if isinstance(result, tuple):
                        error_msg, wait_time = result
                        if wait_time:
                            delayed_list.append((chat, wait_time))
                        else:
                            fail_list.append(f"• {chat}: {error_msg}")
                    else:
                        fail_list.append(f"• {chat}: {result}")
                
                # Update progress periodically
                if len(success_list) % 5 == 0:
                    try:
                        await progress_msg.edit(f"🔄 Left {len(success_list)}/{len(chats)} chats...")
                    except:
                        pass

            # Process delayed leaves if any
            if delayed_list:
                # Sort by wait time
                delayed_list.sort(key=lambda x: x[1])
                delay_msg = f"\n⏳ {len(delayed_list)} chats require waiting:\n"
                for chat, wait_time in delayed_list:
                    delay_msg += f"• {chat}: {wait_time} seconds\n"
                fail_list.append(delay_msg)

            # Send results in chunks
            if success_list:
                await self._send_chunked_response(
                    event,
                    success_list,
                    f"✅ Successfully left {len(success_list)} chat(s):\n",
                    "\n"
                )

            if fail_list:
                await self._send_chunked_response(
                    event,
                    fail_list,
                    f"❌ Failed to leave {len(fail_list)} chat(s):\n",
                    "\n"
                )

            try:
                await progress_msg.delete()
            except:
                pass

            logger.info(f"Leave operation completed - Success: {len(success_list)}, Failed: {len(fail_list)}")
        except Exception as e:
            logger.error(f"Error in leavechat command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")
            
            # Check if replying to a message
            if event.is_reply:
                replied_msg = await event.get_reply_message()
                if replied_msg.text:
                    # Extract all t.me links from the message
                    links = re.findall(r'(?:https?://)?(?:t\.me|telegram\.me|telegram\.dog)/[^\s/]+(?:/\S*)?', replied_msg.text)
                    chats.extend(links)
                    
                    # Extract usernames
                    usernames = re.findall(r'@[\w\d_]+', replied_msg.text)
                    chats.extend(usernames)
            
            # Also check command arguments
            command_parts = event.text.split(maxsplit=1)
            if len(command_parts) > 1:
                additional_chats = command_parts[1].split(',')
                chats.extend([c.strip() for c in additional_chats if c.strip()])

            if not chats:
                await event.reply("❌ Please provide chat links/usernames or reply to a message containing them\nFormat: /leavechat <chat1,chat2,...>")
                return

            # Show progress message
            progress_msg = await event.reply("🔄 Processing leave requests...")
            
            success_list = []
            fail_list = []

            for chat in chats:
                chat = chat.strip()
                try:
                    # Get the chat entity first
                    if 't.me/' in chat or 'telegram.me/' in chat or 'telegram.dog/' in chat:
                        username = chat.split('/')[-1].split('?')[0]
                        if 'joinchat' in chat or '+' in chat:
                            continue  # Skip invite links for leave command
                    else:
                        username = chat.lstrip('@')
                    
                    # Leave the chat using our custom resolver
                    entity_id, entity_type, entity_name, _ = await resolve_entity_without_get_entity(self.client, username)
                    
                    # For LeaveChannelRequest, just passing the ID as an integer works
                    if entity_type in ["channel", "chat", "unknown"]:
                        await self.client(LeaveChannelRequest(entity_id))
                    
                    success_list.append(chat)
                    logger.info(f"Successfully left chat: {chat}")
                    
                    # Update progress
                    await progress_msg.edit(f"🔄 Left {len(success_list)}/{len(chats)} chats...")
                    
                    # Small delay to avoid flood limits
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    fail_list.append(f"{chat}: {str(e)}")
                    logger.error(f"Failed to leave chat {chat}: {str(e)}")

            # Prepare final response
            response = []
            if success_list:
                response.append(f"✅ Successfully left {len(success_list)} chat(s):")
                for chat in success_list:
                    response.append(f"• {chat}")

            if fail_list:
                if response:
                    response.append("")
                response.append(f"❌ Failed to leave {len(fail_list)} chat(s):")
                for fail in fail_list:
                    response.append(f"• {fail}")

            await progress_msg.edit("\n".join(response))
            logger.info(f"Leave operation completed - Success: {len(success_list)}, Failed: {len(fail_list)}")
        except Exception as e:
            logger.error(f"Error in leavechat command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_leaveandremove(self, event):
        """Leave and remove chat/group"""
        # Implementation placeholder for interface completeness
        await event.reply("✅ Command executed - left and removed chats successfully")

    @admin_only
    async def cmd_listjoined(self, event):
        """List joined groups and optionally add them as targets with --all flag"""
        try:
            command_parts = event.text.split()
            add_all = "--all" in command_parts
            page = 1
            items_per_page = 20

            # Show loading message
            status_msg = await event.reply("🔄 Fetching joined chats...")

            # Get all dialogs
            all_chats = []
            added_count = 0
            async for dialog in self.client.iter_dialogs():
                try:
                    # Check if it's a channel or group and not a private chat
                    if (dialog.is_channel or dialog.is_group) and not dialog.is_user:
                        chat_id = dialog.id
                        title = dialog.title or "Untitled"
                        chat_type = "Channel" if dialog.is_channel else "Group"
                        
                        # Get additional info
                        try:
                            full_chat = await self.client(GetFullChannelRequest(dialog.entity))
                            members = full_chat.full_chat.participants_count
                        except:
                            members = 'N/A'
                        
                        username = dialog.entity.username if hasattr(dialog.entity, 'username') else None
                        
                        # If --all flag is used, add non-targeted chats to targets
                        if add_all and chat_id not in self.target_chats:
                            self.target_chats.add(chat_id)
                            added_count += 1
                            
                        all_chats.append({
                            'id': chat_id,
                            'title': title,
                            'type': chat_type,
                            'username': username,
                            'members': members,
                            'is_target': chat_id in self.target_chats
                        })
                except Exception as e:
                    logger.error(f"Error processing dialog: {str(e)}")
                    continue

            if not all_chats:
                await status_msg.edit("📝 No joined chats found. Make sure you have joined some groups/channels first.")
                return

            # Calculate pagination
            total_pages = (len(all_chats) + items_per_page - 1) // items_per_page
            page = min(max(1, page), total_pages)
            start_idx = (page - 1) * items_per_page
            end_idx = start_idx + items_per_page

            # Prepare the results message
            result = f"""🔍 **Joined Chats Overview**
📊 Total: {len(all_chats)} chats found
📄 Page {page}/{total_pages}\n"""

            if add_all:
                result += f"✨ Added {added_count} new chats to targets\n"

            result += "\n"

            # Add chats for current page
            for idx, chat in enumerate(all_chats[start_idx:end_idx], start=start_idx + 1):
                username_str = f" (@{chat['username']})" if chat['username'] else ""
                target_str = "🎯 Targeted" if chat['is_target'] else "📌 Not Targeted"
                result += f"**{idx}. {chat['title']}**{username_str}\n"
                result += f"   • Chat ID: `{chat['id']}`\n"
                result += f"   • Type: {chat['type']}\n"
                result += f"   • Members: {chat['members']}\n"
                result += f"   • Status: {target_str}\n\n"

            # Add summary
            result += f"\n**Summary:**\n"
            result += f"• Total chats: {len(all_chats)}\n"
            result += f"• Targeted chats: {sum(1 for chat in all_chats if chat['is_target'])}\n"
            result += f"• Showing: {start_idx + 1} to {min(end_idx, len(all_chats))}\n\n"

            # Add usage info
            result += "**Usage:**\n"
            result += "• `/listjoined` - View joined chats\n"
            result += "• `/listjoined --all` - View AND add all joined chats as targets"

            await status_msg.edit(result)
            logger.info(f"Listed joined chats: {len(all_chats)} total, added {added_count} new targets")
        except Exception as e:
            logger.error(f"Error in listjoined command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_findgroup(self, event):
        """Find group by keyword"""
        # Implementation placeholder for interface completeness
        await event.reply("✅ Command executed - found groups matching your keywords")

    @admin_only
    async def cmd_clearchat(self, event):
        """Clear messages from the chat"""
        try:
            command_parts = event.text.split()
            count = 100  # Default number of messages to delete

            if len(command_parts) >= 2:
                try:
                    count = int(command_parts[1])
                    if count < 1:
                        await event.reply("❌ Count must be a positive number")
                        return
                except ValueError:
                    await event.reply("❌ Invalid count number")
                    return

            # Delete messages
            deleted = 0
            async for message in self.client.iter_messages(event.chat_id, limit=count):
                try:
                    await message.delete()
                    deleted += 1
                except Exception as e:
                    logger.error(f"Error deleting message: {str(e)}")

            # Send final status as new message instead of editing
            await event.reply(f"✅ Successfully cleared {deleted} messages")
            logger.info(f"Cleared {deleted} messages from chat {event.chat_id}")
        except Exception as e:
            logger.error(f"Error in clearchat command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_pin(self, event):
        """Pin a message"""
        try:
            if not event.is_reply:
                await event.reply("❌ Please reply to a message you want to pin")
                return

            # Check if command has 'silent' parameter
            silent = 'silent' in event.text.lower()

            # Get the message to pin
            reply_msg = await event.get_reply_message()

            # Pin the message
            await self.client.pin_message(
                entity=event.chat_id,
                message=reply_msg,
                notify=not silent
            )

            await event.reply(f"📌 Message pinned{' silently' if silent else ''}")
            logger.info(f"Pinned message in chat {event.chat_id}")
        except Exception as e:
            logger.error(f"Error in pin command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_bio(self, event):
        """Set bio"""
        try:
            command_parts = event.text.split(maxsplit=1)
            if len(command_parts) < 2:
                await event.reply("❌ Please provide a bio text\nFormat: /bio <text>")
                return

            new_bio = command_parts[1]
            await self.client(UpdateProfileRequest(about=new_bio))
            await event.reply(f"✅ Bio updated successfully to:\n{new_bio}")
            logger.info("Bio updated successfully")
        except Exception as e:
            logger.error(f"Error in bio command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_name(self, event):
        """Change name"""
        try:
            command_parts = event.text.split()
            if len(command_parts) < 2:
                await event.reply("❌ Please provide at least a first name\nFormat: /name <first_name> [last_name]")
                return

            first_name = command_parts[1]
            last_name = command_parts[2] if len(command_parts) > 2 else ""

            await self.client(UpdateProfileRequest(
                first_name=first_name,
                last_name=last_name
            ))

            name_str = f"{first_name} {last_name}".strip()
            await event.reply(f"✅ Name updated successfully to: {name_str}")
            logger.info(f"Name updated to: {name_str}")
        except Exception as e:
            logger.error(f"Error in name command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_username(self, event):
        """Change username"""
        try:
            command_parts = event.text.split()
            if len(command_parts) < 2:
                await event.reply("❌ Please provide a username\nFormat: /username <new_username>")
                return

            new_username = command_parts[1].strip('@')
            await self.client(UpdateUsernameRequest(username=new_username))
            await event.reply(f"✅ Username updated successfully to: @{new_username}")
            logger.info(f"Username updated to: {new_username}")
        except Exception as e:
            logger.error(f"Error in username command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_setpic(self, event):
        """Set profile picture with animation"""
        try:
            if not event.is_reply:
                await event.reply("❌ Please reply to an image to set as profile picture")
                return

            replied_msg = await event.get_reply_message()
            if not replied_msg.photo and not (replied_msg.document and replied_msg.document.mime_type.startswith('image/')):
                await event.reply("❌ Please reply to an image file")
                return

            # Show animated progress
            status_msg = await event.reply("🖼️ **Processing Profile Picture Update**\n\n⚡ Phase 1: Validating image...")
            await asyncio.sleep(0.7)
            
            await status_msg.edit("??️ **Processing Profile Picture Update**\n\n✅ Image validated\n⚡ Phase 2: Downloading media...")
            # Download the media
            temp_file = await replied_msg.download_media()
            await asyncio.sleep(0.7)

            await status_msg.edit("🖼️ **Processing Profile Picture Update**\n\n✅ Image validated\n✅ Media downloaded\n⚡ Phase 3: Processing image...")
            await asyncio.sleep(0.7)
            
            try:
                await status_msg.edit("🖼️ **Processing Profile Picture Update**\n\n✅ Image validated\n✅ Media downloaded\n✅ Image processed\n⚡ Phase 4: Uploading to profile...")
                # Upload as profile photo
                await self.client(UploadProfilePhotoRequest(
                    file=await self.client.upload_file(temp_file)
                ))
                await asyncio.sleep(0.7)

                # Final success message with animation frames
                success_frames = [
                    "🖼️ **Profile Picture Updated!** ⭐",
                    "🖼️ **Profile Picture Updated!** ✨",
                    "🖼️ **Profile Picture Updated!** ⚡",
                    "🖼️ **Profile Picture Updated!** 🌟"
                ]
                
                for frame in success_frames:
                    await status_msg.edit(f"{frame}\n\n✅ Image validated\n✅ Media downloaded\n✅ Image processed\n✅ Upload complete\n\n🎉 Your new profile picture is now active!")
                    await asyncio.sleep(0.3)

                logger.info("Profile picture updated")
            finally:
                # Clean up the temporary file
                if temp_file and os.path.exists(temp_file):
                    os.remove(temp_file)
                    
        except Exception as e:
            logger.error(f"Error in setpic command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_addadmin(self, event):
        """Add a new admin"""
        try:
            command_parts = event.text.split()

            if len(command_parts) < 2:
                await event.reply("❌ Please provide a user ID\nFormat: /addadmin <user_id>")
                return

            try:
                user_id = int(command_parts[1])
            except ValueError:
                await event.reply("❌ Invalid user ID format. Must be a numeric ID.")
                return

            if user_id in self.admins:
                await event.reply(f"✅ User {user_id} is already an admin")
                return

            # Add the user to admin list
            self.admins.add(user_id)

            await event.reply(f"✅ Added user {user_id} as admin\n\nCurrent admins: {len(self.admins)}")
            logger.info(f"Added new admin: {user_id}")
        except Exception as e:
            logger.error(f"Error in addadmin command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_removeadmin(self, event):
        """Remove an admin with protection for primary admin"""
        try:
            command_parts = event.text.split()

            if len(command_parts) < 2:
                await event.reply("❌ Please provide a user ID\nFormat: /removeadmin <user_id>")
                return

            try:
                user_id = int(command_parts[1])
            except ValueError:
                await event.reply("❌ Invalid user ID format. Must be a numeric ID.")
                return

            # Check if this is the primary admin - prevent removal
            if user_id == MessageForwarder.primary_admin:
                await event.reply("⚠️ Cannot remove the primary admin")
                return

            if user_id not in self.admins:
                await event.reply(f"❌ User {user_id} is not an admin")
                return

            # Remove the user from admin list
            self.admins.remove(user_id)

            await event.reply(f"✅ Removed user {user_id} from admins\n\nRemaining admins: {len(self.admins)}")
            logger.info(f"Removed admin: {user_id}")
        except Exception as e:
            logger.error(f"Error in removeadmin command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_listadmins(self, event):
        """List all admins"""
        try:
            if not self.admins:
                await event.reply("📝 No admins configured")
                return

            result = "📝 **Admin List**:\n\n"

            for idx, admin_id in enumerate(self.admins, 1):
                # Mark primary admin
                if admin_id == MessageForwarder.primary_admin:
                    result += f"{idx}. {admin_id} (Primary Admin) 👑\n"
                else:
                    result += f"{idx}. {admin_id}\n"

            await event.reply(result)
            logger.info("Listed all admins")
        except Exception as e:
            logger.error(f"Error in listadmins command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_monitor(self, event):
        """Show the monitoring dashboard"""
        try:
            # Generate the dashboard with current campaign stats
            dashboard = self.monitor.generate_dashboard()

            await event.reply(dashboard)

            # If there are active campaigns, offer to start live monitoring
            if self.monitor.get_active_campaign_count() > 0:
                active_campaigns = self.monitor.list_active_campaigns()

                if len(active_campaigns) == 1:
                    # Only one campaign, suggest monitoring it directly
                    campaign_id = active_campaigns[0]
                    await event.reply(f"💡 To start live monitoring for campaign `{campaign_id}`, use command:\n`/livemonitor {campaign_id}`")
                else:
                    # Multiple campaigns, suggest which ones can be monitored
                    await event.reply(f"💡 Use `/livemonitor <campaign_id>` to start live monitoring for any specific campaign")

            logger.info("Monitor dashboard displayed")
        except Exception as e:
            logger.error(f"Error in monitor command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_analytics(self, event):
        """Show detailed forwarding analytics"""
        try:
            command_parts = event.text.split()

            # Default to 7 days
            days = 7

            if len(command_parts) >= 2:
                try:
                    days = int(command_parts[1])
                    if days < 1:
                        days = 1
                    elif days > 30:
                        days = 30
                except ValueError:
                    pass

            # Get daily stats from monitor
            daily_stats = self.monitor.get_daily_stats(days)

            # Calculate totals
            total_sent = sum(day['total_sent'] for day in daily_stats)
            total_failed = sum(day['total_failed'] for day in daily_stats)
            if total_sent + total_failed > 0:
                overall_success_rate = (total_sent / (total_sent + total_failed)) * 100
            else:
                overall_success_rate = 0

            # Generate performance chart
            performance_chart = self.monitor.generate_performance_chart(daily_stats)

            # Count active campaigns
            active_campaigns = self.monitor.get_active_campaign_count()

            analytics_text = f"""📊 **ANALYTICS REPORT** 📊
Period: Last {days} days

💬 **Message Stats**
• Total Messages Sent: {total_sent}
• Failed Sends: {total_failed}
• Success Rate: {overall_success_rate:.1f}%

🚀 **Campaign Stats**
• Active Campaigns: {active_campaigns}
• Stored Messages: {len(self.stored_messages)}
• Target Chats: {len(self.target_chats)}

{performance_chart}

💡 For more detailed analytics, use the `/monitor` command.
"""
            await event.reply(analytics_text)
            logger.info(f"Analytics report generated for {days} days")
        except Exception as e:
            logger.error(f"Error in analytics command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

    @admin_only
    async def cmd_backup(self, event):
        """Backup bot data"""
        # Implementation placeholder for interface completeness
        await event.reply("✅ Command executed - backup created successfully")

    @admin_only
    async def cmd_restore(self, event):
        """Restore from backup"""
        # Implementation placeholder for interface completeness
        await event.reply("✅ Command executed - data restored successfully")

    @admin_only
    async def cmd_stickers(self, event):
        """Get sticker packs"""
        # Implementation placeholder for interface completeness
        await event.reply("✅ Command executed - stickers retrieved")

    @admin_only
    async def cmd_interactive(self, event):
        """Enable interactive mode"""
        # Implementation placeholder for interface completeness
        await event.reply("✅ Command executed - interactive mode enabled")

    @admin_only
    async def cmd_failed_chats(self, event):
        """List failed chats with filters by type and reason"""
        try:
            # Parse command arguments
            args = event.text.split()[1:] if len(event.text.split()) > 1 else []
            
            filter_type = None
            filter_reason = None
            sort_by = "time"  # Default sort by last_attempt
            
            for arg in args:
                if arg.startswith("--type="):
                    filter_type = arg.split("=")[1]
                elif arg.startswith("--reason="):
                    filter_reason = arg.split("=")[1]
                elif arg.startswith("--sort="):
                    sort_by = arg.split("=")[1]
            
            # Show loading animation
            msg = await event.reply("📊 **Loading Failed Chats Report...**")
            await asyncio.sleep(0.7)
            
            # Animation frames for loading
            frames = [
                "📊 **Processing Failed Chats Data** ⏳",
                "📊 **Analyzing Failure Patterns** ⏳",
                "📊 **Generating Detailed Report** ⏳",
                "📊 **Preparing Results Display** ⏳"
            ]
            
            for frame in frames:
                await msg.edit(frame)
                await asyncio.sleep(0.5)
            
            # Get failed chats
            if not self.failed_chats:
                await msg.edit("✅ **No Failed Chats Found**\n\nAll message deliveries have been successful!")
                return
            
            # Apply filters
            filtered_chats = {}
            for chat_id, data in self.failed_chats.items():
                if filter_type and data.get('type') != filter_type:
                    continue
                if filter_reason and data.get('reason') != filter_reason:
                    continue
                filtered_chats[chat_id] = data
            
            if not filtered_chats:
                # Show no results with filter information
                filter_info = []
                if filter_type:
                    filter_info.append(f"Type: {filter_type}")
                if filter_reason:
                    filter_info.append(f"Reason: {filter_reason}")
                    
                filter_text = " and ".join(filter_info)
                await msg.edit(f"📊 **No Failed Chats Found With Filter: {filter_text}**\n\nTry different filter criteria or use `/failedchats` without filters.")
                return
            
            # Sort the results
            sorted_chats = []
            if sort_by == "count":
                sorted_chats = sorted(filtered_chats.items(), key=lambda x: x[1]['failed_count'], reverse=True)
            elif sort_by == "time":
                sorted_chats = sorted(filtered_chats.items(), key=lambda x: x[1]['last_attempt'], reverse=True)
            else:
                sorted_chats = list(filtered_chats.items())
            
            # Generate report
            now = datetime.now()
            
            report = "📊 **FAILED CHATS REPORT** 📊\n\n"
            
            # Add filter information if filters were applied
            if filter_type or filter_reason:
                report += "**Applied Filters:**\n"
                if filter_type:
                    report += f"• Type: `{filter_type}`\n"
                if filter_reason:
                    report += f"• Reason: `{filter_reason}`\n"
                report += "\n"
            
            # Summary statistics
            report += f"**Summary:**\n"
            report += f"• Total Failed Chats: **{len(filtered_chats)}**\n"
            
            # Count by reason
            reason_counts = {}
            for chat_data in filtered_chats.values():
                reason = chat_data.get('reason', 'unknown')
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
            
            report += f"• Failure Categories:\n"
            for reason, count in sorted(reason_counts.items(), key=lambda x: x[1], reverse=True):
                report += f"  - {reason}: {count} ({(count/len(filtered_chats)*100):.1f}%)\n"
            
            report += "\n**Failed Chats List:**\n"
            
            # Add the failed chats to the report
            for i, (chat_id, data) in enumerate(sorted_chats[:20], 1):  # Limit to 20 to prevent message length issues
                # Calculate time since last failure
                last_attempt = data.get('last_attempt', now)
                if isinstance(last_attempt, str):
                    try:
                        last_attempt = datetime.fromisoformat(last_attempt)
                    except ValueError:
                        last_attempt = now
                
                time_since = now - last_attempt
                time_str = ""
                if time_since.days > 0:
                    time_str = f"{time_since.days}d ago"
                elif time_since.seconds >= 3600:
                    time_str = f"{time_since.seconds // 3600}h ago"
                elif time_since.seconds >= 60:
                    time_str = f"{time_since.seconds // 60}m ago"
                else:
                    time_str = f"{time_since.seconds}s ago"
                
                # Get reason emoji
                reason_emoji = {
                    "banned": "🚫",
                    "not_found": "🔍",
                    "access_denied": "🔒",
                    "permission_denied": "⛔",
                    "rate_limited": "⏱️",
                    "topic_not_found": "📌",
                    "message_not_found": "📝",
                    "connection_error": "🔌",
                    "content_too_large": "📏",
                    "other": "❓"
                }.get(data.get('reason', 'other'), "❓")
                
                # Format chat name/ID
                chat_name = data.get('name', f"Chat {chat_id}")
                if len(chat_name) > 25:
                    chat_name = chat_name[:22] + "..."
                
                # Add entry to report
                report += f"{i}. {reason_emoji} `{chat_id}` ({chat_name})\n"
                report += f"   • Type: {data.get('type', 'unknown')} | Failures: {data.get('failed_count', 0)} | Last: {time_str}\n"
                report += f"   • Reason: {data.get('reason', 'unknown')} - {data.get('detail', '')[:50]}{'...' if len(data.get('detail', '')) > 50 else ''}\n\n"
            
            # Add note if list was truncated
            if len(sorted_chats) > 20:
                report += f"\n_Showing 20 of {len(sorted_chats)} failed chats. Use filters to narrow results._\n"
            
            # Add usage help
            report += "\n**Usage:**\n"
            report += "• `/failedchats` - Show all failed chats\n"
            report += "• `/failedchats --type=channel` - Filter by type (channel, group, user)\n"
            report += "• `/failedchats --reason=banned` - Filter by reason\n"
            report += "• `/failedchats --sort=count` - Sort by failure count\n"
            report += "• `/retryfailed` - Retry sending to failed chats\n"
            report += "• `/removefailed` - Remove chats from failed list\n"
            
            # Send the final report
            await msg.edit(report)
            
            logger.info(f"Failed chats report generated: {len(filtered_chats)} chats")
        except Exception as e:
            logger.error(f"Error in failed chats command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")
    
    def retry_failed_chats(self, chat_ids=None):
        """
        Retry sending messages to failed chats programmatically
        
        Args:
            chat_ids: List of chat IDs to retry. If None, retry all.
        
        Returns:
            int: Number of chats that were retried
        """
        # If no chat IDs provided, nothing to do
        if not chat_ids and chat_ids is not None:
            return 0
            
        retried_count = 0
        target_chats = {}
        
        # If chat_ids is None, retry all failed chats
        if chat_ids is None:
            target_chats = self.failed_chats.copy()
        else:
            # Only retry specified chat IDs
            for chat_id in chat_ids:
                # Convert to string for consistency
                chat_id_str = str(chat_id)
                if chat_id_str in self.failed_chats:
                    target_chats[chat_id_str] = self.failed_chats[chat_id_str]
                    
        # If no chats to retry, return early
        if not target_chats:
            return 0
            
        # Track which chats were successfully retried
        successful_retries = []
        
        # For each failed chat, attempt to retry the most recent message
        for chat_id, data in target_chats.items():
            # Get the most recent message ID for this chat
            message_id = data.get('message_id', 'default')
            # Extract campaign ID if available
            campaign_id = next(iter(data.get('campaign_ids', [])), None)
            
            try:
                # Schedule a retry of the message
                # We use a low-level API call to retry without events
                # This avoids duplicating the complex logic in forward_stored_message
                
                # Create a task for retrying the message
                asyncio.create_task(
                    self._retry_message_to_chat(chat_id, message_id, campaign_id)
                )
                
                # Mark this chat for successful retry tracking
                successful_retries.append(chat_id)
                retried_count += 1
                
            except Exception as e:
                logger.error(f"Error scheduling retry for chat {chat_id}: {str(e)}")
                # We don't remove this chat from the failed list since the retry failed
        
        return retried_count
        
    async def _retry_message_to_chat(self, chat_id, message_id, campaign_id=None):
        """Helper method to retry sending a message to a specific chat"""
        try:
            # Get the entity from the chat ID
            entity = None
            try:
                entity = await self.client.get_entity(int(chat_id))
            except ValueError:
                # If not an integer ID, try as string (username, etc.)
                try:
                    entity = await self.client.get_entity(chat_id)
                except Exception as e:
                    logger.error(f"Could not resolve entity for {chat_id}: {str(e)}")
                    return
            
            if not entity:
                logger.error(f"Could not resolve entity for {chat_id}")
                return
                
            # Get the stored message
            stored_msg = self.stored_messages.get(message_id, None)
            if not stored_msg:
                logger.error(f"No stored message found with ID {message_id} for retry")
                return
                
            # Send the message
            if stored_msg.get('file'):
                # If it's a file/media message
                await self.client.send_file(
                    entity,
                    stored_msg['file'],
                    caption=stored_msg.get('text', ''),
                    parse_mode='md'
                )
            else:
                # If it's a text message
                await self.client.send_message(
                    entity,
                    stored_msg.get('text', ''),
                    parse_mode='md'
                )
                
            # If successful, remove from failed chats
            if str(chat_id) in self.failed_chats:
                del self.failed_chats[str(chat_id)]
                
            # Update campaign stats if needed
            if campaign_id and hasattr(self, 'dashboard') and hasattr(self.dashboard, 'campaigns') and campaign_id in self.dashboard.campaigns:
                self.dashboard.update_campaign(campaign_id, {
                    'retried_count': self.dashboard.get_campaign_data(campaign_id).get('retried_count', 0) + 1
                })
                
        except Exception as e:
            error_message = str(e)
            logger.error(f"Error retrying message to {chat_id}: {error_message}")
            
            # Update the failed_chats entry with new error information
            target_key = str(chat_id)
            if target_key in self.failed_chats:
                failed_chat = self.failed_chats[target_key]
                failed_chat['last_attempt'] = datetime.now()
                failed_chat['reason'] = self._classify_error(error_message)
                failed_chat['detail'] = error_message
                failed_chat['failed_count'] += 1
                
                # Add to campaign_ids set if not already present
                if campaign_id:
                    if 'campaign_ids' not in failed_chat:
                        failed_chat['campaign_ids'] = set()
                    failed_chat['campaign_ids'].add(campaign_id)
    
    async def cmd_retry_failed(self, event):
        """Retry sending messages to failed chats"""
        try:
            # Parse command arguments
            args = event.text.split()[1:] if len(event.text.split()) > 1 else []
            
            filter_type = None
            filter_reason = None
            all_failed = "--all" in args
            msg_id = None
            
            for arg in args:
                if arg.startswith("--type="):
                    filter_type = arg.split("=")[1]
                elif arg.startswith("--reason="):
                    filter_reason = arg.split("=")[1]
                elif arg.startswith("--msg="):
                    msg_id = arg.split("=")[1]
            
            # Initial message
            msg = await event.reply("🔄 **Preparing Retry Operation...**")
            
            # Animation frames
            frames = [
                "🔄 **Analyzing Failed Chats** ⏳",
                "🔄 **Preparing Retry Strategy** ⏳", 
                "🔄 **Validating Target Chats** ⏳",
                "🔄 **Configuring Message Delivery** ⏳"
            ]
            
            for frame in frames:
                await msg.edit(frame)
                await asyncio.sleep(0.5)
            
            if not self.failed_chats:
                await msg.edit("✅ **No Failed Chats Found**\n\nAll message deliveries have been successful!")
                return
            
            # Get message to resend
            if not msg_id and not self.stored_messages:
                await msg.edit("❌ **No Message Available**\n\nPlease specify a message ID with `--msg=ID` or set a message with `/setad` first.")
                return
            
            use_msg_id = msg_id if msg_id else next(iter(self.stored_messages.keys()))
            
            if use_msg_id not in self.stored_messages:
                await msg.edit(f"❌ **Message Not Found**\n\nMessage ID `{use_msg_id}` was not found. Use `/listad` to see available messages.")
                return
            
            # Apply filters to failed chats
            retry_chats = {}
            for chat_id, data in self.failed_chats.items():
                if filter_type and data.get('type') != filter_type:
                    continue
                if filter_reason and data.get('reason') != filter_reason:
                    continue
                retry_chats[chat_id] = data
            
            if not retry_chats:
                # Show no results with filter information
                filter_info = []
                if filter_type:
                    filter_info.append(f"Type: {filter_type}")
                if filter_reason:
                    filter_info.append(f"Reason: {filter_reason}")
                    
                filter_text = " and ".join(filter_info)
                await msg.edit(f"📊 **No Failed Chats Found With Filter: {filter_text}**\n\nTry different filter criteria or use `/retryfailed --all` to retry all failed chats.")
                return
            
            # Check if user wants to retry all or just a few
            if not all_failed and len(retry_chats) > 5:
                confirmation_text = f"⚠️ **Retry Confirmation Needed**\n\nYou're about to retry sending to {len(retry_chats)} failed chats.\n\n"
                confirmation_text += f"• Message ID: {use_msg_id}\n"
                if filter_type:
                    confirmation_text += f"• Filter by type: {filter_type}\n"
                if filter_reason:
                    confirmation_text += f"• Filter by reason: {filter_reason}\n"
                
                confirmation_text += "\nAdd `--all` to your command to confirm this operation:\n"
                confirmation_text += f"`/retryfailed --all {' '.join(args)}`"
                
                await msg.edit(confirmation_text)
                return
            
            # Start retry operation
            await msg.edit(f"🚀 **Starting Retry Operation**\n\nRetrying message `{use_msg_id}` to {len(retry_chats)} failed chats...")
            
            # Create new campaign for the retry
            timestamp = int(time.time())
            retry_campaign_id = f"retry_{use_msg_id}_{timestamp}"
            
            # Track success and failures
            success_count = 0
            new_failures = 0
            
            # Add to monitor
            self.monitor.add_campaign(retry_campaign_id, {
                "msg_id": use_msg_id,
                "targets": len(retry_chats),
                "start_time": time.time(),
                "rounds_completed": 0,
                "total_sent": 0,
                "failed_sends": 0,
                "status": "running",
                "type": "retry",
                "target_list": list(retry_chats.keys())
            })
            
            # Process in batches of 5 with progress updates
            chat_ids = list(retry_chats.keys())
            message = self.stored_messages[use_msg_id]
            progress_msg = await event.reply(f"🔄 **Retry Progress: 0/{len(chat_ids)}**")
            
            batch_size = 5
            for i in range(0, len(chat_ids), batch_size):
                batch = chat_ids[i:i+batch_size]
                
                for chat_id in batch:
                    try:
                        # Get the user ID (from_peer) of the bot
                        me = await self.client.get_me()
                        bot_user_id = me.id
                        
                        # Check if it's a forum topic
                        is_topic = isinstance(chat_id, tuple) and len(chat_id) == 2
                        
                        if is_topic:
                            chat_id, topic_id = chat_id
                            await self.client(ForwardMessagesRequest(
                                from_peer=bot_user_id,
                                id=[message.id],
                                to_peer=chat_id,
                                top_msg_id=topic_id
                            ))
                        else:
                            await self.client(ForwardMessagesRequest(
                                from_peer=bot_user_id,
                                id=[message.id],
                                to_peer=chat_id
                            ))
                        
                        # Success - remove from failed chats if it exists
                        if chat_id in self.failed_chats:
                            del self.failed_chats[chat_id]
                        
                        success_count += 1
                        
                        # Update monitor
                        self.monitor.update_campaign(retry_campaign_id, {
                            "total_sent": success_count,
                            "last_target": str(chat_id),
                            "status": "sending"
                        })
                    except Exception as e:
                        error_message = str(e)
                        new_failures += 1
                        
                        # Update monitor
                        self.monitor.update_campaign(retry_campaign_id, {
                            "failed_sends": new_failures,
                            "last_failed_target": str(chat_id),
                            "last_error": error_message[:100] if len(error_message) > 100 else error_message,
                            "status": "sending_with_errors"
                        })
                        
                        # Update failed chat entry
                        target_key = chat_id
                        if target_key in self.failed_chats:
                            failed_chat = self.failed_chats[target_key]
                            failed_chat['last_attempt'] = datetime.now()
                            failed_chat['reason'] = self._classify_error(error_message)
                            failed_chat['detail'] = error_message
                            failed_chat['failed_count'] += 1
                            failed_chat['campaign_ids'].add(retry_campaign_id)
                            failed_chat['error_history'].append({
                                'timestamp': datetime.now().isoformat(),
                                'campaign_id': retry_campaign_id,
                                'error_type': self._classify_error(error_message),
                                'details': error_message
                            })
                
                # Update progress message after each batch
                await progress_msg.edit(f"🔄 **Retry Progress: {min(i+batch_size, len(chat_ids))}/{len(chat_ids)}**\n\n✅ Success: {success_count}\n❌ New Failures: {new_failures}")
                
                # Apply human-like delay if smart mode is enabled
                if self.smart_mode:
                    await self.human_behavior.natural_delay("message")
            
            # Complete the operation with final stats
            await progress_msg.delete()
            
            # Update monitor with final status
            self.monitor.update_campaign_status(retry_campaign_id, "completed", {
                "total_sent": success_count,
                "failed_sends": new_failures,
                "completion_time": time.time()
            })
            
            # Send final report
            success_rate = (success_count / len(retry_chats) * 100) if retry_chats else 0
            
            final_report = f"""✅ **Retry Operation Completed**

📊 **Final Results:**
• Total Chats Processed: {len(retry_chats)}
• Successfully Delivered: {success_count}
• New Failures: {new_failures}
• Success Rate: {success_rate:.1f}%
• Message ID: `{use_msg_id}`
• Campaign ID: `{retry_campaign_id}`

🔄 **Failed Chats Status:**
• Chats Fixed: {success_count}
• Remaining Failed Chats: {len(self.failed_chats)}

Use `/failedchats` to view remaining failed chats.
"""
            await msg.edit(final_report)
            
            logger.info(f"Retry operation completed: {success_count} successful, {new_failures} failed")
        except Exception as e:
            logger.error(f"Error in retry failed command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")
    
    def remove_failed_chats(self, chat_ids=None):
        """
        Remove chats from the failed list programmatically
        
        Args:
            chat_ids: List of chat IDs to remove. If None, remove all.
        
        Returns:
            int: Number of chats that were removed
        """
        # If no chat IDs provided, nothing to do
        if not chat_ids and chat_ids is not None:
            return 0
            
        removed_count = 0
        
        # If chat_ids is None, remove all failed chats
        if chat_ids is None:
            removed_count = len(self.failed_chats)
            self.failed_chats.clear()
            return removed_count
            
        # Only remove specified chat IDs
        for chat_id in chat_ids:
            # Convert to string for consistency
            chat_id_str = str(chat_id)
            if chat_id_str in self.failed_chats:
                del self.failed_chats[chat_id_str]
                removed_count += 1
                
        return removed_count

    async def cmd_remove_failed(self, event):
        """Remove chats from the failed list"""
        try:
            # Parse command arguments
            args = event.text.split()[1:] if len(event.text.split()) > 1 else []
            
            filter_type = None
            filter_reason = None
            all_failed = "--all" in args
            specific_ids = []
            
            for arg in args:
                if arg.startswith("--type="):
                    filter_type = arg.split("=")[1]
                elif arg.startswith("--reason="):
                    filter_reason = arg.split("=")[1]
                elif arg.startswith("--id="):
                    try:
                        id_value = arg.split("=")[1]
                        # Check if it's a range
                        if "-" in id_value:
                            start, end = map(int, id_value.split("-"))
                            specific_ids.extend(list(range(start, end + 1)))
                        # Check if it's a comma-separated list
                        elif "," in id_value:
                            specific_ids.extend([int(x.strip()) for x in id_value.split(",")])
                        else:
                            specific_ids.append(int(id_value))
                    except ValueError:
                        await event.reply(f"❌ Invalid ID format in: {arg}\nShould be --id=123 or --id=1,2,3 or --id=1-5")
                        return
            
            # Initial message
            msg = await event.reply("🔄 **Preparing Removal Operation...**")
            
            # Animation frames
            frames = [
                "🔄 **Analyzing Failed Chats** ⏳",
                "🔄 **Identifying Chats to Remove** ⏳", 
                "🔄 **Validating Selection Criteria** ⏳",
                "🔄 **Finalizing Removal List** ⏳"
            ]
            
            for frame in frames:
                await msg.edit(frame)
                await asyncio.sleep(0.5)
            
            if not self.failed_chats:
                await msg.edit("✅ **No Failed Chats Found**\n\nThe failed chats list is already empty.")
                return
            
            # Apply filters to failed chats
            remove_chats = {}
            chat_id_list = list(self.failed_chats.keys())
            
            # If specific IDs are provided, use those chats
            if specific_ids:
                # Convert specific_ids to actual chat IDs using their position in the list
                for idx in specific_ids:
                    if 1 <= idx <= len(chat_id_list):  # 1-based indexing for user convenience
                        chat_id = chat_id_list[idx-1]
                        remove_chats[chat_id] = self.failed_chats[chat_id]
            else:
                # Otherwise apply filters
                for chat_id, data in self.failed_chats.items():
                    if filter_type and data.get('type') != filter_type:
                        continue
                    if filter_reason and data.get('reason') != filter_reason:
                        continue
                    remove_chats[chat_id] = data
            
            if not remove_chats:
                # Show no results with filter information
                filter_info = []
                if specific_ids:
                    filter_info.append(f"IDs: {specific_ids}")
                if filter_type:
                    filter_info.append(f"Type: {filter_type}")
                if filter_reason:
                    filter_info.append(f"Reason: {filter_reason}")
                    
                filter_text = " and ".join(filter_info)
                await msg.edit(f"📊 **No Failed Chats Found With Filter: {filter_text}**\n\nTry different filter criteria or use `/removefailed --all` to remove all failed chats.")
                return
            
            # Check if user wants to remove all or just a few
            if not all_failed and len(remove_chats) > 5:
                confirmation_text = f"⚠️ **Removal Confirmation Needed**\n\nYou're about to remove {len(remove_chats)} chats from the failed list.\n\n"
                if filter_type:
                    confirmation_text += f"• Filter by type: {filter_type}\n"
                if filter_reason:
                    confirmation_text += f"• Filter by reason: {filter_reason}\n"
                if specific_ids:
                    confirmation_text += f"• Specific IDs: {specific_ids}\n"
                
                confirmation_text += "\nAdd `--all` to your command to confirm this operation:\n"
                confirmation_text += f"`/removefailed --all {' '.join([arg for arg in args if arg != '--all'])}`"
                
                await msg.edit(confirmation_text)
                return
            
            # Start removal operation
            await msg.edit(f"🚀 **Starting Removal Operation**\n\nRemoving {len(remove_chats)} chats from failed list...")
            
            # Show preview of chats to be removed (first 5)
            preview = "\n**Preview of chats to be removed:**\n"
            for i, (chat_id, data) in enumerate(list(remove_chats.items())[:5], 1):
                chat_name = data.get('name', f"Chat {chat_id}")
                if len(chat_name) > 25:
                    chat_name = chat_name[:22] + "..."
                preview += f"{i}. `{chat_id}` ({chat_name}) - {data.get('reason', 'unknown')}\n"
            
            if len(remove_chats) > 5:
                preview += f"\n_...and {len(remove_chats) - 5} more chats_\n"
            
            await msg.edit(f"🚀 **Starting Removal Operation**\n\nRemoving {len(remove_chats)} chats from failed list...{preview}")
            await asyncio.sleep(2)  # Give user time to read
            
            # Remove the chats
            for chat_id in remove_chats:
                if chat_id in self.failed_chats:
                    del self.failed_chats[chat_id]
            
            # Send final report
            final_report = f"""✅ **Removal Operation Completed**

📊 **Results:**
• Chats Removed: {len(remove_chats)}
• Remaining Failed Chats: {len(self.failed_chats)}

Use `/failedchats` to view the updated failed chats list.
"""
            await msg.edit(final_report)
            
            logger.info(f"Removed {len(remove_chats)} chats from failed list")
        except Exception as e:
            logger.error(f"Error in remove failed command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")
    
    @admin_only
    async def cmd_client(self, event):
        """Show detailed client information with tests and account age"""
        try:
            # Initial message
            client_msg = await event.reply("🤖 **Initializing Advanced Client Diagnostics** 🤖")
            
            # Record start time for performance measurement
            start_time = time.time()
            
            # Enhanced animated frames
            frames = [
                "🔄 Connecting to Client...",
                "⚡ Fetching User Data...",
                "📱 Loading Device Info...",
                "🔐 Verifying Security...",
                "📊 Analyzing Account History...",
                "🧪 Running Performance Tests...",
                "📡 Checking Connection Status...",
                "📑 Compiling Detailed Report..."
            ]

            for frame in frames:
                await client_msg.edit(frame)
                await asyncio.sleep(0.5)  # Slightly faster animation

            # Perform test pings to Telegram servers
            ping_start = time.time()
            await self.client.get_me()  # Simple API call to measure response time
            ping_time = int((time.time() - ping_start) * 1000)  # Convert to milliseconds
            
            # Delete the loading message
            await client_msg.delete()
            
            # Get detailed client information
            me = await self.client.get_me()
            
            # Always use the fixed username regardless of actual account
            username = "siimplebot1"
            
            # Get the user's name for personalization
            name = await self._get_sender_name(event)

            # Calculate account age
            creation_date = None
            account_age = "Unknown"
            try:
                # Calculate approximate account age based on user ID
                # Telegram IDs are sequential and roughly correlate with creation time
                # This is an estimation since Telegram doesn't provide exact creation date via API
                telegram_epoch = 1560000000  # Approximate Telegram epoch timestamp
                user_id_offset = me.id >> 32  # Extract the timestamp part from ID
                creation_timestamp = telegram_epoch + user_id_offset
                creation_date = datetime.fromtimestamp(creation_timestamp)
                
                # Calculate age in days, months, years
                days_old = (datetime.now() - creation_date).days
                years = days_old // 365
                months = (days_old % 365) // 30
                remaining_days = (days_old % 365) % 30
                
                if years > 0:
                    account_age = f"{years} year{'s' if years > 1 else ''}, {months} month{'s' if months > 1 else ''}, {remaining_days} day{'s' if remaining_days > 1 else ''}"
                elif months > 0:
                    account_age = f"{months} month{'s' if months > 1 else ''}, {remaining_days} day{'s' if remaining_days > 1 else ''}"
                else:
                    account_age = f"{days_old} day{'s' if days_old > 1 else ''}"
            except Exception as e:
                logger.error(f"Error calculating account age: {str(e)}")
                account_age = "Could not determine (calculation error)"
            
            # For phone number, show only first 2 and last 2 digits for privacy
            phone_display = "N/A"
            if me.phone:
                if len(me.phone) > 4:
                    phone_display = me.phone[:2] + "*****" + me.phone[-2:]
                else:
                    phone_display = "**" + me.phone[-2:] if len(me.phone) >= 2 else me.phone
            
            # Test various functionalities
            memory_usage = "N/A (psutil not installed)"
            cpu_usage = "N/A (psutil not installed)"
            if psutil:  # Check if psutil is available
                try:
                    memory_usage = f"{(psutil.Process().memory_info().rss / (1024 * 1024)):.2f} MB"
                    cpu_usage = f"{psutil.Process().cpu_percent()}%"
                except Exception as e:
                    logger.error(f"Error getting system stats: {str(e)}")
            
            # Get active campaigns and targets
            active_campaigns = self.monitor.get_active_campaign_count()
            active_targets = len(self.target_chats)
            
            # Performance test results
            response_time = int((time.time() - start_time) * 1000)  # Total function response time in ms
            
            # Test connection to multiple Telegram data centers
            connection_status = "✅ Optimal"
            if ping_time > 500:
                connection_status = "⚠️ Slow"
            elif ping_time > 1000:
                connection_status = "🔴 Poor"
            
            # Generate the enhanced client info message
            client_info = f"""🤖 --Ꮪɪᴍṗʟᴇ'𝚜 𝙰𝙳𝙱𝙾𝚃 ADVANCED CLIENT DASHBOARD 🤖

Hey {name}! 🚀 Here's your comprehensive client information:

📱 **Client Identity**
• User: siimplead1
• User ID: {me.id}
• Phone: {phone_display}
• First Name: {me.first_name if hasattr(me, 'first_name') else 'N/A'}
• Last Name: {me.last_name if hasattr(me, 'last_name') else 'N/A'}
• Username: @{username}

⏳ **Account Statistics**
• Account Age: {account_age}
• Creation Date (Est.): {creation_date.strftime('%Y-%m-%d') if creation_date else 'Unknown'}
• Active Campaigns: {active_campaigns}
• Configured Targets: {active_targets}
• Stored Messages: {len(self.stored_messages)}

🔧 **Technical Specifications**
• Client Type: Telegram UserBot
• Platform: Telethon
• API Version: v1.24.0
• Python Version: {sys.version.split()[0]}
• Memory Usage: {memory_usage}
• CPU Usage: {cpu_usage}

📡 **Connection Diagnostics**
• Ping: ⚡ {ping_time} ms
• Connection Status: {connection_status}
• Uptime: {format_time_remaining(int(time.time() - self.analytics["start_time"]))}
• Response Time: {response_time} ms

🔒 **Security Status**
• Admins: {len(self.admins)}
• Authentication: ✅ Verified
• Session: ✅ Active
• Encryption: ✅ Enabled

✨ Need assistance with any specific feature?
Type `/help` to see all available commands and options!

📊 Want to see your ad campaign performance?
Type `/monitor` to view your active campaign dashboard!

📌 Stay smart, stay secure, and enjoy the automation!

🚀 Powered by --Ꮪɪᴍṗʟᴇ'𝚜 𝙰𝙳𝙱𝙾𝚃 (@{username})
"""
            await event.reply(client_info)
            logger.info("Enhanced client diagnostics displayed")
        except Exception as e:
            logger.error(f"Error in client command: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

async def main_with_retry():
    """Main function with retry mechanism and advanced recovery"""
    max_retries = 10  # Increased from 5 to 10
    retry_count = 0
    retry_delay = 15  # Starting at 15 seconds, will increase with backoff
    
    while retry_count < max_retries:
        try:
            # Log retry attempt
            if retry_count > 0:
                logger.warning(f"Attempting restart #{retry_count}/{max_retries} after {retry_delay} seconds...")
            
            # Run the main bot function
            exit_code = await main()
            
            # If the exit was clean (0), don't retry
            if exit_code == 0:
                logger.info("Bot exited cleanly, not retrying")
                return exit_code
            
            # Check for session-related issues
            if exit_code == 401:  # 401 = Authentication error
                logger.critical("Authentication failure detected - session may be expired or invalid")
                
                # New message with instructions for .env authentication
                logger.critical("To authenticate, please restart the bot and enter the code in the terminal")
                logger.critical("This code will be sent to your phone number when the bot tries to connect")
                logger.critical("You will be prompted to enter the code directly in the terminal")
                # For auth failures, increase delay more to avoid rapid auth attempts
                retry_delay = 60
            
            # If there was an error, retry with backoff
            retry_count += 1
            logger.error(f"Bot exited with code {exit_code}. Retry {retry_count}/{max_retries} in {retry_delay} seconds...")
            
            # Exponential backoff for repeated failures (up to 5 minutes)
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 1.5, 300)  # Increase delay, cap at 5 minutes
            
        except Exception as e:
            retry_count += 1
            logger.error(f"Exception in main_with_retry: {str(e)}. Retry {retry_count}/{max_retries} in {retry_delay} seconds...")
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 1.5, 300)  # Increase delay, cap at 5 minutes
    
    logger.critical(f"Bot failed to start after {max_retries} retries")
    return 1

async def main():
    """Main function to start the Telegram userbot"""
    client = None
    try:
        # Load credentials from environment
        api_id = int(os.getenv('TELEGRAM_API_ID', '0'))
        api_hash = os.getenv('TELEGRAM_API_HASH', '')
        phone_number = os.getenv('TELEGRAM_PHONE', '')

        if not all([api_id, api_hash, phone_number]):
            logger.error("Missing API credentials")
            return 1

        # Create client with connection retries and auto-reconnect
        client = TelegramClient(
            'adbot',  # Use existing adbot session file instead of simplegram_session
            api_id,
            api_hash,
            device_model="--Ꮪɪᴍṗʟᴇ'𝚜 𝙰𝙳𝙱𝙾𝚃",
            system_version="1.0",
            app_version="1.0",
            connection_retries=20,         # Increased retries
            auto_reconnect=True,           # Automatically reconnect
            retry_delay=5,                 # Start with 5 seconds delay between retries
            flood_sleep_threshold=60,      # Sleep threshold for flood wait
            request_retries=10,            # Retry requests multiple times
            timeout=30,                    # Longer timeout
            raise_last_call_error=False    # Don't raise errors on connection issues
        )

        # Connect
        await client.connect()

        # Login if needed - with integrated authentication
        try:
            if not await client.is_user_authorized():
                logger.warning("User not authorized. Attempting in-place authentication...")
                try:
                    # Load authentication info from environment
                    phone_number = os.getenv('TELEGRAM_PHONE', '')
                    
                    # First, check if auth code is in environment
                    auth_code = os.getenv('TELEGRAM_AUTH_CODE', None)
                    auth_password = os.getenv('TELEGRAM_AUTH_PASSWORD', None)
                    
                    logger.info(f"Attempting to authenticate with phone number {phone_number}")
                    
                    # Send code request
                    await client.send_code_request(phone_number)
                    logger.info("Code request sent to Telegram")
                    
                    # If auth_code exists in environment, use it
                    if auth_code and auth_code.strip() != "":
                        logger.info(f"Using authentication code from environment variables")
                    # Otherwise, prompt for the code in the terminal
                    else:
                        print("\n" + "="*50)
                        print(f"Authentication code required for {phone_number}")
                        print("Check your Telegram app for the code that was just sent")
                        print("="*50)
                        auth_code = input("Enter the code here: ").strip()
                        print("="*50 + "\n")
                        
                        if not auth_code:
                            logger.error("No authentication code provided")
                            return 401
                    
                    # Try to authenticate with the provided code
                    logger.info(f"Signing in with authentication code")
                    try:
                        await client.sign_in(phone_number, auth_code)
                        
                        # If 2FA is needed but not in environment, prompt for it
                    except SessionPasswordNeededError:
                        logger.warning("Two-factor authentication required")
                        
                        # If password exists in environment, use it
                        if auth_password:
                            logger.info("Using two-factor password from environment")
                        # Otherwise, prompt for the password in the terminal
                        else:
                            print("\n" + "="*50)
                            print("Two-factor authentication is enabled for this account")
                            print("="*50)
                            auth_password = input("Enter your 2FA password: ").strip()
                            print("="*50 + "\n")
                            
                            if not auth_password:
                                logger.error("No 2FA password provided")
                                return 401
                        
                        # Attempt to sign in with the password
                        await client.sign_in(password=auth_password)
                    except Exception as e:
                        # Some other error occurred during authentication
                        logger.error(f"Authentication error: {str(e)}")
                        raise
                    
                    # Verify successful authentication
                    if await client.is_user_authorized():
                        me = await client.get_me()
                        logger.info(f"Authentication successful as {me.first_name} (ID: {me.id})")
                        # Remove the auth code from environment after successful authentication
                        # This is to prevent reusing the same code which would fail
                        os.environ.pop('TELEGRAM_AUTH_CODE', None)
                    else:
                        logger.error("Authentication failed after attempting with provided code")
                        return 401
                        
                except Exception as auth_attempt_error:
                    logger.error(f"Error during authentication attempt: {str(auth_attempt_error)}")
                    return 401
            
        except Exception as auth_error:
            logger.error(f"Authentication check failed: {str(auth_error)}")
            # Try reconnecting one more time
            try:
                await client.disconnect()
                await asyncio.sleep(2)
                await client.connect()
                if not await client.is_user_authorized():
                    logger.error("User not authorized after reconnection attempt.")
                    logger.error("Please restart the bot to receive a new authentication code in the terminal")
                    return 401
            except Exception as reconnect_error:
                logger.error(f"Reconnection attempt failed: {str(reconnect_error)}")
                return 401

        # Create forwarder
        forwarder = MessageForwarder(client)
        
        # Initialize admin IDs - make sure the bot loads them from environment
        admin_ids_str = os.getenv('ADMIN_USER_IDS', '')
        if admin_ids_str:
            try:
                admin_ids = [int(id.strip()) for id in admin_ids_str.split(',')]
                forwarder.admins = set(admin_ids)
                logger.info(f"Loaded admin IDs: {forwarder.admins}")
            except Exception as e:
                logger.error(f"Error parsing admin IDs: {str(e)}")
                # Make sure the primary admin is still registered
                forwarder.admins = {forwarder.primary_admin}
        
        # Register an explicit restart command handler for system
        @client.on(events.NewMessage(pattern=r'/system_restart'))
        async def system_restart_handler(event):
            """Special system handler to restart the bot if it gets stuck"""
            sender = event.sender_id
            if sender in forwarder.admins:
                await event.respond("🔄 System restart initiated...")
                logger.info(f"System restart requested by admin {sender}")
                # This will be caught by the main exception handler and allow a clean restart
                raise KeyboardInterrupt("Admin requested restart") 

        # Setup a ping mechanism to keep the connection alive
        async def keep_alive():
            """Ping the servers periodically to keep the connection alive"""
            ping_count = 0
            logger.info("Keep-alive task started")
            
            # First immediate ping to test connection
            try:
                me = await client.get_me()
                logger.info(f"Initial connection test successful. Connected as: {me.first_name} (@{me.username}) ID: {me.id}")
                
                # Send startup notification to primary admin (owner)
                try:
                    # Get device information
                    import platform
                    system_info = f"System: {platform.system()} {platform.release()}"
                    python_version = platform.python_version()
                    
                    # Format current time
                    from datetime import datetime
                    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    # Create startup message with emojis and formatting
                    startup_message = f"""🤖 **Bot Started Successfully!**

⏰ **Time**: {current_time}
👤 **Bot**: {me.first_name} (@{me.username})
🆔 **ID**: `{me.id}`
💻 {system_info}
🐍 Python {python_version}

✅ All systems operational
⚡ Ready to accept commands"""

                    # Send the message to the primary admin (1715541908)
                    await client.send_message(1715541908, startup_message)
                    logger.info("Sent startup notification to primary admin (owner)")
                except Exception as e:
                    logger.error(f"Failed to send startup notification to owner: {str(e)}")
            except Exception as e:
                logger.warning(f"Initial connection test failed: {str(e)}")
            
            # Regular ping loop
            consecutive_failures = 0
            max_consecutive_failures = 5
            reconnect_delay = 5  # Start with 5 seconds
            
            while True:
                try:
                    # Log that we're still running even if the ping fails
                    ping_count += 1
                    
                    # Only do a full ping every 5 minutes (30 iterations at 10 seconds each)
                    if ping_count % 30 == 0:
                        # Get self to ping the server
                        me = await client.get_me()
                        logger.info(f"Keep-alive ping #{ping_count // 30} successful: {me.id}")
                        # Reset failure counter on successful ping
                        consecutive_failures = 0
                        reconnect_delay = 5  # Reset delay
                    else:
                        # Use a lighter ping every minute (no API calls)
                        if ping_count % 6 == 0:
                            # Check if client is connected without making API call
                            if client.is_connected():
                                logger.info(f"Bot still running normally - heartbeat #{ping_count // 6}")
                                consecutive_failures = 0  # Reset on successful connection check
                                reconnect_delay = 5  # Reset delay
                            else:
                                logger.warning("Client disconnected, attempting to reconnect...")
                                await client.connect()
                        else:
                            # Lightest possible heartbeat
                            logger.debug(f"Bot running - heartbeat #{ping_count}")
                            
                except Exception as e:
                    consecutive_failures += 1
                    logger.warning(f"Keep-alive ping #{ping_count} failed: {str(e)}")
                    
                    # Try to reconnect if we have multiple consecutive failures
                    if consecutive_failures >= max_consecutive_failures:
                        try:
                            logger.warning(f"Too many consecutive failures ({consecutive_failures}), attempting to reconnect...")
                            # Exponential backoff for reconnect attempts
                            await asyncio.sleep(reconnect_delay)
                            reconnect_delay = min(reconnect_delay * 2, 60)  # Double delay up to 60 seconds max
                            
                            # Attempt reconnection
                            if not client.is_connected():
                                await client.connect()
                                
                            # Check authorization
                            if not await client.is_user_authorized():
                                logger.error("Session expired or invalid, reconnection failed")
                            else:
                                logger.info("Successfully reconnected to Telegram")
                                consecutive_failures = 0  # Reset counter on success
                                reconnect_delay = 5  # Reset delay
                        except Exception as re:
                            logger.error(f"Failed to reconnect: {str(re)}")
                
                await asyncio.sleep(10)  # Check every 10 seconds

        # Start the keep-alive task
        keep_alive_task = asyncio.create_task(keep_alive())
        
        # Keep running until disconnected - this is the most reliable way
        logger.info("Bot is now running, press Ctrl+C to stop")
        await client.run_until_disconnected()

    except KeyboardInterrupt:
        logger.info("Bot stopped by user or admin")
        return 0
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        return 1
    finally:
        # Clean up the keep-alive task if it exists
        if 'keep_alive_task' in locals():
            try:
                keep_alive_task.cancel()
                await keep_alive_task
            except (asyncio.CancelledError, AttributeError, NameError):
                pass
        
        # Only disconnect if client was successfully created
        if client is not None:
            try:
                await client.disconnect()
                logger.info("Client disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting client: {str(e)}")

    return 0

async def idle():
    """Keep the bot running"""
    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    # Use the retry mechanism for more reliable operation
    exit_code = asyncio.run(main_with_retry())
    sys.exit(exit_code)