import os
import asyncio
import sqlite3
from datetime import datetime, timedelta
from telethon import TelegramClient, events
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Telegram API Credentials
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

# Create the bot client
client = TelegramClient('adbot', API_ID, API_HASH).start(bot_token=BOT_TOKEN)

# Database Setup
DB_FILE = "adbot.db"
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS subscriptions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER UNIQUE,
        expiry_date TEXT,
        delay INTEGER DEFAULT 5
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS targets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        group TEXT
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS ads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        message TEXT
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS global_settings (
        id INTEGER PRIMARY KEY,
        global_delay INTEGER DEFAULT 5
    )
""")
cursor.execute("INSERT OR IGNORE INTO global_settings (id, global_delay) VALUES (1, 5)")
conn.commit()
conn.close()

# Function to check subscription status
def is_subscription_active(user_id):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT expiry_date FROM subscriptions WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    conn.close()
    if result:
        expiry_date = datetime.strptime(result[0], "%Y-%m-%d %H:%M:%S")
        return expiry_date > datetime.now()
    return False

# Function to send subscription reminders
async def send_reminders():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, expiry_date FROM subscriptions")
    subscriptions = cursor.fetchall()
    conn.close()
    
    for user_id, expiry_date in subscriptions:
        expiry = datetime.strptime(expiry_date, "%Y-%m-%d %H:%M:%S")
        remaining_days = (expiry - datetime.now()).days
        if remaining_days in [3, 1]:  # Send reminders 3 days and 1 day before expiry
            await client.send_message(user_id, f"⚠️ Reminder: Your subscription expires in {remaining_days} days! Please renew to continue your service.")
            await client.send_message(ADMIN_ID, f"🔔 Subscription for {user_id} is expiring in {remaining_days} days.")

# Command to add an ad message
@client.on(events.NewMessage(pattern='/addad'))
async def add_ad(event):
    message = event.message.text.split(" ", 1)[1] if len(event.message.text.split(" ", 1)) > 1 else None
    if not message:
        await event.reply("Usage: /addad <ad message>")
        return
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO ads (user_id, message) VALUES (?, ?)", (event.sender_id, message))
    conn.commit()
    conn.close()
    await event.reply("✅ Ad message added!")

# Command to list all ad messages
@client.on(events.NewMessage(pattern='/listads'))
async def list_ads(event):
    user_id = event.sender_id
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT message FROM ads WHERE user_id = ?", (user_id,))
    ads = [row[0] for row in cursor.fetchall()]
    conn.close()
    ad_list = '\n'.join(ads) if ads else "No ads set. Use /addad to add messages."
    await event.reply(f"📢 Your Ad Messages:\n{ad_list}")

# Command to get bot stats (admin only)
@client.on(events.NewMessage(pattern='/stats'))
async def stats(event):
    if event.sender_id != ADMIN_ID:
        return
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM subscriptions")
    subs_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM ads")
    ads_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM targets")
    targets_count = cursor.fetchone()[0]
    conn.close()
    stats_text = f"📊 *Bot Statistics:*\n\n👥 Subscribed Users: {subs_count}\n📢 Ads Created: {ads_count}\n🎯 Target Groups: {targets_count}"
    await event.reply(stats_text)

# Run the bot and schedule reminders
async def main():
    while True:
        await send_reminders()
        await asyncio.sleep(86400)  # Run once per day

client.loop.create_task(main())
print("✅ Bot is running...")
client.run_until_disconnected()
