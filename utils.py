class AdminHandler:
    @staticmethod
    async def verify_admin(event, admin_ids):
        return event.sender_id in admin_ids

async def safe_execution(func):
    try:
        return await func
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return None

def get_random_delay(min_delay=5, max_delay=15):
    import random
    return random.randint(min_delay, max_delay)

async def get_chat_display_name(client, chat_id):
    chat = await client.get_entity(chat_id)
    return chat.title if hasattr(chat, 'title') else chat.first_name