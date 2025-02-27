import asyncio
from telethon.sync import TelegramClient
from telethon import errors

class TelegramForwarder:
    def __init__(self, api_id, api_hash, phone_number):
        self.api_id = api_id
        self.api_hash = api_hash
        self.phone_number = phone_number
        self.client = TelegramClient(f'session_{phone_number}', api_id, api_hash)

    async def connect_client(self):
        await self.client.connect()
        if not await self.client.is_user_authorized():
            await self.client.send_code_request(self.phone_number)
            try:
                await self.client.sign_in(self.phone_number, input('Enter the code: '))
            except errors.rpcerrorlist.SessionPasswordNeededError:
                password = input('Two-step verification enabled. Enter password: ')
                await self.client.sign_in(password=password)

    async def list_chats(self):
        await self.connect_client()
        dialogs = await self.client.get_dialogs()
        
        with open(f"chats_of_{self.phone_number}.txt", "w", encoding="utf-8") as file:
            for dialog in dialogs:
                print(f"Chat ID: {dialog.id}, Title: {dialog.title}")
                file.write(f"Chat ID: {dialog.id}, Title: {dialog.title}\n")

        print("Chats list saved to file!")

    async def forward_messages(self, source_chat_id, destination_chat_ids, keywords):
        await self.connect_client()
        last_message_id = (await self.client.get_messages(source_chat_id, limit=1))[0].id

        while True:
            messages = await self.client.get_messages(source_chat_id, min_id=last_message_id, limit=None)

            for message in reversed(messages):
                if keywords:
                    if message.text and any(keyword in message.text.lower() for keyword in keywords):
                        for dest_chat in destination_chat_ids:
                            await self.client.send_message(dest_chat, message.text)
                            print(f"Forwarded to {dest_chat}")
                else:
                    for dest_chat in destination_chat_ids:
                        await self.client.send_message(dest_chat, message.text)
                        print(f"Forwarded to {dest_chat}")

                last_message_id = max(last_message_id, message.id)

            await asyncio.sleep(5)

def read_credentials():
    try:
        with open("credentials.txt", "r") as file:
            api_id, api_hash, phone_number = [line.strip() for line in file.readlines()]
            return api_id, api_hash, phone_number
    except FileNotFoundError:
        return None, None, None

def write_credentials(api_id, api_hash, phone_number):
    with open("credentials.txt", "w") as file:
        file.write(f"{api_id}\n{api_hash}\n{phone_number}\n")

async def main():
    api_id, api_hash, phone_number = read_credentials()

    if not api_id or not api_hash or not phone_number:
        api_id = input("Enter your API ID: ")
        api_hash = input("Enter your API Hash: ")
        phone_number = input("Enter your phone number: ")
        write_credentials(api_id, api_hash, phone_number)

    forwarder = TelegramForwarder(api_id, api_hash, phone_number)

    print("1. List Chats\n2. Forward Messages")
    choice = input("Enter choice: ")

    if choice == "1":
        await forwarder.list_chats()
    elif choice == "2":
        source_chat_id = int(input("Enter source chat ID: "))
        destination_chat_ids = list(map(int, input("Enter destination chat IDs (comma-separated): ").split(",")))
        keywords = input("Enter keywords (comma-separated, leave blank for all): ").split(",") if input().strip() else []
        await forwarder.forward_messages(source_chat_id, destination_chat_ids, keywords)
    else:
        print("Invalid choice")

if __name__ == "__main__":
    asyncio.run(main())
    
