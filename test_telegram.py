import os
import asyncio
from dotenv import load_dotenv
from telegram import Bot

async def main():
    load_dotenv()
    TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

    if not TOKEN or not CHAT_ID:
        raise SystemExit("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env")

    bot = Bot(token=TOKEN)
    resp = await bot.send_message(
        chat_id=int(CHAT_ID),
        text="✅ Test alert from crypto-gainers-scanner. If you see this, Telegram works."
    )
    print("Message sent. message_id:", resp.message_id)

if __name__ == "__main__":
    asyncio.run(main())
