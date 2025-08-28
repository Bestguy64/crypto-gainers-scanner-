# check_env.py
from dotenv import load_dotenv
import os, sys
load_dotenv()
print("TELEGRAM_BOT_TOKEN present:", bool(os.getenv("TELEGRAM_BOT_TOKEN")))
print("TELEGRAM_CHAT_ID present:", os.getenv("TELEGRAM_CHAT_ID"))
