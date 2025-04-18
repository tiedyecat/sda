# google_chat_notifier.py

import os
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client
import requests

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
WEBHOOK_URL = os.environ.get("GOOGLE_CHAT_WEBHOOK")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_flagged_summaries():
    today = datetime.today().strftime("%Y-%m-%d")
    response = supabase.table("meta_ads_monitoring") \
        .select("business_name, flagged_reason, ai_summary") \
        .eq("flagged", True) \
        .eq("date", today) \
        .execute()
    return response.data or []

def send_to_google_chat():
    if not WEBHOOK_URL:
        print("❌ Missing GOOGLE_CHAT_WEBHOOK in .env")
        return

    flagged = fetch_flagged_summaries()
    if not flagged:
        print("✅ No flagged summaries to send today.")
        return

    summary_lines = [
        f"📌 *{row['business_name']}* — {row['flagged_reason']}: {row['ai_summary']}"
        for row in flagged
    ]

    payload = {"text": "\n".join(summary_lines)}

    try:
        response = requests.post(WEBHOOK_URL, json=payload)
        if response.status_code == 200:
            print("📨 Sent flagged summaries to Google Chat")
        else:
            print(f"❌ Google Chat Error: {response.status_code}")
    except Exception as e:
        print(f"❌ Exception during Google Chat send: {e}")

if __name__ == "__main__":
    send_to_google_chat()
