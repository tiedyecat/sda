# google_chat_notifier.py
import os
from textwrap import shorten

import requests
from dotenv import load_dotenv
from supabase import Client, create_client

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
WEBHOOK_URL = os.environ.get("GOOGLE_CHAT_WEBHOOK")

if not (SUPABASE_URL and SUPABASE_KEY):
    raise EnvironmentError("❌ SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY missing")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_all_summaries() -> list[dict]:
    """Return every row in the table (no filters)."""
    resp = (
        supabase.table("meta_ads_monitoring")
        .select("business_name, flagged_reason, ai_summary")
        .execute()
    )
    return resp.data or []


def format_row(row: dict) -> str:
    """Format one row for Google Chat."""
    biz = row["business_name"]
    reason = row.get("flagged_reason")
    summary = shorten(row.get("ai_summary", ""), width=500, placeholder="…")

    return (
        f"📌 *{biz}* — {reason}: {summary}"
        if reason
        else f"📌 *{biz}*: {summary}"
    )


def chunk_lines(lines: list[str], max_chars: int = 4000) -> list[str]:
    """Split large message into ≤4000‑char chunks for Chat."""
    chunks, current = [], ""
    for line in lines:
        if len(current) + len(line) + 1 > max_chars:
            chunks.append(current.rstrip())
            current = ""
        current += line + "\n"
    if current:
        chunks.append(current.rstrip())
    return chunks


def send_to_google_chat() -> None:
    if not WEBHOOK_URL:
        print("❌ Missing GOOGLE_CHAT_WEBHOOK in .env")
        return

    rows = fetch_all_summaries()
    if not rows:
        print("✅ No summaries found in meta_ads_monitoring.")
        return

    message_chunks = chunk_lines([format_row(r) for r in rows])

    for chunk in message_chunks:
        payload = {"text": chunk}
        try:
            resp = requests.post(WEBHOOK_URL, json=payload, timeout=10)
            resp.raise_for_status()
            print("📨 Sent summaries to Google Chat")
        except requests.RequestException as err:
            print(f"❌ Google Chat Error: {err}")


if __name__ == "__main__":
    send_to_google_chat()
