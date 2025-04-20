# chat-notifier.py
import os
import time
from typing import List

import requests
from dotenv import load_dotenv
from supabase import Client, create_client

# ─── ENV & CONSTANTS ────────────────────────────────────────────────
load_dotenv()

SUPABASE_URL   = os.environ["SUPABASE_URL"]
SUPABASE_KEY   = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
WEBHOOK_URL    = os.environ.get("GOOGLE_CHAT_WEBHOOK")

BATCH_SIZE     = 1_000   # Supabase rows per page
CHAT_LIMIT     = 4_000   # Max chars per Google Chat message
MAX_RETRIES    = 5       # Tries on HTTP 429
BASE_WAIT      = 2       # Initial back‑off seconds
# ────────────────────────────────────────────────────────────────────

if not (SUPABASE_URL and SUPABASE_KEY):
    raise EnvironmentError(
        "❌ SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY missing from environment"
    )

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ─── DATA FETCHING ──────────────────────────────────────────────────
def fetch_all_rows() -> List[dict]:
    rows, start = [], 0
    while True:
        end = start + BATCH_SIZE - 1
        resp = (
            supabase.table("meta_ads_monitoring")
            .select("business_name, flagged_reason, ai_summary")
            .range(start, end)
            .execute()
        )
        page = resp.data or []
        rows.extend(page)
        if len(page) < BATCH_SIZE:
            break
        start += BATCH_SIZE
    return rows


# ─── HELPERS ────────────────────────────────────────────────────────
def format_row(r: dict) -> str:
    biz = r.get("business_name", "Unknown Business")
    reason = r.get("flagged_reason") or ""
    summary = (r.get("ai_summary") or "").strip()
    return f"📌 *{biz}* — {reason}: {summary}" if reason else f"📌 *{biz}*: {summary}"


def chunk_lines(lines: List[str], limit: int = CHAT_LIMIT) -> List[str]:
    chunks, current = [], ""
    for line in lines:
        if len(current) + len(line) + 1 > limit:
            chunks.append(current.rstrip())
            current = ""
        current += line + "\n"
    if current:
        chunks.append(current.rstrip())
    return chunks


def post_with_retry(payload: dict) -> None:
    wait, tries = BASE_WAIT, 0
    while True:
        try:
            resp = requests.post(WEBHOOK_URL, json=payload, timeout=10)
            if resp.status_code == 429:
                tries += 1
                if tries > MAX_RETRIES:
                    raise RuntimeError("Exceeded retry limit on 429 errors")
                retry_after = int(resp.headers.get("Retry-After", 0))
                sleep_time = retry_after if retry_after else wait
                print(f"⏳ 429 received, sleeping {sleep_time}s then retrying…")
                time.sleep(sleep_time)
                wait *= 2  # exponential back‑off
                continue
            resp.raise_for_status()
            return
        except requests.exceptions.RequestException as err:
            raise RuntimeError(f"Google Chat send failure: {err}") from err


# ─── MAIN ───────────────────────────────────────────────────────────
def send_to_google_chat() -> None:
    if not WEBHOOK_URL:
        print("❌ Missing GOOGLE_CHAT_WEBHOOK in .env")
        return

    rows = fetch_all_rows()
    if not rows:
        print("✅ meta_ads_monitoring is empty.")
        return

    messages = chunk_lines([format_row(r) for r in rows])
    print(f"➡️  Sending {len(rows)} summaries in {len(messages)} batched message(s)…")

    for idx, msg in enumerate(messages, start=1):
        print(f"  • Posting batch {idx}/{len(messages)}")
        post_with_retry({"text": msg})

    print("📨 Done.")


if __name__ == "__main__":
    send_to_google_chat()
