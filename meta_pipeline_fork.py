# New forked version: Modularized Meta Ads Data Pipeline

import os
import aiohttp
import asyncio
import json
from aiohttp_retry import RetryClient, ExponentialRetry
from datetime import datetime, timedelta
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

# ========== CONFIG ==========
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
BASE_URL = "https://graph.facebook.com/v22.0"

# ========== AD ACCOUNTS (MOVED TO ENV OR CONFIG) ==========
AD_ACCOUNTS = json.loads(os.environ.get("AD_ACCOUNTS_JSON", "[]"))

# ========== API HELPERS ==========
async def fetch_url(client, url, params):
    async with client.get(url, params=params) as res:
        if res.status != 200:
            text = await res.text()
            raise Exception(f"{res.status}: {text}")
        return await res.json()

async def fetch_insights(client, ad_account_id, since, until):
    url = f"{BASE_URL}/act_{ad_account_id}/insights"
    params = {
        "access_token": ACCESS_TOKEN,
        "fields": "date_start,ad_id,impressions,clicks,spend,actions",
        "level": "ad",
        "time_range": json.dumps({"since": since, "until": until}),
        "limit": 50
    }
    return await fetch_url(client, url, params)

# ========== PROCESSING ==========
def parse_actions(actions):
    def safe_sum(action_type):
        return sum(int(a.get("value", 0)) for a in actions if a.get("action_type") == action_type)
    return safe_sum("lead"), safe_sum("purchase")

def calculate_metrics(row, leads, purchases):
    impressions = int(row.get("impressions", 0))
    clicks = int(row.get("clicks", 0))
    spend = float(row.get("spend", 0))
    ctr = (clicks / impressions * 100) if impressions else 0
    cpc = (spend / clicks) if clicks else 0
    cpa = (spend / (leads + purchases)) if (leads + purchases) else 0
    return impressions, clicks, spend, ctr, cpc, cpa

# ========== SUPABASE ==========
def upsert_to_supabase(account_id, business_name, row, metrics):
    data = {
        "account_id": account_id,
        "business_name": business_name,
        "date": row.get("date_start"),
        "ad_id": row.get("ad_id"),
        "impressions": metrics[0],
        "clicks": metrics[1],
        "spend": metrics[2],
        "ctr": metrics[3],
        "cpc": metrics[4],
        "cpa": metrics[5],
        "leads": metrics[6],
        "purchases": metrics[7],
        "flagged": False,
        "flagged_reason": None
    }
    supabase.table("meta_ads_monitoring").upsert(data, on_conflict=["account_id", "date", "ad_id"]).execute()

# ========== LOOP ==========
async def process_ad_account(business_name, account_id):
    retry = ExponentialRetry(attempts=5)
    async with RetryClient(retry_options=retry) as client:
        today = datetime.today().date()
        since = (today - timedelta(days=7)).isoformat()
        until = today.isoformat()
        try:
            insights = await fetch_insights(client, account_id, since, until)
            for row in insights.get("data", []):
                leads, purchases = parse_actions(row.get("actions", []))
                metrics = calculate_metrics(row, leads, purchases)
                upsert_to_supabase(account_id, business_name, row, (*metrics, leads, purchases))
            print(f"✅ {business_name} done")
        except Exception as e:
            print(f"⚠️ {business_name} failed: {e}")

# ========== MAIN ==========
async def main():
    tasks = [process_ad_account(name, acct_id) for name, acct_id in AD_ACCOUNTS]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
