import os
import aiohttp
import asyncio
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client
import nest_asyncio
from aiohttp_retry import RetryClient, ExponentialRetry

load_dotenv()
nest_asyncio.apply()

# ========== CONFIGURATION ==========
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
BASE_URL = "https://graph.facebook.com/v22.0"
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========== LOAD AD ACCOUNT MAPPING ==========
with open("ad_accounts_by_team.json") as f:
    AD_ACCOUNT_MAP = json.load(f)

# Flattened account list for now (until we implement team-based views)
AD_ACCOUNTS = [
    (business_name, account_id)
    for team_accounts in AD_ACCOUNT_MAP.values()
    for business_name, account_id in team_accounts
]

# ========== ASYNC UTILITY ==========
async def fetch_url(client, url, params, max_retries=5):
    retries = 0
    while retries < max_retries:
        await asyncio.sleep(1)
        async with client.get(url, params=params) as response:
            if response.status == 200:
                return await response.json()
            else:
                text = await response.text()
                if response.status == 400 and "too many calls" in text.lower():
                    retries += 1
                    await asyncio.sleep(30 * retries)
                else:
                    raise Exception(f"Error {response.status}: {text}")
    raise Exception("Max retries reached.")

# ========== FETCH FUNCTIONS ==========
async def fetch_ads(client, ad_account_id):
    url = f"{BASE_URL}/act_{ad_account_id}/ads"
    params = {
        "access_token": ACCESS_TOKEN,
        "fields": "id,name,adset_id,campaign_id",
        "limit": 100
    }
    ads = []
    while url:
        data = await fetch_url(client, url, params)
        ads.extend(data.get("data", []))
        url = data.get("paging", {}).get("next")
    return ads

async def fetch_campaigns(client, ad_account_id):
    url = f"{BASE_URL}/act_{ad_account_id}/campaigns"
    params = {
        "access_token": ACCESS_TOKEN,
        "fields": "id,name,daily_budget",
        "limit": 100
    }
    data = await fetch_url(client, url, params)
    return {c["id"]: {
        "campaign_name": c["name"],
        "daily_budget": float(c.get("daily_budget", 0)) / 1_000_000
    } for c in data.get("data", [])}

async def fetch_insights(client, ad_account_id, time_range):
    url = f"{BASE_URL}/act_{ad_account_id}/insights"
    params = {
        "access_token": ACCESS_TOKEN,
        "fields": "date_start,date_stop,ad_id,impressions,clicks,ctr,spend,frequency,actions",
        "level": "ad",
        "time_range": json.dumps(time_range),
        "limit": 50
    }
    return await fetch_url(client, url, params)

# ========== PROCESS + SAVE ==========
async def process_and_save(ad_account_id, time_range, business_name):
    retry_options = ExponentialRetry(attempts=5)
    async with RetryClient(raise_for_status=False, retry_options=retry_options) as client:
        insights = await fetch_insights(client, ad_account_id, time_range)
        ads = await fetch_ads(client, ad_account_id)
        campaigns = await fetch_campaigns(client, ad_account_id)

        ad_map = {ad["id"]: ad for ad in ads}
        rows_to_insert = []

        for insight in insights.get("data", []):
            ad_data = ad_map.get(insight.get("ad_id"), {})
            ad_name = ad_data.get("name", "Unknown")
            campaign_id = ad_data.get("campaign_id")
            campaign_info = campaigns.get(campaign_id, {})
            campaign_name = campaign_info.get("campaign_name")
            daily_budget = campaign_info.get("daily_budget")

            impressions = int(insight.get("impressions", 0))
            clicks = int(insight.get("clicks", 0))
            spend = float(insight.get("spend", 0))
            frequency = float(insight.get("frequency", 0))
            ctr = (clicks / impressions * 100) if impressions else 0
            cpc = (spend / clicks) if clicks else 0
            cpm = (spend / impressions * 1000) if impressions else 0

            actions = insight.get("actions", [])
            leads = sum(
                int(action.get("value", 0))
                for action in actions
                if action.get("action_type") == "lead"
            )
            purchases = sum(
                int(action.get("value", 0))
                for action in actions
                if action.get("action_type") == "purchase"
            )
            conversions = leads + purchases
            cpa = (spend / conversions) if conversions else 0

            rows_to_insert.append({
                "account_id": ad_account_id,
                "business_name": business_name,
                "ad_name": ad_name,
                "campaign_id": campaign_id,
                "campaign_name": campaign_name,
                "impressions": impressions,
                "clicks": clicks,
                "ctr": ctr,
                "spend": spend,
                "daily_budget": daily_budget,
                "cpc": cpc,
                "cpm": cpm,
                "frequency": frequency,
                "conversions": conversions,
                "leads": leads,
                "purchases": purchases,
                "cpa": cpa,
                "date": insight.get("date_start"),
                "flagged": False,
                "flagged_reason": None
            })

        supabase.table("meta_ads_monitoring").upsert(
            rows_to_insert,
            on_conflict=["account_id", "date"]
        ).execute()
        print(f"✅ Inserted {len(rows_to_insert)} rows for {business_name}")

# ========== MAIN ==========
async def main():
    today = datetime.today()
    past_30_days = today - timedelta(days=30)
    time_range = {
        "since": past_30_days.strftime("%Y-%m-%d"),
        "until": today.strftime("%Y-%m-%d")
    }

    for i, (business_name, ad_account_id) in enumerate(AD_ACCOUNTS):
        print(f"\n🔄 [{i+1}/{len(AD_ACCOUNTS)}] Processing: {business_name} (act_{ad_account_id})")
        try:
            await process_and_save(ad_account_id, time_range, business_name)
        except Exception as e:
            print(f"⚠️ Skipping {business_name} due to error: {e}")
        await asyncio.sleep(3)

if __name__ == "__main__":
    asyncio.run(main())
