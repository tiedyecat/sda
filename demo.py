"""
Meta Ads ⇒ Supabase daily ingestion
-----------------------------------
• Requires environment variables:
    ACCESS_TOKEN, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
• Requires a JSON file mapping teams → [(business_name, ad_account_id), …]:
    ad_accounts_by_team.json   (change the path below if yours is different)
• Table meta_ads_monitoring MUST have a UNIQUE index on
    (account_id, ad_id, date)  – run the SQL block below once:

    -- add ad_id column if you don’t already have it
    alter table meta_ads_monitoring
        add column if not exists ad_id text;

    -- guarantee one row per ad per account per day
    create unique index if not exists meta_ads_monitoring_uidx
        on meta_ads_monitoring (account_id, ad_id, date);
"""

import os
import aiohttp
import asyncio
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client, SupabaseException
import nest_asyncio
from aiohttp_retry import RetryClient, ExponentialRetry

load_dotenv()
nest_asyncio.apply()

# ========== CONFIGURATION ==========
ACCESS_TOKEN   = os.environ.get("ACCESS_TOKEN")                # <-- REQUIRED
BASE_URL       = "https://graph.facebook.com/v22.0"
SUPABASE_URL   = os.environ["SUPABASE_URL"]                    # <-- REQUIRED
SUPABASE_KEY   = os.environ["SUPABASE_SERVICE_ROLE_KEY"]       # <-- REQUIRED
AD_ACCOUNT_JSON_PATH = "ad_accounts_by_team.json"              # <-- UPDATE IF NEEDED

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========== LOAD AD ACCOUNT MAPPING ==========
with open(AD_ACCOUNT_JSON_PATH) as f:
    AD_ACCOUNT_MAP = json.load(f)

AD_ACCOUNTS = [
    (business_name, account_id)
    for team_accounts in AD_ACCOUNT_MAP.values()
    for business_name, account_id in team_accounts
]

# ========== ASYNC HTTP WRAPPER ==========
async def fetch_url(client: aiohttp.ClientSession, url: str, params: dict, max_retries: int = 5):
    retries = 0
    while retries < max_retries:
        await asyncio.sleep(1)
        try:
            async with client.get(url, params=params) as resp:
                if resp.status == 200:
                    return await resp.json()
                text = await resp.text()
                print(f"Meta API Error {resp.status}: {text}")
                if resp.status in {400, 429} and "too many calls" in text.lower():
                    retries += 1
                    await asyncio.sleep(30 * retries)
                else:
                    raise Exception(f"Meta API failed: {resp.status}: {text}")
        except Exception as e:
            print(f"❌ fetch_url exception: {e}")
            retries += 1
            await asyncio.sleep(5 * retries)
    raise Exception("Max retries reached for Meta API call")

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
        params = {}    # subsequent pages already include the token in `url`
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
        "time_range[since]": time_range["since"],
        "time_range[until]": time_range["until"],
        "time_increment": 1,                  # daily breakdown
        "limit": 50
    }
    return await fetch_url(client, url, params)

# ========== PROCESS + UPSERT ==========
async def process_and_save(ad_account_id: str, time_range: dict, business_name: str):
    retry_opts = ExponentialRetry(attempts=5)
    async with RetryClient(raise_for_status=False, retry_options=retry_opts) as client:
        try:
            insights   = await fetch_insights(client, ad_account_id, time_range)
            ads        = await fetch_ads(client, ad_account_id)
            campaigns  = await fetch_campaigns(client, ad_account_id)
        except Exception as e:
            print(f"❌ API fetch failed for {business_name}: {e}")
            return

        ad_map = {ad["id"]: ad for ad in ads}
        rows   = []

        for ins in insights.get("data", []):
            try:
                ad_id   = ins.get("ad_id")
                ad_data = ad_map.get(ad_id, {})
                camp_id = ad_data.get("campaign_id")
                camp    = campaigns.get(camp_id, {})

                impressions = int(ins.get("impressions", 0))
                clicks      = int(ins.get("clicks", 0))
                spend       = float(ins.get("spend", 0))
                frequency   = float(ins.get("frequency", 0))

                ctr = (clicks / impressions * 100) if impressions else 0
                cpc = (spend / clicks) if clicks else 0
                cpm = (spend / impressions * 1000) if impressions else 0

                acts      = ins.get("actions", [])
                leads     = sum(int(a.get("value", 0)) for a in acts if a.get("action_type") == "lead")
                purchases = sum(int(a.get("value", 0)) for a in acts if a.get("action_type") == "purchase")
                conv      = leads + purchases
                cpa       = (spend / conv) if conv else 0

                rows.append({
                    "account_id":     ad_account_id,
                    "business_name":  business_name,
                    "ad_id":          ad_id,
                    "ad_name":        ad_data.get("name", "Unknown"),
                    "campaign_id":    camp_id,
                    "campaign_name":  camp.get("campaign_name"),
                    "impressions":    impressions,
                    "clicks":         clicks,
                    "ctr":            ctr,
                    "spend":          spend,
                    "daily_budget":   camp.get("daily_budget"),
                    "cpc":            cpc,
                    "cpm":            cpm,
                    "frequency":      frequency,
                    "conversions":    conv,
                    "leads":          leads,
                    "purchases":      purchases,
                    "cpa":            cpa,
                    "date":           ins.get("date_start"),
                    "flagged":        False,
                    "flagged_reason": None
                })
            except Exception as row_err:
                print(f"⚠️ Row error ({business_name}): {row_err}")

        if not rows:
            print(f"ℹ️ No data for {business_name}")
            return

        try:
            supabase.table("meta_ads_monitoring") \
                    .upsert(rows, on_conflict=["account_id", "ad_id", "date"]) \
                    .execute()
            print(f"✅ {business_name}: inserted/updated {len(rows)} rows")
        except SupabaseException as se:
            print(f"❌ Supabase ({business_name}): {se.message}")
        except Exception as e:
            print(f"❌ General supabase error ({business_name}): {e}")

# ========== MAIN ==========
async def main():
    today = datetime.utcnow().date()
    since = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    until = today.strftime("%Y-%m-%d")
    time_range = {"since": since, "until": until}

    for idx, (biz, acc) in enumerate(AD_ACCOUNTS, 1):
        print(f"\n🔄 [{idx}/{len(AD_ACCOUNTS)}] {biz} (act_{acc})")
        try:
            await process_and_save(acc, time_range, biz)
        except Exception as e:
            print(f"⚠️ Skipping {biz}: {e}")
        await asyncio.sleep(3)   # gentle pacing

if __name__ == "__main__":
    asyncio.run(main())

