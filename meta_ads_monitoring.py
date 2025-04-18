import os
import aiohttp
import asyncio
from aiohttp_retry import RetryClient, ExponentialRetry
from datetime import datetime, timedelta
import json
from supabase import create_client, Client
import nest_asyncio

nest_asyncio.apply()

# ========== CONFIGURATION ==========
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN", "your-facebook-access-token")
BASE_URL = "https://graph.facebook.com/v22.0"

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========== ACCOUNT LIST ==========
AD_ACCOUNTS = [
    ("425 Fitness - Bothell", "15966930"),
    ("425 Fitness - Issaquah", "1791553457827738"),
    ("425 Fitness - Redmond", "1796703287312755"),
    ("Absolute Recomp", "581107862007716"),
    ("American Barbell Clubs", "768011641837919"),
    ("CAC", "430492915277766"),
    ("Club Fit", "2978635188937841"),
    ("Club Fitness", "3860891150613766"),
    ("Curl - Diamond Bar", "1582311701978540"),
    ("Curl - Newport Beach", "439964580942138"),
    ("Curl - Riverside", "260102209680906"),
    ("Curl - Westminster", "3448117208769669"),
    ("Curl - Yorba Linda", "742929074164598"),
    ("Defined Fitness (NEW)", "778779483157578"),
    ("F19 - Arroyo Grande", "1129161167635243"),
    ("F19 - Bellflower", "391285962931791"),
    ("F19 - Brea", "154524177522964"),
    ("F19 - Buena Park", "733550457736583"),
    ("F19 - Chino", "1246849129480029"),
    ("F19 - Gardena", "3288589024796677"),
    ("F19 - Huntington Beach", "465905625092536"),
    ("F19 - Maywood", "353825873625518"),
    ("F19 - Menifee", "1110239503212814"),
    ("F19 - Milpitas", "162039226723214"),
    ("F19 - Mission Viejo", "231198712644144"),
    ("F19 - Murrieta", "2173769466141513"),
    ("F19 - Oceanside", "908024833582635"),
    ("F19 - Orange", "611113991033078"),
    ("F19 - Pico Rivera", "731074285152548"),
    ("F19 - San Clemente", "876251070119483"),
    ("F19 - San Mateo", "1960093251005052"),
    ("F19 - Temecula", "1669572166835145"),
    ("F19 - Wildomar", "997242517906978"),
    ("LEVEL Fitness Pelham", "820514018691762"),
    ("LEVEL Fitness Thornwood", "1171062777200647"),
    ("LEVEL Fitness Yorktown", "339863677199288"),
    ("LEVEL Somers", "1102554120713582"),
    ("Focus Fitness Club", "493003589707254"),
    ("Gentry", "1116948612833379"),
    ("GG Quebec - Laval", "4240296322862443"),
    ("GG Quebec - VSL", "514167960371052"),
    ("GymBox", "21592172220878916"),
    ("Hidden", "2199284363711953"),
    ("Henry's Gymnasium - Capitol Hill", "3245179648899298"),
    ("inMotion", "1315711728953574"),
    ("Maximum", "210589141110391"),
    ("Physiq", "797120043955249"),
    ("Powerhouse Bethlehem", "458742165022754"),
    ("Powerhouse East Lansing", "169457014390081"),
    ("Powerhouse Milford", "240093310350087"),
    ("Powerhouse Northville", "369111118813510"),
    ("Powerhouse Novi & West Bloomfield", "2439107192881819"),
    ("Powerhouse Saline", "273950090531648"),
    ("Powerhouse Shelby", "356127662270365"),
    ("Powerhouse South Lyon", "640642956610407"),
    ("Powerhouse Southfield", "675546043390548"),
    ("Powerhouse St. Clair Shores", "254332192962492"),
    ("Powerhouse Ypsilanti", "701871214003172"),
    ("PUMP24 Harrisonburg", "1113809299872673"),
    ("PUMP24 South Riding", "555471603635789"),
    ("Olympus Fitness", "817510707050988"),
    ("BodyFuel Fitness", "153306196665189"),
    ("Afterburn Fitness 1", "983503355690314"),
    ("Afterburn Fitness 2", "753454598581795"),
    ("CLUB4 - Prattville", "725278396132912"),
    ("CLUB4 - Schillinger", "3857358031186659"),
    ("CLUB4 - Shreveport", "937226184733455"),
    ("CLUB4 - Slidell", "1559285031582494"),
    ("CLUB4 - Starkville", "6483928041710615"),
    ("CLUB4 - Tillman's Corner", "904491511426823"),
    ("CLUB4 - Trussville", "1056669778768498"),
    ("CLUB4", "250052719425863"),
    ("CLUB4 - Lubbock (New)", "3513413998952998"),
    ("RedX", "733532694051063"),
    ("Shasta Athletic Club", "245388930016665"),
    ("West Seattle Health Club", "3658212624277624"),
    ("World Gym Beaumont", "180438538439013")
]
# FINAL VERSION: Meta Ads monitoring with full AD_ACCOUNTS, Supabase insert, campaign budget, and error handling

import os
import aiohttp
import asyncio
from aiohttp_retry import RetryClient, ExponentialRetry
from datetime import datetime, timedelta
import json
from supabase import create_client, Client
import nest_asyncio

nest_asyncio.apply()

# ========== CONFIGURATION ==========
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN", "your-facebook-access-token")
BASE_URL = "https://graph.facebook.com/v22.0"

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========== ACCOUNT LIST ==========
AD_ACCOUNTS = [
    ("425 Fitness - Bothell", "15966930"),
    ("425 Fitness - Issaquah", "1791553457827738"),
    ("425 Fitness - Redmond", "1796703287312755"),
    ("Absolute Recomp", "581107862007716"),
    ("American Barbell Clubs", "768011641837919"),
    ("CAC", "430492915277766"),
    ("Club Fit", "2978635188937841"),
    ("Club Fitness", "3860891150613766"),
    ("Curl - Diamond Bar", "1582311701978540"),
    ("Curl - Newport Beach", "439964580942138"),
    ("Curl - Riverside", "260102209680906"),
    ("Curl - Westminster", "3448117208769669"),
    ("Curl - Yorba Linda", "742929074164598"),
    ("Defined Fitness (NEW)", "778779483157578"),
    ("F19 - Arroyo Grande", "1129161167635243"),
    ("F19 - Bellflower", "391285962931791"),
    ("F19 - Brea", "154524177522964"),
    ("F19 - Buena Park", "733550457736583"),
    ("F19 - Chino", "1246849129480029"),
    ("F19 - Gardena", "3288589024796677"),
    ("F19 - Huntington Beach", "465905625092536"),
    ("F19 - Maywood", "353825873625518"),
    ("F19 - Menifee", "1110239503212814"),
    ("F19 - Milpitas", "162039226723214"),
    ("F19 - Mission Viejo", "231198712644144"),
    ("F19 - Murrieta", "2173769466141513"),
    ("F19 - Oceanside", "908024833582635"),
    ("F19 - Orange", "611113991033078"),
    ("F19 - Pico Rivera", "731074285152548"),
    ("F19 - San Clemente", "876251070119483"),
    ("F19 - San Mateo", "1960093251005052"),
    ("F19 - Temecula", "1669572166835145"),
    ("F19 - Wildomar", "997242517906978"),
    ("LEVEL Fitness Pelham", "820514018691762"),
    ("LEVEL Fitness Thornwood", "1171062777200647"),
    ("LEVEL Fitness Yorktown", "339863677199288"),
    ("LEVEL Somers", "1102554120713582"),
    ("Focus Fitness Club", "493003589707254"),
    ("Gentry", "1116948612833379"),
    ("GG Quebec - Laval", "4240296322862443"),
    ("GG Quebec - VSL", "514167960371052"),
    ("GymBox", "21592172220878916"),
    ("Hidden", "2199284363711953"),
    ("Henry's Gymnasium - Capitol Hill", "3245179648899298"),
    ("inMotion", "1315711728953574"),
    ("Maximum", "210589141110391"),
    ("Physiq", "797120043955249"),
    ("Powerhouse Bethlehem", "458742165022754"),
    ("Powerhouse East Lansing", "169457014390081"),
    ("Powerhouse Milford", "240093310350087"),
    ("Powerhouse Northville", "369111118813510"),
    ("Powerhouse Novi & West Bloomfield", "2439107192881819"),
    ("Powerhouse Saline", "273950090531648"),
    ("Powerhouse Shelby", "356127662270365"),
    ("Powerhouse South Lyon", "640642956610407"),
    ("Powerhouse Southfield", "675546043390548"),
    ("Powerhouse St. Clair Shores", "254332192962492"),
    ("Powerhouse Ypsilanti", "701871214003172"),
    ("PUMP24 Harrisonburg", "1113809299872673"),
    ("PUMP24 South Riding", "555471603635789"),
    ("Olympus Fitness", "817510707050988"),
    ("BodyFuel Fitness", "153306196665189"),
    ("Afterburn Fitness 1", "983503355690314"),
    ("Afterburn Fitness 2", "753454598581795"),
    ("CLUB4 - Prattville", "725278396132912"),
    ("CLUB4 - Schillinger", "3857358031186659"),
    ("CLUB4 - Shreveport", "937226184733455"),
    ("CLUB4 - Slidell", "1559285031582494"),
    ("CLUB4 - Starkville", "6483928041710615"),
    ("CLUB4 - Tillman's Corner", "904491511426823"),
    ("CLUB4 - Trussville", "1056669778768498"),
    ("CLUB4", "250052719425863"),
    ("CLUB4 - Lubbock (New)", "3513413998952998"),
    ("RedX", "733532694051063"),
    ("Shasta Athletic Club", "245388930016665"),
    ("West Seattle Health Club", "3658212624277624"),
    ("World Gym Beaumont", "180438538439013")
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
            ad_id = insight.get("ad_id")
            ad_data = ad_map.get(ad_id, {})
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
            conversions = sum(
                int(action.get("value", 0))
                for action in actions
                if action.get("action_type") in ["offsite_conversion", "purchase", "lead"]
            )
            cpa = (spend / conversions) if conversions else 0

            rows_to_insert.append({
                "account_id": ad_account_id,
                "business_name": business_name,
                "ad_id": ad_id,
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
                "cpa": cpa,
                "date": insight.get("date_start"),
                "flagged": False,
                "flagged_reason": None
            })

        supabase.table("meta_ads_monitoring").upsert(
            rows_to_insert, on_conflict=["account_id", "ad_id", "date"]
        ).execute()
        print(f"✅ Inserted {len(rows_to_insert)} rows for account {ad_account_id} - {business_name}")

# ========== MAIN ==========
async def main():
    today = datetime.today()
    past_7_days = today - timedelta(days=7)
    time_range = {
        "since": past_7_days.strftime("%Y-%m-%d"),
        "until": today.strftime("%Y-%m-%d")
    }

    for i, (business_name, ad_account_id) in enumerate(AD_ACCOUNTS):
        print(f"\n🔄 [{i+1}/{len(AD_ACCOUNTS)}] Processing: {business_name} (act_{ad_account_id})")
        try:
            await process_and_save(ad_account_id, time_range, business_name)
        except Exception as e:
            print(f"⚠️ Skipping {business_name} due to error: {e}")
        await asyncio.sleep(3)

await main()

