import os
import aiohttp
import asyncio
import json
import uuid
import hashlib
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client, SupabaseException
import nest_asyncio
from aiohttp_retry import RetryClient, ExponentialRetry

load_dotenv()
nest_asyncio.apply()

# ========== HELPER FUNCTIONS ==========
def generate_deterministic_uuid(ad_id, date_str):
    """Generate a stable UUID from ad_id and date combination"""
    base = f"{ad_id}-{date_str}"
    return str(uuid.UUID(hashlib.md5(base.encode()).hexdigest()))

# ========== CONFIGURATION ==========
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")              # <-- make sure this is set
BASE_URL = "https://graph.facebook.com/v22.0"
SUPABASE_URL = os.environ["SUPABASE_URL"]                  # <-- make sure this is set
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]     # <-- make sure this is set
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========== LOAD AD ACCOUNT MAPPING ==========
AD_ACCOUNT_JSON_PATH = "ad_accounts_by_team.json"          # <-- path to your mapping file
with open(AD_ACCOUNT_JSON_PATH) as f:
    AD_ACCOUNT_MAP = json.load(f)

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
        try:
            async with client.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    text = await response.text()
                    print(f"Meta API Error {response.status}: {text}")
                    if response.status == 400 and "too many calls" in text.lower():
                        retries += 1
                        await asyncio.sleep(30 * retries)
                    else:
                        raise Exception(f"Meta API failed: {response.status}: {text}")
        except Exception as e:
            print(f"❌ Error during fetch_url: {e}")
            retries += 1
            await asyncio.sleep(5 * retries)
    raise Exception("Max retries reached for Meta API call.")

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
        "fields": "date_start,date_stop,ad_id,impressions,clicks,ctr,spend,frequency,reach,actions",
        "level": "ad",
        "time_range[since]": time_range["since"],
        "time_range[until]": time_range["until"],
        "time_increment": 1,  # Daily breakdown enabled
        "limit": 50
    }
    
    all_insights = []
    page_count = 0
    
    while url:
        page_count += 1
        data = await fetch_url(client, url, params)
        insights_page = data.get("data", [])
        all_insights.extend(insights_page)
        
        # Get the next page URL if available
        url = data.get("paging", {}).get("next")
        
        # Clear params for subsequent requests to avoid parameter duplication
        params = {}
        
        print(f"  📊 Fetched insights page {page_count} ({len(insights_page)} records)")
    
    print(f"  ✅ Total: {len(all_insights)} insight records across {page_count} pages")
    return {"data": all_insights}

async def fetch_ad_creatives(client, ad_id):
    """Fetch creative details for a specific ad"""
    url = f"{BASE_URL}/{ad_id}/adcreatives"
    params = {
        "access_token": ACCESS_TOKEN,
        "fields": "id,name,object_story_spec,thumbnail_url,asset_feed_spec,image_hash,object_type",
        "limit": 10  # Typically one ad has few creatives
    }
    
    creatives_data = await fetch_url(client, url, params)
    return creatives_data.get("data", [])

async def get_image_url_from_hash(client, ad_account_id, image_hash):
    """Get the actual image URL from an image hash"""
    if not image_hash:
        return None
        
    url = f"{BASE_URL}/act_{ad_account_id}/adimages"
    params = {
        "access_token": ACCESS_TOKEN,
        "hashes": json.dumps([image_hash])
    }
    
    result = await fetch_url(client, url, params)
    if result and "data" in result and len(result["data"]) > 0:
        # The URL is nested in the response
        return result["data"][0].get("url")
    return None

# ========== CREATIVE PROCESSING ==========
def extract_creative_elements(creative):
    """Extract text, descriptions, CTAs from creatives"""
    elements = {
        "creative_id": creative.get("id"),
        "headline": None,
        "description": None,
        "cta_type": None,
        "thumbnail_url": creative.get("thumbnail_url"),
        "image_hash": None
    }
    
    # Extract from object_story_spec based on type
    object_story_spec = creative.get("object_story_spec", {})
    
    # Image ads
    if "link_data" in object_story_spec:
        link_data = object_story_spec["link_data"]
        elements["headline"] = link_data.get("name")
        elements["description"] = link_data.get("description") or link_data.get("message")
        elements["image_hash"] = link_data.get("image_hash")
        
        # Extract CTA
        if "call_to_action" in link_data:
            elements["cta_type"] = link_data["call_to_action"].get("type")
    
    # Video ads        
    elif "video_data" in object_story_spec:
        video_data = object_story_spec["video_data"]
        elements["headline"] = video_data.get("title")
        elements["description"] = video_data.get("message")
        
        # Extract CTA
        if "call_to_action" in video_data:
            elements["cta_type"] = video_data["call_to_action"].get("type")
    
    # For dynamic creative ads
    if "asset_feed_spec" in creative:
        asset_feed = creative["asset_feed_spec"]
        if "images" in asset_feed:
            # Get first image hash
            if asset_feed["images"] and len(asset_feed["images"]) > 0:
                elements["image_hash"] = asset_feed["images"][0].get("hash")
        
        # Extract other elements from ad templates
        if "ad_templates" in asset_feed and len(asset_feed["ad_templates"]) > 0:
            template = asset_feed["ad_templates"][0]
            elements["headline"] = template.get("title") or elements["headline"]
            elements["description"] = template.get("body") or elements["description"]
    
    return elements

# ========== PROCESS + SAVE ==========
async def process_and_save(ad_account_id, time_range, business_name):
    retry_options = ExponentialRetry(attempts=5)
    async with RetryClient(raise_for_status=False, retry_options=retry_options) as client:
        try:
            insights = await fetch_insights(client, ad_account_id, time_range)
            ads = await fetch_ads(client, ad_account_id)
            campaigns = await fetch_campaigns(client, ad_account_id)
        except Exception as api_error:
            print(f"❌ API Fetch Error for {business_name}: {api_error}")
            return

        ad_map = {ad["id"]: ad for ad in ads}
        rows_to_insert = []
        
        # Creative cache to avoid redundant API calls
        creative_cache = {}

        for insight in insights.get("data", []):
            try:
                ad_id = insight.get("ad_id")
                date_str = insight.get("date_start")
                
                # Fetch creative data for this ad (use cache if available)
                if ad_id not in creative_cache:
                    creatives = await fetch_ad_creatives(client, ad_id)
                    creative_cache[ad_id] = creatives
                else:
                    creatives = creative_cache[ad_id]
                
                # Process creative elements (use first creative if multiple exist)
                creative_elements = {}
                if creatives and len(creatives) > 0:
                    creative_elements = extract_creative_elements(creatives[0])
                    
                    # If we have an image hash, get the actual URL
                    if creative_elements.get("image_hash"):
                        # Check if we already got this image URL
                        image_hash = creative_elements.get("image_hash")
                        if f"img_{image_hash}" not in creative_cache:
                            image_url = await get_image_url_from_hash(
                                client, 
                                ad_account_id, 
                                image_hash
                            )
                            creative_cache[f"img_{image_hash}"] = image_url
                        else:
                            image_url = creative_cache[f"img_{image_hash}"]
                            
                        creative_elements["image_url"] = image_url
                
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
                reach = int(insight.get("reach", 0))  # Added reach metric
                ctr = (clicks / impressions * 100) if impressions else 0
                cpc = (spend / clicks) if clicks else 0
                cpm = (spend / impressions * 1000) if impressions else 0

                actions = insight.get("actions", [])
                leads = sum(int(a.get("value", 0)) for a in actions if a.get("action_type") == "lead")
                purchases = sum(int(a.get("value", 0)) for a in actions if a.get("action_type") == "purchase")
                conversions = leads + purchases
                cpa = (spend / conversions) if conversions else 0

                rows_to_insert.append({
                    "id": generate_deterministic_uuid(ad_id, date_str),  # Deterministic UUID
                    "ad_id": ad_id,  # Store ad_id for filtering/grouping
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
                    "reach": reach,  # Added reach field
                    "conversions": conversions,
                    "leads": leads,
                    "purchases": purchases,
                    "cpa": cpa,
                    "date": date_str,
                    "flagged": False,
                    "flagged_reason": None,
                    
                    # Creative fields
                    "creative_id": creative_elements.get("creative_id"),
                    "headline": creative_elements.get("headline"),
                    "description": creative_elements.get("description"),
                    "cta_type": creative_elements.get("cta_type"),
                    "thumbnail_url": creative_elements.get("thumbnail_url"),
                    "image_url": creative_elements.get("image_url"),
                    
                    # Updated timestamp with timezone (fixed deprecation warning)
                    "updated_at": datetime.now(timezone.utc).isoformat()
                })
            except Exception as row_err:
                print(f"⚠️ Error processing insight row for {business_name}: {row_err}")

        # Don't attempt empty inserts
        if not rows_to_insert:
            print(f"ℹ️ No rows to insert for {business_name}")
            return

        try:
            # Process in batches for better error handling
            batch_size = 100
            for i in range(0, len(rows_to_insert), batch_size):
                batch = rows_to_insert[i:i+batch_size]
                try:
                    supabase.table("meta_ads_monitoring") \
                            .upsert(batch, on_conflict=["id"]) \
                            .execute()
                    print(f"✅ Inserted batch {i//batch_size + 1} ({len(batch)} rows) for {business_name}")
                except Exception as batch_err:
                    print(f"❌ Error inserting batch {i//batch_size + 1} for {business_name}: {batch_err}")
                    
                    # If specific PostgREST error, try individual row insertion
                    if hasattr(batch_err, 'code') and batch_err.code == 'PGRST100':
                        print(f"  PostgREST parsing error. Try checking data types and field names.")
                        
                        # Try inserting one by one to find problematic row
                        for j, row in enumerate(batch):
                            try:
                                supabase.table("meta_ads_monitoring") \
                                    .upsert([row], on_conflict=["id"]) \
                                    .execute()
                            except Exception as row_err:
                                print(f"  ❌ Error at row {j+1}: {row_err}")
            
            # Log date ranges for debugging
            if rows_to_insert:
                print(f"📅 Dates: {sorted(set(row['date'] for row in rows_to_insert))}")
                
        except SupabaseException as supa_err:
            print(f"❌ Supabase Error for {business_name}: {supa_err.message}")
        except Exception as e:
            print(f"❌ General Supabase Insert Error for {business_name}: {e}")

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
