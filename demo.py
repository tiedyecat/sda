import os, sys, json, asyncio, random, pathlib
from datetime import datetime, timedelta, timezone
from typing import Dict, List
import aiohttp, nest_asyncio
from aiohttp_retry import RetryClient, ExponentialRetry
from dotenv import load_dotenv
from supabase import create_client, Client, SupabaseException

# ENV & SETUP
load_dotenv(override=True)
nest_asyncio.apply()

TOKEN = (os.getenv("ACCESS_TOKEN") or "").strip().strip('"').strip("'")
if not TOKEN.startswith(("EAA", "EAAG")):
    sys.exit("❌ ACCESS_TOKEN missing or wrong type (must start with EAA/EAAG)")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
if not (SUPABASE_URL and SUPABASE_KEY):
    sys.exit("❌ Supabase env vars missing")

print(f"🔐 Token prefix OK: {TOKEN[:10]}…")

BASE_URL = "https://graph.facebook.com/v22.0"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
ACCOUNTS_JSON = "ad_accounts_by_team.json"
TABLE_NAME = "meta_ads_monitoring"
UNIQUE_KEYS = ["account_id", "ad_id", "date"]
BATCH_SIZE = 100  # Number of rows to process at once
BATCH_SLEEP = 1.5

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def explain_supabase_error(err: SupabaseException | Exception) -> str:
    if isinstance(err, SupabaseException):
        try:
            body = err.response.json()
        except Exception:
            body = {}
        code = body.get("code")
        if code == "42P10":
            cols_csv = ", ".join(UNIQUE_KEYS)
            return (
                "42P10: ON CONFLICT columns have no UNIQUE index.\n"
                f"→ Run once:\n"
                f"   CREATE UNIQUE INDEX IF NOT EXISTS {TABLE_NAME}_uidx "
                f"ON {TABLE_NAME} ({cols_csv});"
            )
        return " | ".join(
            x for x in [
                f"code={code}",
                body.get("message"),
                body.get("details"),
                body.get("hint")
            ] if x
        )
    return str(err)[:300]

def log_bad_rows(rows: List[Dict], label: str):
    log_dir = pathlib.Path("error_logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = log_dir / f"bad_rows_{label}_{ts}.json"
    with open(path, "w") as f:
        json.dump(rows, f, indent=2, default=str)
    print(f"📝 Saved {len(rows)} failing rows → {path}")

def load_accounts(path: str) -> List[tuple]:
    try:
        with open(path) as f:
            mapping = json.load(f)
    except FileNotFoundError:
        sys.exit(f"❌ {path} not found.")
    except json.JSONDecodeError as e:
        sys.exit(f"❌ {path} invalid JSON: {e}")
    return [(biz, acc) for team in mapping.values() for biz, acc in team]

ACCOUNTS = load_accounts(ACCOUNTS_JSON)
print(f"📋 {len(ACCOUNTS)} ad accounts loaded.")

async def fetch_url(sess, url, params=None, tries=5):
    delay = 0
    for attempt in range(tries):
        await asyncio.sleep(delay)
        try:
            async with sess.get(url, params=params) as r:
                if r.status == 200:
                    return await r.json()
                body = await r.text()
                print(f"⚠️ Meta {r.status}: {body[:200]}")
                delay = (2 ** attempt) + random.uniform(0, 1)
                continue
        except aiohttp.ClientError as ce:
            print(f"❌ ClientError: {ce}")
            delay = (2 ** attempt) + random.uniform(0, 1)
    raise RuntimeError(f"Meta request failed after {tries} attempts: {url}")

async def fetch_ads(sess, act):
    url = f"{BASE_URL}/act_{act}/ads"
    params = {"fields": "id,name,adset_id,campaign_id", "limit": 100}
    ads, nxt = [], url
    while nxt:
        data = await fetch_url(sess, nxt, params)
        ads.extend(data.get("data", []))
        nxt, params = data.get("paging", {}).get("next"), None
    return {a["id"]: a for a in ads}

async def fetch_campaigns(sess, act):
    url = f"{BASE_URL}/act_{act}/campaigns"
    params = {"fields": "id,name,daily_budget", "limit": 100}
    campaigns, nxt = [], url
    while nxt:
        data = await fetch_url(sess, nxt, params)
        campaigns.extend(data.get("data", []))
        nxt, params = data.get("paging", {}).get("next"), None
    return {
        c["id"]: {
            "campaign_name": c["name"],
            "daily_budget": float(c.get("daily_budget", 0)) / 1_000_000
        } for c in campaigns
    }

async def fetch_insights(sess, act, rng):
    url = f"{BASE_URL}/act_{act}/insights"
    params = {
        "fields": (
            "date_start,date_stop,ad_id,impressions,clicks,spend,"
            "ctr,frequency,actions"
        ),
        "level": "ad",
        "time_range[since]": rng["since"],
        "time_range[until]": rng["until"],
        "time_increment": 1,
        "limit": 50
    }
    insights, nxt = [], url
    page_count = 0
    while nxt:
        data = await fetch_url(sess, nxt, params)
        insights.extend(data.get("data", []))
        nxt, params = data.get("paging", {}).get("next"), None
        if nxt:
            page_count += 1
            print(f"  📄 Fetching insights page {page_count+1} ({len(insights)} rows so far)")
            await asyncio.sleep(0.5)  # Slight delay to prevent rate limiting
    return {"data": insights}

async def process_account(sess, business, act, rng):
    try:
        print(f"📊 Fetching insights for {business}...")
        insights = await fetch_insights(sess, act, rng)
        print(f"📊 Fetching ads for {business}...")
        ads = await fetch_ads(sess, act)
        print(f"📊 Fetching campaigns for {business}...")
        camps = await fetch_campaigns(sess, act)
    except Exception as e:
        print(f"❌ API fetch failed for {business}: {e}")
        return

    rows = []
    for ins in insights.get("data", []):
        try:
            ad = ads.get(ins["ad_id"], {})
            camp = camps.get(ad.get("campaign_id"))
            impr = int(ins.get("impressions", 0))
            clk = int(ins.get("clicks", 0))
            spd = float(ins.get("spend", 0))
            ctr = clk / impr * 100 if impr else 0
            cpc = spd / clk if clk else 0
            cpm = spd / impr * 1000 if impr else 0
            freq = float(ins.get("frequency", 0))
            acts = ins.get("actions", [])
            leads = sum(int(a["value"]) for a in acts if a["action_type"] == "lead")
            purch = sum(int(a["value"]) for a in acts if "purchase" in a["action_type"])
            conv = leads + purch
            cpa = spd / conv if conv else 0

            rows.append({
                "account_id": act,
                "business_name": business,
                "ad_id": ins["ad_id"],
                "ad_name": ad.get("name"),
                "campaign_id": ad.get("campaign_id"),
                "campaign_name": camp.get("campaign_name") if camp else None,
                "daily_budget": camp.get("daily_budget") if camp else None,
                "impressions": impr,
                "clicks": clk,
                "ctr": ctr,
                "spend": spd,
                "cpc": cpc,
                "cpm": cpm,
                "frequency": freq,
                "leads": leads,
                "purchases": purch,
                "conversions": conv,
                "cpa": cpa,
                "date": ins["date_start"],
                "flagged": False,
                "flagged_reason": None
            })
        except Exception as pe:
            print(f"⚠️ Row parse error ({business}): {pe}")

    if not rows:
        print(f"ℹ️ No rows for {business}")
        return

    # Process in batches to handle large datasets
    total_rows = len(rows)
    print(f"🔢 Processing {total_rows} rows in batches of {BATCH_SIZE}")
    
    for i in range(0, total_rows, BATCH_SIZE):
        batch = rows[i:i+BATCH_SIZE]
        try:
            supabase.table(TABLE_NAME).upsert(batch, on_conflict=UNIQUE_KEYS).execute()
            print(f"✅ {business}: upserted batch {i//BATCH_SIZE + 1}/{(total_rows-1)//BATCH_SIZE + 1} ({len(batch)} rows)")
        except SupabaseException as batch_err:
            msg = explain_supabase_error(batch_err)
            print(f"❌ Batch upsert failed ({business}, batch {i//BATCH_SIZE + 1}):\n{msg}")
            if "42P10" in msg:
                print("🔧 The unique index is missing. Run the SQL commands to add it.")
                sys.exit(1)
            
            # Fall back to row-by-row processing for this batch
            bad = []
            for r in batch:
                try:
                    supabase.table(TABLE_NAME).upsert([r], on_conflict=UNIQUE_KEYS).execute()
                except SupabaseException as row_err:
                    bad.append(r)
                    print(f"    🚫 row error: {explain_supabase_error(row_err)}")
            if bad:
                log_bad_rows(bad, f"{business}_batch_{i//BATCH_SIZE + 1}")
        
        # Add a small delay between batches
        if i + BATCH_SIZE < total_rows:
            await asyncio.sleep(0.5)

async def main():
    # Verify schema before processing
    try:
        # Check if the unique index exists
        result = supabase.table("pg_indexes") \
            .select("indexname") \
            .eq("tablename", TABLE_NAME) \
            .like("indexdef", f"%{UNIQUE_KEYS[0]}%") \
            .execute()
        
        if not result.data:
            print(f"⚠️ Warning: No unique index found for {UNIQUE_KEYS}. Run the SQL commands first.")
            print("CREATE UNIQUE INDEX IF NOT EXISTS meta_ads_monitoring_uidx")
            print(f"ON {TABLE_NAME} ({', '.join(UNIQUE_KEYS)});")
            print("NOTIFY pgrst, 'reload schema';")
    except Exception as e:
        print(f"⚠️ Schema verification failed: {e}")
    
    today = datetime.now(timezone.utc).date()
    rng = {
        "since": (today - timedelta(days=30)).strftime("%Y-%m-%d"),
        "until": today.strftime("%Y-%m-%d")
    }

    connector = aiohttp.TCPConnector(limit_per_host=6)
    retry_opt = ExponentialRetry(attempts=5)
    async with RetryClient(connector=connector, headers=HEADERS, retry_options=retry_opt) as sess:
        for i, (biz, act) in enumerate(ACCOUNTS, 1):
            print(f"\n🔄 [{i}/{len(ACCOUNTS)}] {biz} (act_{act})")
            await process_account(sess, biz, act, rng)
            await asyncio.sleep(BATCH_SLEEP)

if __name__ == "__main__":
    asyncio.run(main())
