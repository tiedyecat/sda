# gpt_flagging.py

import os
import openai
from datetime import datetime, timedelta
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
openai.api_key = os.environ["OPENAI_API_KEY"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========== GET RECENT DATA ==========
def get_recent_rows():
    since = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
    response = supabase.table("meta_ads_monitoring") \
        .select("id, business_name, spend, impressions, clicks, ctr, cpc, cpm, cpa, leads, purchases, conversions, date") \
        .gte("date", since) \
        .order("date", desc=True).execute()
    rows = response.data or []

    # Pre-flagging logic before GPT
    for row in rows:
        row["pre_flag"] = False
        row["pre_reason"] = None
        try:
            date_obj = datetime.strptime(row["date"], "%Y-%m-%d")
            days_old = (datetime.today() - date_obj).days
            if row["leads"] == 0 and days_old >= 2:
                row["pre_flag"] = True
                row["pre_reason"] = "No leads in 2+ days"
            elif row["spend"] > 500:
                row["pre_flag"] = True
                row["pre_reason"] = "High Daily Spend"
            elif row["cpa"] > 35:
                row["pre_flag"] = True
                row["pre_reason"] = "High CPA"
        except:
            continue
    return rows

# ========== FORMAT PROMPT ==========
def build_prompt(rows):
    examples = "".join([
        f"- Business: {row['business_name']}, Spend: ${row['spend']}, CPA: ${row['cpa']}, CTR: {row['ctr']}%, Leads: {row['leads']}, Conversions: {row['conversions']}\n"
        for row in rows
    ])
    return f"""
You are a senior paid media strategist working as an AI performance reviewer for Meta ad campaigns.

Your task is to analyze ad performance data from various gym and fitness businesses over the past 7 days. You should consider aggregated patterns — such as consistently high spend, persistently low CTR, or no conversions across multiple days — when making your flagging decisions.

Use your expertise in CTR, CPC, CPA, ROAS, lead generation, and spend management to make strategic judgments across the entire time range, not just one row at a time. in CTR, CPC, CPA, ROAS, lead generation, and spend management to make strategic judgments.

Guidelines:
- Do not make up values — use only what's provided.
- Use plain language — imagine you're explaining to a campaign manager.
- Be direct. Flag only rows that clearly warrant attention.
- Then, provide a ranked list of the *Top 5 underperforming businesses* this week based on your analysis.

Return a JSON array where each object contains:
  "business_name", "flagged", "reason", "summary"

Data:
{examples}

Respond only with a valid JSON list and include a separate key called "top_5_summary" with your ranked overview:
[
  {{"business_name": ..., "flagged": true, "reason": ..., "summary": ... }},
  ...
]
"""

# ========== CALL OPENAI ==========
def get_ai_flags(prompt):
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You're a paid ads optimization assistant."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.3,
    )
    text = response.choices[0].message.content
    return eval(text)  # You could use json.loads with extra validation

# ========== UPDATE SUPABASE ==========
def update_rows(ai_results):
    for row in ai_results:
        updates = {
            "flagged": row["flagged"],
            "flagged_reason": row["reason"],
            "ai_summary": row["summary"]
        }
        supabase.table("meta_ads_monitoring").update(updates) \
            .eq("business_name", row["business_name"]) \
            .eq("date", datetime.today().strftime("%Y-%m-%d")) \
            .execute()

# ========== MAIN ==========
def main():
    rows = get_recent_rows()
    if not rows:
        print("No data to analyze.")
        return
    prompt = build_prompt(rows)
    flagged_rows = get_ai_flags(prompt)
    update_rows(flagged_rows)
    print("✅ AI flagging and summaries complete.")

if __name__ == "__main__":
    main()
