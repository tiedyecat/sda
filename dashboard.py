import os
import time
import pandas as pd
import streamlit as st
from datetime import date, timedelta
from supabase import create_client
from openai import OpenAI
from dotenv import load_dotenv
import traceback

# ──────── LOAD ENV ────────
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not OPENAI_API_KEY:
    st.error("Missing environment variables (SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, OPENAI_API_KEY). Please configure them.")
    st.stop()

# ──────── CONFIGURATION ────────
MODEL_NAME = "gpt-4-0125-preview"
BATCH_SIZE = 50
CACHE_TTL = 600  # 10 minutes

# ──────── PAGE SETUP ────────
st.set_page_config(
    page_title="Fitness & Gym Ad Performance Analyzer", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# ──────── CLEAR CACHE BUTTON (for development) ────────
st.sidebar.title("Developer Tools")
if st.sidebar.button("Clear Cache"):
    st.cache_data.clear()
    st.success("Cache cleared!")

# ──────── DATA FETCHING & CLEANING ────────
@st.cache_data(ttl=CACHE_TTL)
def fetch_and_clean_data() -> pd.DataFrame:
    """Fetch and clean all advertising data from Supabase."""
    try:
        # Force clear cache during development to ensure fresh data
        st.cache_data.clear()
        
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        select_columns = "date,business_name,leads,purchases,impressions,clicks,spend"
        
        # CRITICAL FIX: NO DATE FILTER - fetch ALL data
        st.warning("TEST MODE: Fetching ALL data, date filter is currently disabled.")
        resp = sb.table("meta_ads_monitoring").select(select_columns).execute()
        
        if not resp.data:
            st.warning("No data found in 'meta_ads_monitoring' table at all. Verify content/permissions in Supabase.")
            return pd.DataFrame()
        
        df = pd.DataFrame(resp.data)
        df = df.dropna(subset=["business_name"])
        df["business_name"] = df["business_name"].str.strip().str.title()
        df["date"] = pd.to_datetime(df["date"]).dt.date
        
        numeric_cols = ["leads", "purchases", "impressions", "clicks", "spend"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
                if col != 'spend':
                   df[col] = df[col].astype(int)
            else:
                 st.warning(f"Expected numeric column '{col}' not found.")
        
        df = df.sort_values(by=['date', 'business_name'], ascending=[False, True])
        if not df.empty:
             st.info(f"Successfully fetched and cleaned {len(df)} rows.")
        return df
    
    except Exception as e:
        st.error(f"Data fetch/cleaning error: {str(e)}. Check Supabase connection/table names.")
        return pd.DataFrame()

# ──────── FITNESS MARKETING EXPERT PROMPT ────────
def generate_fitness_marketing_prompt(question: str, batch: pd.DataFrame) -> str:
    """Generate a prompt for the fitness marketing expert analysis."""
    max_date = batch['date'].max() if not batch.empty else 'N/A'
    min_date = batch['date'].min() if not batch.empty else 'N/A'
    
    return f"""
# EXPERT FITNESS MARKETING ANALYST ROLE

You are an elite fitness marketing analyst with 15+ years experience optimizing ad campaigns for gyms and fitness centers. You're the industry's top consultant who specializes in rescuing struggling gyms from poor marketing performance. Gym owners pay you $10,000/month for your expertise because your recommendations consistently deliver results.


## ANALYSIS REQUEST
"{question}"

## REPORT FORMAT
1. **Executive Summary**: Start with 2-3 sentences directly answering the question based on the data
2. **Key Metrics**: Present a well-formatted markdown table with the most relevant metrics
3. **Performance Analysis**: Provide 3-5 paragraphs analyzing performance trends, comparing businesses, and identifying strengths/weaknesses
4. **Actionable Recommendations**: List 5-10 specific, data-backed recommendations to improve marketing performance
5. **Next Steps**: Suggest follow-up analyses or data points that would enhance future decision-making

## FORMATTING REQUIREMENTS
- Use proper markdown tables, bold for emphasis, and clear headings
- Format all currency values with $ and two decimal places
- For metrics tables, always include calculated metrics like Cost per Lead (CPL), Cost per Purchase (CPP), CTR, etc.
- Use business-specific terms familiar to gym/fitness marketers

## IMPORTANT FITNESS INDUSTRY CONTEXT
- Lead quality is typically more important than quantity for gyms
- Creative fatigue happens quickly in fitness ads (refresh every 2-3 weeks)
- Conversion paths are typically: Ad → Lead → Consultation → Membership
- Local targeting and remarketing are critical for gym success
- Seasonality heavily impacts fitness marketing (Jan-Mar and Sep are peak periods)
- Optimal CPL for fitness ranges from $5-25 depending on membership value
- Industry benchmark CTR for fitness is 1.5-3%

## IMPORTANT NOTES
- Only use data from the provided sample - never invent metrics or data points
- Focus on advertising performance metrics relevant to fitness businesses
- Highlight both positive performance and areas for improvement
- Always relate metrics back to business impact (member acquisition costs, etc.)
"""

# ──────── GPT ANALYSIS ENGINE ────────
def query_data_with_gpt(df: pd.DataFrame, question: str) -> str:
    """Process data through GPT for fitness marketing expert analysis."""
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
    except Exception as e:
        st.error(f"Failed to initialize OpenAI client: {e}")
        return "Error: OpenAI client initialization failed."

    # For small datasets, analyze all at once for better context
    if len(df) <= 150:
        prompt = generate_fitness_marketing_prompt(question, df)
        try:
            with st.spinner("Consulting our fitness marketing expert..."):
                response = client.chat.completions.create(
                    model=MODEL_NAME,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.2,  # Lower temperature for factual responses
                    max_tokens=1500   # Allow detailed analysis
                )
            return response.choices[0].message.content
        except Exception as e:
            st.error(f"Error during analysis: {str(e)}")
            return f"Analysis failed. Error: {str(e)}"
    
    # For larger datasets, use batch processing with spinners instead of progress bars
    all_responses = []
    total_rows = len(df)
    num_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE
    
    # Use info message instead of progress bar to avoid errors
    st.info(f"Processing {total_rows} rows in {num_batches} batches...")
    
    for i in range(0, total_rows, BATCH_SIZE):
        batch_num = i // BATCH_SIZE + 1
        batch = df.iloc[i:min(i + BATCH_SIZE, total_rows)]
        
        if batch.empty:
            continue
            
        prompt = generate_fitness_marketing_prompt(question, batch)
        
        try:
            progress_text = f"Analyzing batch {batch_num}/{num_batches} ({len(batch)} rows)..."
            with st.spinner(progress_text):
                response = client.chat.completions.create(
                    model=MODEL_NAME,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.2,
                    max_tokens=1200
                )
            
            if response.choices and len(response.choices) > 0 and response.choices[0].message:
                all_responses.append(response.choices[0].message.content)
            
        except Exception as e:
            st.error(f"Error analyzing batch {batch_num}: {str(e)}")
            print(f"--- Full Traceback for Batch {batch_num} Error ---")
            print(traceback.format_exc())
            print("--- End Traceback ---")
            return f"Analysis failed on batch {batch_num}. Error: {str(e)}"
    
    # Synthesize all responses with a final analysis
    if all_responses:
        synthesis_prompt = f"""
You are the nation's top paid media consultant for fitness and gyms. You've analyzed multiple batches of data and provided these insights:

{" ".join(all_responses)}

Synthesize these insights into one cohesive analysis that addresses the original question:
"{question}"

Focus on providing practical, actionable recommendations for fitness business owners. Include a summary table of key metrics where appropriate and highlight the most important findings that would impact a gym's marketing ROI.
"""
        try:
            with st.spinner("Synthesizing final analysis..."):
                final_response = client.chat.completions.create(
                    model=MODEL_NAME,
                    messages=[{"role": "user", "content": synthesis_prompt}],
                    temperature=0.2,
                    max_tokens=1500
                )
            return final_response.choices[0].message.content
        except Exception as e:
            # Fall back to concatenating responses if synthesis fails
            return "\n\n---\n\n".join(all_responses)
    
    return "No insights could be generated from the available data."

# ──────── MAIN APPLICATION FLOW ────────
st.title("🏋️‍♀️ Fitness Marketing Performance Analyzer")
st.markdown("*Expert analysis for gyms and fitness businesses*")

# Fetch data
data_df = fetch_and_clean_data()

if data_df.empty:
    st.error("Cannot proceed: No data available from Supabase.")
    st.info("""
    **Troubleshooting steps:**
    1. Check that your Supabase project is properly set up
    2. Verify the table name is 'meta_ads_monitoring'
    3. Ensure the columns match what the app expects (date, business_name, leads, etc.)
    4. Check that your service role key has proper permissions
    5. Try running this SQL in Supabase: `SELECT COUNT(*) FROM meta_ads_monitoring`
    """)
else:
    # Display data summary metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Businesses", f"{data_df['business_name'].nunique()}")
    with col2:
        st.metric("Date Range", f"{data_df['date'].min()} to {data_df['date'].max()}")
    with col3:
        st.metric("Total Leads", f"{int(data_df['leads'].sum())}")
    with col4:
        st.metric("Total Spend", f"${data_df['spend'].sum():.2f}")
    
    with st.expander("📋 View Raw Marketing Data", expanded=False):
        st.dataframe(
            data_df,
            use_container_width=True, 
            height=300,
            column_config={
                "spend": st.column_config.NumberColumn(
                    "Spend ($)",
                    format="$%.2f"
                ),
                "date": st.column_config.DateColumn(
                    "Date",
                    format="YYYY-MM-DD"
                )
            }
        )

    # Query section
    st.markdown("## 💬 Ask Your Fitness Marketing Expert")
    
    question = st.text_input(
        "",
        placeholder="e.g., 'Which fitness business has the best cost per lead?', 'How can we improve our Meta ad performance?'"
    )
    
    col1, col2 = st.columns([1, 3])
    with col1:
        analyze_button = st.button("✨ Analyze", use_container_width=True, type="primary")
    with col2:
        if analyze_button:
            if question:
                start_time = time.time()
                answer = query_data_with_gpt(data_df, question)
                end_time = time.time()
                st.success(f"Analysis completed in {end_time - start_time:.2f} seconds.")
                
                # Display the expert analysis with markdown formatting
                st.markdown("## 🏋️ Fitness Marketing Expert Analysis")
                st.markdown(answer)
            else:
                st.warning("Please enter a specific question about your fitness marketing performance.")

# ──────── SIDEBAR ────────
with st.sidebar:
    st.title("🏋️ Fitness Marketing Expert")
    st.info("""
    This tool provides expert-level analysis for your fitness and gym 
    marketing data, helping you maximize ROI and scale your fitness business.
    """)
    
    st.markdown("### How to Use")
    st.markdown("""
    1. Ensure your Supabase database is connected
    2. Ask specific questions about your marketing data
    3. Review the expert analysis and recommendations
    4. Implement the suggestions to improve performance
    """)
    
    st.markdown("### Sample Questions")
    questions = [
        "What's our average cost per lead across all fitness businesses?",
        "Which gym has the highest lead-to-purchase conversion rate?",
        "How can we improve our Facebook ad performance for our CrossFit box?",
        "What's the optimal ad spend allocation for each fitness business?",
        "Which marketing metrics should we focus on improving for our yoga studio?",
        "Compare the performance of all gyms over the last month.",
        "What's our ROAS for each gym and how can we improve it?"
    ]
    
    for q in questions:
        if st.button(q, key=q):
            st.session_state.question = q
            st.experimental_rerun()
    
    st.divider()
    st.caption("Powered by GPT & Supabase • v1.0.0")

