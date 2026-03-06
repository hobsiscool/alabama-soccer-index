import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from model import get_rankings_with_trend
from predictor import predict_matchup
import os
import re
from streamlit_autorefresh import st_autorefresh
from dotenv import load_dotenv

# Load variables from .env file for local development
load_dotenv()

# 1. Live Heartbeat: Auto-refresh the dashboard every 30 seconds
st_autorefresh(interval=30000, key="datarefresh")

st.set_page_config(page_title="AL Soccer Analytics", layout="wide")
engine = create_engine(os.getenv("DATABASE_URL"))

# --- Helper Functions ---
def format_trend(val):
    if val > 0: return f"⬆️ {int(val)}"
    if val < 0: return f"⬇️ {int(abs(val))}"
    return "↔️"

# 2. Math Cache: Set to 60s to maintain performance during background scraping
@st.cache_data(ttl=60) 
def get_cached_rankings(_df):
    return get_rankings_with_trend(_df)

st.title("⚽ Alabama Soccer Power Index")

# --- Progress & Metadata ---
try:
    count_df = pd.read_sql("SELECT count(*) as total FROM games", engine)
    total_games = count_df['total'].iloc[0]
    # statewide goal set to 3500 games
    progress = min(total_games / 3500, 1.0)
    st.progress(progress, text=f"Statewide Database Population: {total_games} games captured")
except:
    pass

try:
    last_update_raw = pd.read_sql("SELECT MAX(game_date) FROM games", engine).iloc[0, 0]
    if last_update_raw:
        for month in ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun']:
            last_update_raw = last_update_raw.replace(month, f" {month}")
        last_update_raw = re.sub(r'(\d{4})', r'\1 | ', last_update_raw)
        st.caption(f"🗓️ Latest Data Point: {last_update_raw}")
except:
    pass

# --- Main Data Engine ---
try:
    df = pd.read_sql("SELECT * FROM games", engine)
    if not df.empty:
        class_map = {
            "1670": "7A", "1671": "6A", "1672": "5A", 
            "1673": "4A", "1674": "1A-3A"
        }
        
        # Calculate Rankings
        rankings = get_cached_rankings(df)
        lookup = df.set_index('team')['classification'].to_dict()

        # --- Statewide Top 25 Section (AHSAA Only) ---
        st.header("🏆 Statewide Top 25 (AHSAA Only)")

        # Filter for teams that belong to a known Alabama classification
        ahsaa_teams = [team for team, c_id in lookup.items() if c_id in class_map]
        rankings_ahsaa = rankings[rankings.index.isin(ahsaa_teams)].copy()

        # Calculate Average 7A Rating for the Giant Killer badge
        avg_7a = rankings_ahsaa[rankings_ahsaa.index.map(lookup) == "1670"]['Rating'].mean()

        top_25 = rankings_ahsaa.head(25).copy()
        top_25.insert(0, 'Rank', range(1, len(top_25) + 1))
        top_25['Class'] = top_25.index.map(lambda x: class_map.get(lookup.get(x)))

        # Giant Killer badge: Non-7A teams outperforming the 7A average
        top_25.index = [
            f"{name} ⚔️" if (lookup.get(name) != "1670" and row['Rating'] > avg_7a) else name 
            for name, row in top_25.iterrows()
        ]

        st.dataframe(
            top_25[['Rank', 'Class', 'Trend', 'Rating', 'SOS']].style.background_gradient(
                cmap='RdYlGn', 
                subset=['Rating', 'SOS']
            ).format(precision=2),
            width='stretch'
        )

        # --- Sidebar & Detailed Classification View ---
        st.divider()
        sel_class = st.sidebar.multiselect(
            "Filter Detailed View:", 
            list(class_map.keys()), 
            default=["1671", "1670"], 
            format_func=lambda x: class_map[x]
        )
        
        st.header(f"📊 Detailed Rankings: {', '.join([class_map[c] for c in sel_class])}")
        
        display_df = rankings[rankings.index.map(lookup).isin(sel_class)].copy()
        display_df.insert(0, 'Rank', range(1, len(display_df) + 1))
        display_df['Trend'] = display_df['Trend'].apply(format_trend)
        
        st.dataframe(
            display_df[['Rank', 'Trend', 'Rating', 'SOS']].style.background_gradient(
                cmap='RdYlGn', 
                subset=['Rating', 'SOS']
            ).format(precision=2),
            width='stretch'
        )
        
        # --- Matchup Predictor ---
        st.divider()
        st.header("🔮 Matchup Predictor")
        c1, c2 = st.columns(2)

        # Alphabetical list of Alabama-only teams
        sorted_teams = sorted(ahsaa_teams) 

        h_team = c1.selectbox("Home Team", sorted_teams, index=0)
        a_team = c2.selectbox("Away Team", sorted_teams, index=min(1, len(sorted_teams)-1))
        
        if st.button("Run Simulation"):
            res = predict_matchup(rankings.loc[h_team, 'Rating'], rankings.loc[a_team, 'Rating'])
            res_col1, res_col2, res_col3 = st.columns(3)
            res_col1.metric(f"{h_team}", f"{res['h_win']:.1%}")
            res_col2.metric("Draw probability", f"{res['draw']:.1%}")
            res_col3.metric(f"{a_team}", f"{res['a_win']:.1%}")
            st.success(f"**Projected Score:** {h_team} {res['score'][0]} - {res['score'][1]} {a_team}")
            
            st.subheader("Recent Head-to-Head results")
            h2h_df = df[((df['team'] == h_team) & (df['opponent'] == a_team)) | 
                        ((df['team'] == a_team) & (df['opponent'] == h_team))].copy()
            if not h2h_df.empty:
                st.table(h2h_df[['game_date', 'team', 'score_f', 'score_a', 'opponent']])
    else:
        st.info("Database empty. Scraper is currently visiting all classifications...")
except Exception as e:
    st.error(f"Error in dashboard: {e}")