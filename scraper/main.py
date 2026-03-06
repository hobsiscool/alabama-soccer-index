import os, requests, hashlib, time, random, datetime, re
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(os.getenv("DATABASE_URL"))
BASE_URL = "https://scorbord.com"

def get_session():
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=2)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    })
    return session

def normalize_name(name):
    """Standardizes names to prevent hash mismatches between different pages."""
    name = name.replace("High School", "").replace("High", "").replace("HS", "")
    return name.strip().title()

def get_game_hash(date, t1, t2):
    """Collapses date and team names into a unique ID."""
    c1, c2 = normalize_name(t1), normalize_name(t2)
    date_key = "".join(str(date).split())
    combined = "".join(sorted([c1, c2])) + date_key
    return hashlib.md5(combined.encode()).hexdigest()

def process_match_element(match, primary_team_name, c_id):
    """Extracts scores and updates DB, healing incorrect classifications."""
    f_el = match.find('span', class_='our_score')
    a_el = match.find('span', class_='their_score')
    
    if f_el and a_el and f_el.text.strip() and a_el.text.strip():
        opp_link = match.find('div', class_='opponent').find('a')
        if not opp_link: return
        
        raw_opp = opp_link.text.strip()
        date_raw = match.find('div', class_='date').get_text(strip=True)
        date_clean = re.sub(r'^[a-zA-Z]+', '', date_raw).strip() 
        
        is_home = "@" not in raw_opp
        clean_opp = raw_opp.replace("@", "").strip()
        n_primary = normalize_name(primary_team_name)
        n_opponent = normalize_name(clean_opp)

        is_neutral = False
        comment = match.find('div', class_='comment')
        if comment:
            txt = comment.text.lower()
            if any(w in txt for w in ['neutral', 'shootout', 'tournament', 'classic']):
                is_neutral, is_home = True, False

        g_id = get_game_hash(date_clean, n_primary, n_opponent)
        
        with engine.begin() as conn:
            # DO UPDATE: Ensures the official Class ID overwrites '0000' placeholders
            conn.execute(text("""
                INSERT INTO games (game_id, game_date, team, opponent, score_f, score_a, is_home, is_neutral, classification)
                VALUES (:id, :dt, :t, :o, :sf, :sa, :ih, :in, :cl)
                ON CONFLICT (game_id) DO UPDATE SET 
                    classification = CASE WHEN games.classification = '0000' THEN EXCLUDED.classification ELSE games.classification END,
                    score_f = EXCLUDED.score_f,
                    score_a = EXCLUDED.score_a
            """), {"id": g_id, "dt": date_clean, "t": n_primary, "o": n_opponent, 
                   "sf": int(f_el.text), "sa": int(a_el.text), "ih": is_home, "in": is_neutral, "cl": c_id})

def scrape_cycle():
    session = get_session()
    classes = [("1671", "6A"), ("1670", "7A"), ("1672", "5A"), ("1673", "4A"), ("1674", "1A-3A")]
    
    for c_id, c_name in classes:
        # Added flush=True for live GitHub progress
        print(f"Syncing Class {c_name}...", flush=True) 
        try:
            res = session.get(f"{BASE_URL}/classifications/{c_id}/teams")
            soup = BeautifulSoup(res.text, 'html.parser')
            teams = soup.select('#team_list .name a')
        except Exception as e:
            print(f"Error loading class {c_name}: {e}", flush=True)
            continue

        for link in teams:
            team_name = link.text.strip().replace("@", "")
            team_url = BASE_URL + link.get('href')
            print(f"  Checking {team_name}...", flush=True)
            time.sleep(random.uniform(1.1, 1.8)) 
            
            try:
                team_res = session.get(team_url)
                team_soup = BeautifulSoup(team_res.text, 'html.parser')
                for match in team_soup.find_all('div', class_='match'):
                    process_match_element(match, team_name, c_id)
            except Exception as e:
                print(f"  Error on {team_name}: {e}", flush=True)

if __name__ == "__main__":
    scrape_cycle()