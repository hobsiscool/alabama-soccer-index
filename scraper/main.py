import os, requests, hashlib, time, random, datetime, re
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()

# --- DIAGNOSTIC TOGGLE ---
# Set to True to ONLY check Pelham and ensure all 11 games are caught.
CHECK_PELHAM_ONLY = False 
# -------------------------

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
    """Standardizes names and strips special characters."""
    name = name.replace("High School", "").replace("High", "").replace("HS", "")
    name = name.replace("@", "").replace("†", "").strip()
    return name.strip().title()

def get_game_hash(date, t1, t2):
    c1, c2 = normalize_name(t1), normalize_name(t2)
    # This removes EVERY bit of whitespace (spaces, tabs, non-breaking spaces)
    date_key = re.sub(r'\s+', '', str(date))
    combined = "".join(sorted([c1, c2])) + date_key
    return hashlib.md5(combined.encode()).hexdigest()

def process_match_element(match, primary_team_name, c_id):
    # Standardize all whitespace in the date immediately
    date_el = match.find('div', class_='date')
    if not date_el: return
    
    date_raw = date_el.get_text(strip=True)
    # Clean 'ThursdayFeb 5, 2026' into 'Feb 5, 2026'
    date_clean = re.sub(r'^[a-zA-Z]+', '', date_raw)
    date_clean = " ".join(date_clean.split())

    # Find scores - handle potentially empty results for upcoming games
    f_el = match.find('span', class_='our_score')
    a_el = match.find('span', class_='their_score')
    score_f = int(f_el.text.strip()) if f_el and f_el.text.strip() else None
    score_a = int(a_el.text.strip()) if a_el and a_el.text.strip() else None

    opp_link = match.find('a', href=re.compile(r'/teams/'))
    if not opp_link: return
    
    n_primary = normalize_name(primary_team_name)
    n_opponent = normalize_name(opp_link.text)

    # Location Logic
    is_home = "@" not in opp_link.get_text()
    is_neutral = False
    comment = match.find('div', class_='comment')
    if comment and any(w in comment.text.lower() for w in ['neutral', 'shootout', 'tournament']):
        is_neutral, is_home = True, False

    g_id = get_game_hash(date_clean, n_primary, n_opponent)
    
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO games (game_id, game_date, team, opponent, score_f, score_a, is_home, is_neutral, classification)
            VALUES (:id, :dt, :t, :o, :sf, :sa, :ih, :in, :cl)
            ON CONFLICT (game_id) DO UPDATE SET 
                classification = EXCLUDED.classification,
                score_f = EXCLUDED.score_f,
                score_a = EXCLUDED.score_a
        """), {"id": g_id, "dt": date_clean, "t": n_primary, "o": n_opponent, 
               "sf": score_f, "sa": score_a, "ih": is_home, "in": is_neutral, "cl": c_id})

def deep_sync_team(session, name, url, cid):
    print(f"  Nuclear Sync: {name}...", flush=True)
    try:
        res = session.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')
        # SELECTOR FIX: Grabs 'match' and 'match conference'
        matches = soup.select('.match') 
        for m in matches:
            process_match_element(m, name, cid)
    except Exception as e:
        print(f"Error: {e}")

def scrape_cycle():
    session = get_session()
    
    if CHECK_PELHAM_ONLY:
        print("!!! RUNNING PELHAM DIAGNOSTIC SYNC !!!", flush=True)
        deep_sync_team(session, "Pelham", "https://scorbord.com/teams/752720", "1671")
        print("Diagnostic complete. Check SQL count now.", flush=True)
        return

    # Normal state-wide logic below...
    classes = [("1670", "7A"), ("1671", "6A"), ("1672", "5A"), ("1673", "4A"), ("1674", "1A-3A")]
    for c_id, c_name in classes:
        print(f"Syncing Class {c_name}...", flush=True)
        res = session.get(f"{BASE_URL}/classifications/{c_id}/teams")
        soup = BeautifulSoup(res.text, 'html.parser')
        for link in soup.select('#team_list .name a'):
            deep_sync_team(session, link.text.strip(), BASE_URL + link.get('href'), c_id)

def deep_sync_team(session, name, url, cid):
    print(f"  Scraping: {name}...", flush=True)
    try:
        res = session.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')
        # This finds ALL games regardless of tournament headers
        matches = soup.select('div.match, div.match.conference')
        for m in matches:
            process_match_element(m, name, cid)
        time.sleep(random.uniform(1.0, 1.5))
    except: pass

if __name__ == "__main__":
    scrape_cycle()