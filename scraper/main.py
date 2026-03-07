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
    """Nuclear standardization to unify 'Ghost Teams' across the state."""
    name = name.replace("High School", "").replace("High", "").replace("HS", "")
    name = name.replace("@", "").replace("†", "").strip()
    return name.strip().title()

def get_game_hash(date, t1, t2):
    """Unique ID that is identical regardless of who is 'Home' or 'Away'."""
    c1, c2 = normalize_name(t1), normalize_name(t2)
    # Strip all whitespace from date to prevent 'Feb 5' vs 'Feb  5' mismatches
    date_key = "".join(str(date).split())
    combined = "".join(sorted([c1, c2])) + date_key
    return hashlib.md5(combined.encode()).hexdigest()

def process_match_element(match, primary_team_name, c_id):
    """Universal hunter for played and upcoming games with forced healing."""
    # 1. Score Extraction
    f_el = match.find('span', class_='our_score')
    a_el = match.find('span', class_='their_score')
    score_f, score_a = None, None
    if f_el and a_el and f_el.text.strip():
        try:
            score_f, score_a = int(f_el.text.strip()), int(a_el.text.strip())
        except: pass

    # 2. Opponent & Date Extraction
    opp_link = match.find('a', href=re.compile(r'/teams/'))
    if not opp_link: return
    
    raw_opp_text = opp_link.text.strip()
    n_primary = normalize_name(primary_team_name)
    n_opponent = normalize_name(raw_opp_text)
    
    date_el = match.find('div', class_='date')
    if date_el:
        date_raw = date_el.get_text(strip=True)
        date_clean = re.sub(r'^[a-zA-Z]+', '', date_raw) # Strip 'Thursday'
        date_clean = " ".join(date_clean.split())
    else:
        date_clean = "Unknown"

    # 3. Location Logic
    is_home = "@" not in raw_opp_text
    is_neutral = False
    comment = match.find('div', class_='comment')
    if comment:
        txt = comment.text.lower()
        if any(w in txt for w in ['neutral', 'shootout', 'tournament', 'classic']):
            is_neutral, is_home = True, False

    g_id = get_game_hash(date_clean, n_primary, n_opponent)
    
    with engine.begin() as conn:
        # DO UPDATE forces the official Class ID and latest scores
        conn.execute(text("""
            INSERT INTO games (game_id, game_date, team, opponent, score_f, score_a, is_home, is_neutral, classification)
            VALUES (:id, :dt, :t, :o, :sf, :sa, :ih, :in, :cl)
            ON CONFLICT (game_id) DO UPDATE SET 
                classification = EXCLUDED.classification,
                score_f = COALESCE(EXCLUDED.score_f, games.score_f),
                score_a = COALESCE(EXCLUDED.score_a, games.score_a),
                is_home = EXCLUDED.is_home,
                is_neutral = EXCLUDED.is_neutral
        """), {"id": g_id, "dt": date_clean, "t": n_primary, "o": n_opponent, 
               "sf": score_f, "sa": score_a, "ih": is_home, "in": is_neutral, "cl": c_id})

def scrape_cycle():
    session = get_session()
    
    # PHASE 1: Build the Master Alabama Directory
    print("Building Alabama Master Baseline...", flush=True)
    try:
        res = session.get(f"{BASE_URL}/states/al/sports/boys-soccer/teams")
        soup = BeautifulSoup(res.text, 'html.parser')
        all_teams = []
        for li in soup.select('#team_list .team'):
            n_link = li.select_one('.name a')
            c_link = li.select_one('.classification a')
            if n_link and c_link:
                # Extracting classification ID from the href (e.g., 1671)
                cid = c_link.get('href').split('/')[-2]
                all_teams.append((n_link.text.strip(), BASE_URL + n_link.get('href'), cid))
    except Exception as e:
        print(f"Failed to build master list: {e}", flush=True)
        return

    # PHASE 2: Deep-Dive every team profile in the state
    print(f"Total teams found: {len(all_teams)}. Starting deep sync...", flush=True)
    for name, url, cid in all_teams:
        print(f"  Syncing: {normalize_name(name)} (Class {cid})...", flush=True)
        time.sleep(random.uniform(1.0, 1.8)) # Safety throttle
        try:
            t_res = session.get(url)
            t_soup = BeautifulSoup(t_res.text, 'html.parser')
            # The .match selector handles regular, area, and conference games
            for match in t_soup.select('.match'):
                process_match_element(match, name, cid)
        except Exception as e:
            print(f"    Error syncing {name}: {e}", flush=True)

if __name__ == "__main__":
    scrape_cycle()
