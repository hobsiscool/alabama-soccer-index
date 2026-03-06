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
    """Nuclear standardization to prevent ranking inflation and duplicate teams."""
    name = name.replace("High School", "").replace("High", "").replace("HS", "")
    name = name.replace("@", "").strip()
    return name.strip().title()

def get_game_hash(date, t1, t2):
    """Creates a unique ID by collapsing all date whitespace."""
    c1, c2 = normalize_name(t1), normalize_name(t2)
    date_key = "".join(str(date).split())
    combined = "".join(sorted([c1, c2])) + date_key
    return hashlib.md5(combined.encode()).hexdigest()

def process_match_element(match, primary_team_name, c_id):
    """Universal hunter for played and upcoming games with neutral site detection."""
    # 1. Score Extraction (COALESCE handles missing scores for upcoming games)
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
        date_clean = "Upcoming"

    # 3. Location Logic (Neutral Site & Home/Away)
    is_home = "@" not in raw_opp_text
    is_neutral = False
    comment = match.find('div', class_='comment')
    if comment:
        txt = comment.text.lower()
        if any(w in txt for w in ['neutral', 'shootout', 'tournament', 'classic']):
            is_neutral, is_home = True, False

    g_id = get_game_hash(date_clean, n_primary, n_opponent)
    
    with engine.begin() as conn:
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
    # Ordered 7A first to anchor ratings
    classes = [("1670", "7A"), ("1671", "6A"), ("1672", "5A"), ("1673", "4A"), ("1674", "1A-3A")]
    
    for c_id, c_name in classes:
        print(f"Syncing Class {c_name}...", flush=True)
        try:
            res = session.get(f"{BASE_URL}/classifications/{c_id}/teams")
            soup = BeautifulSoup(res.text, 'html.parser')
            teams = soup.select('#team_list .name a')
        except: continue

        for link in teams:
            t_name = link.text.strip()
            t_url = BASE_URL + link.get('href')
            print(f"  Checking {normalize_name(t_name)}...", flush=True)
            time.sleep(random.uniform(1.0, 1.5)) 
            try:
                t_res = session.get(t_url)
                t_soup = BeautifulSoup(t_res.text, 'html.parser')
                for m in t_soup.find_all('div', class_='match'):
                    process_match_element(m, t_name, c_id)
            except: pass

if __name__ == "__main__":
    scrape_cycle()