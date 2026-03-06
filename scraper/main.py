import os, requests, hashlib, time, random
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

def get_game_hash(date, t1, t2):
    # Ensure ID is created from CLEAN names to prevent duplicate game entries
    c1 = t1.replace("@", "").strip()
    c2 = t2.replace("@", "").strip()
    combined = "".join(sorted([c1, c2])) + str(date).strip()
    return hashlib.md5(combined.encode()).hexdigest()

def scrape_cycle():
    session = get_session()
    classes = [("1671", "6A"), 
        ("1670", "7A"), 
        ("1672", "5A"),
        ("1673", "4A"),
        ("1674", "1A-3A")]
    
    for c_id, c_name in classes:
        print(f"Syncing Class {c_name}...")
        try:
            res = session.get(f"{BASE_URL}/classifications/{c_id}/teams")
            soup = BeautifulSoup(res.text, 'html.parser')
            teams = soup.select('#team_list .name a')
        except Exception as e:
            print(f"Error loading class {c_name}: {e}")
            continue

        for link in teams:
            # Nuclear Clean: Strip @ from the primary team name immediately
            team_name = link.text.strip().replace("@", "")
            team_url = BASE_URL + link.get('href')
            
            print(f"Checking {team_name}...")
            time.sleep(random.uniform(1.5, 3.0)) 
            
            try:
                team_res = session.get(team_url)
                team_soup = BeautifulSoup(team_res.text, 'html.parser')
                
                for match in team_soup.find_all('div', class_='match'):
                    f_el = match.find('span', class_='our_score')
                    a_el = match.find('span', class_='their_score')
                    
                    if f_el and a_el:
                        raw_opp = match.find('div', class_='opponent').find('a').text.strip()
                        date_raw = match.find('div', class_='date').get_text(strip=True)
                        date_clean = date_raw.replace("Wednesday", "Wed ").replace("Thursday", "Thu ").replace("Friday", "Fri ")
                        
                        # LOCATION & NAME CLEANING
                        # Check for @ BEFORE removing it to determine Home/Away
                        is_home = "@" not in raw_opp
                        clean_opp = raw_opp.replace("@", "").strip()

                        is_neutral = False
                        comment = match.find('div', class_='comment')
                        if comment:
                            txt = comment.text.lower()
                            if any(w in txt for w in ['neutral', 'shootout', 'tournament', 'classic']):
                                is_neutral, is_home = True, False

                        # Hash and Save with 100% clean names
                        g_id = get_game_hash(date_clean, team_name, clean_opp)
                        
                        with engine.begin() as conn:
                            conn.execute(text("""
                                INSERT INTO games (game_id, game_date, team, opponent, score_f, score_a, is_home, is_neutral, classification)
                                VALUES (:id, :dt, :t, :o, :sf, :sa, :ih, :in, :cl)
                                ON CONFLICT (game_id) DO NOTHING
                            """), {"id": g_id, "dt": date_clean, "t": team_name, "o": clean_opp, 
                                   "sf": int(f_el.text), "sa": int(a_el.text), "ih": is_home, "in": is_neutral, "cl": c_id})
            except Exception as e:
                print(f"Error on {team_name}: {e}")

if __name__ == "__main__":
    # Check if we are running on GitHub Actions
    is_github = os.getenv("GITHUB_ACTIONS") == "true"
    
    if is_github:
        print("Starting automated single-cycle scrape...")
        scrape_cycle()
        print("GitHub scrape complete.")
    else:
        # Keep your local infinite loop logic
        time.sleep(5)
        while True:
            scrape_cycle()
            print("Local cycle complete. Sleeping for 24 hours...")
            time.sleep(86400)