import pandas as pd
import requests
import json
import os
from config import USERNAME, STATE_FILE, DATA_FILE, HEADERS

# =========================
# API FUNCTIONS
# =========================
def get_archives(username):
    url = f"https://api.chess.com/pub/player/{username}/games/archives"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()["archives"]

def get_games_from_archive(archive_url):
    r = requests.get(archive_url, headers=HEADERS)
    r.raise_for_status()
    return r.json()["games"]

# =========================
# STATE MANAGEMENT
# =========================
def load_last_archive():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f).get("last_processed_archive")
    return None

def save_last_archive(archive_url):
    with open(STATE_FILE, "w") as f:
        json.dump({"last_processed_archive": archive_url}, f)

# =========================
# PARSING
# =========================
def parse_games(games):
    rows = []
    for g in games:
        rows.append({
            "uuid": g.get("uuid"),
            "url": g.get("url"),
            "white": g["white"]["username"],
            "black": g["black"]["username"],
            "result_white": g["white"]["result"],
            "result_black": g["black"]["result"],
            "white_elo": g["white"]["rating"],
            "black_elo": g["black"]["rating"],
            "time_control": g.get("time_control"),
            "time_class": g.get("time_class"),
            "rated": g.get("rated"),
            "eco": g.get("eco"),
            "accuracy_white": g.get("accuracies", {}).get("white"),
            "accuracy_black": g.get("accuracies", {}).get("black"),
            "end_datetime": pd.to_datetime(g.get("end_time"), unit="s")
        })
    return pd.DataFrame(rows)

def extract_main():
    print("▶ Starting daily Chess ETL")

    archives = get_archives(USERNAME)
    last_archive = load_last_archive()

    if last_archive:
        archives_to_process = [a for a in archives if a > last_archive]
    else:
        archives_to_process = archives

    # always reprocess current month
    if archives:
        current_archive = archives[-1]
        if current_archive not in archives_to_process:
            archives_to_process.append(current_archive)

    print(f"▶ Archives to process: {len(archives_to_process)}")

    games = []
    for archive in archives_to_process:
        games.extend(get_games_from_archive(archive))
        save_last_archive(archive)

    if not games:
        print("▶ No new games found")
        return

    df = parse_games(games)
    return df

