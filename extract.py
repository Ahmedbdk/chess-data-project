import pandas as pd
import requests
import json
import os
from config import USERNAME, STATE_FILE, DATA_FILE, HEADERS
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm


def get_archives(username):
    url = f"https://api.chess.com/pub/player/{username}/games/archives"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()["archives"]

def get_games_from_archive(archive_url):
    try:
        r = requests.get(archive_url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return r.json().get("games", [])
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error for {archive_url}: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error for {archive_url}: {e}")
        return []


def load_last_archive():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f).get("last_processed_archive")
    return None

def save_last_archive(archive_url):
    with open(STATE_FILE, "w") as f:
        json.dump({"last_processed_archive": archive_url}, f)


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
def extract_main(max_workers=5):
    print("Starting daily Chess ETL")

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

    print(f"Archives to process: {len(archives_to_process)}")

    if not archives_to_process:
        print("No new archives")
        return

    games = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
                executor.submit(get_games_from_archive, archive): archive
        for archive in archives_to_process
        }

        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Fetching archives"
        ):
            archive_url = futures[future]
            try:
                archive_games = future.result()
                games.extend(archive_games)
                save_last_archive(archive_url)
            except Exception as e:
                print(f"Failed archive {archive_url}: {e}")



    if not games:
        print("No new games found")
        return

    df = parse_games(games)
    return df
