import pandas as pd
import numpy as np
import requests
import json
import os

# =========================
# CONFIGURATION
# =========================
USERNAME = "dengrimmeko".lower()
OUTPUT_DIR = "data"
STATE_FILE = "state.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ChessDataBot/1.0)"
}

os.makedirs(OUTPUT_DIR, exist_ok=True)

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
            "white": g["white"]["username"].lower(),
            "black": g["black"]["username"].lower(),
            "result_white": g["white"]["result"],
            "result_black": g["black"]["result"],
            "white_elo": g["white"]["rating"],
            "black_elo": g["black"]["rating"],
            "time_control": g.get("time_control"),
            "time_class": g.get("time_class"),
            "rated": bool(g.get("rated")),
            "eco": g.get("eco"),
            "accuracy_white": g.get("accuracies", {}).get("white"),
            "accuracy_black": g.get("accuracies", {}).get("black"),
            "end_datetime": pd.to_datetime(g.get("end_time"), unit="s")
        })
    return pd.DataFrame(rows)

# =========================
# MAIN PIPELINE
# =========================
def main():
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

    if not games:
        print("▶ No new games found")
        return

    bronze_df = parse_games(games)

    # =========================
    # GOLD TRANSFORMATION (player-centric)
    # =========================
    gold_df = bronze_df.drop_duplicates(subset="uuid").copy()
    gold_df["player"] = USERNAME

    gold_df["opponent"] = np.where(
        gold_df["white"] == USERNAME,
        gold_df["black"],
        gold_df["white"]
    )

    gold_df["result"] = np.where(
        gold_df["white"] == USERNAME,
        gold_df["result_white"],
        gold_df["result_black"]
    )

    gold_df["player_elo"] = np.where(
        gold_df["white"] == USERNAME,
        gold_df["white_elo"],
        gold_df["black_elo"]
    )

    gold_df["opponent_elo"] = np.where(
        gold_df["white"] == USERNAME,
        gold_df["black_elo"],
        gold_df["white_elo"]
    )

    gold_df["player_color"] = np.where(
        gold_df["white"] == USERNAME,
        "white",
        "black"
    )

    gold_df["player_accuracy"] = np.where(
        gold_df["white"] == USERNAME,
        gold_df["accuracy_white"],
        gold_df["accuracy_black"]
    )

    gold_df["opening"] = gold_df["eco"].str.split("openings/").str[1]

    gold_df = gold_df[[
        "uuid",
        "player",
        "player_elo",
        "opponent",
        "opponent_elo",
        "player_color",
        "result",
        "time_class",
        "time_control",
        "opening",
        "rated",
        "end_datetime",
        "player_accuracy",
        "url"
    ]]

    output_path = f"{OUTPUT_DIR}/chess_games_{USERNAME}_gold.csv"

    if os.path.exists(output_path):
        existing = pd.read_csv(output_path, parse_dates=["end_datetime"])
        gold_df = (
            pd.concat([existing, gold_df])
            .drop_duplicates(subset="uuid")
            .sort_values("end_datetime")
        )

    gold_df.to_csv(output_path, index=False)
    save_last_archive(archives[-1])

    print(f"✔ CSV updated: {output_path}")
    print("✔ State updated")

# =========================
# ENTRY POINT
# =========================
if __name__ == "__main__":
    main()
