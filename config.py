import os


# =========================
# CONFIGURATION
# =========================
USERNAME = "ahmedbdk".lower()
OUTPUT_DIR = "data"
STATE_FILE = os.path.join(OUTPUT_DIR, "state.json")
DATA_FILE = os.path.join(OUTPUT_DIR, "chess_games.csv")
PLAYER_FILE = os.path.join(OUTPUT_DIR, "player.csv")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ChessDataBot/1.0)"
}
os.makedirs(OUTPUT_DIR, exist_ok=True)