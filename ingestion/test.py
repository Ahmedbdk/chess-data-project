import requests
import csv
import os
from datetime import timezone, datetime

USERNAME = "ahmedbdk"  # change ici
BASE_URL = "https://api.chess.com/pub/player"

# Headers pour passer Cloudflare
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.chess.com/"
}

def get_json(url):
    r = requests.get(url, headers=HEADERS, timeout=30)
    if r.status_code != 200:
        print(f"❌ Erreur {r.status_code} sur {url}")
        return None
    return r.json()

# 1️⃣ Get archives
archives_url = f"{BASE_URL}/{USERNAME}/games/archives"
archives_data = get_json(archives_url)

if not archives_data:
    raise Exception("Impossible de récupérer les archives. Vérifie le username.")

archives = archives_data.get("archives", [])

# 2️⃣ Définir le chemin du CSV dans le dossier parent du script
script_dir = os.path.dirname(os.path.abspath(__file__))  # dossier du script
parent_dir = os.path.dirname(script_dir)  # dossier parent
csv_file = os.path.join(parent_dir, f"{USERNAME}_chess_games.csv")

# 3️⃣ Ecrire CSV
with open(csv_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "game_id",
        "username",
        "opponent",
        "played_at",
        "platform",
        "color",
        "result",
        "opening",
        "eco",
        "game_duration",
        "termination"
    ])

    for archive_url in archives:
        games_data = get_json(archive_url)
        if not games_data:
            continue

        for g in games_data.get("games", []):
            if g["white"]["username"].lower() == USERNAME.lower():
                color = "white"
                opponent = g["black"]["username"]
                result = g["white"]["result"]
            else:
                color = "black"
                opponent = g["white"]["username"]
                result = g["black"]["result"]

            played_at = datetime.fromtimestamp(g["end_time"], tz=timezone.utc).isoformat()

            writer.writerow([
                g.get("uuid"),
                USERNAME,
                opponent,
                played_at,
                "chess.com",
                color,
                result,
                g.get("opening", {}).get("name"),
                g.get("eco"),
                g.get("time_control"),
                g.get("termination")
            ])

print(f"✅ CSV généré avec succès dans {csv_file}")
