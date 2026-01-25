import requests

username = input("Player username: ").strip()

url = f"https://api.chess.com/pub/player/{username}"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/117.0.0.0 Safari/537.36"
}

# Fetch player
x = requests.get(url, headers=headers)

print("Status code:", x.status_code)
print("Raw response:", x.text[:500])

try:
    data = x.json()
    print(data)
except ValueError:
    print("Response is not valid JSON!")
    exit()

# Fetch archives
archives_url = f"https://api.chess.com/pub/player/{username}/games/archives"
r = requests.get(archives_url, headers=headers)
archives = r.json()["archives"]

print("Nombre d'archives:", len(archives))
print("Dernière archive:", archives[-1])

# Fetch games
games_url = archives[-1]
r = requests.get(games_url, headers=headers)
games_data = r.json()

print("Clés disponibles:", games_data.keys())
print("Nombre de parties:", len(games_data["games"]))

sample_game = games_data["games"][0]
print(sample_game.keys())


def parse_time_control(tc: str):
    if not tc:
        return 0, 0
    if "+" in tc:
        base, inc = tc.split("+")
        return int(base), int(inc)
    return int(tc), 0


base_time, increment = parse_time_control(sample_game.get("time_control", "0"))

bronze_record = {
    "game_id": sample_game["uuid"],
    "player_username": username,
    "source": "chesscom",
    "time_control": sample_game.get("time_control"),
    "base_time_seconds": base_time,
    "increment_seconds": increment,
    "time_class": sample_game.get("time_class"),
    "raw_json": sample_game
}

print(bronze_record)

bronze_records = []

for game in games_data["games"]:
    base_time, increment = parse_time_control(game.get("time_control", "0"))

    bronze_record = {
        "game_id": game["uuid"],
        "player_username": username,
        "source": "chesscom",
        "time_control": game.get("time_control"),
        "base_time_seconds": base_time,
        "increment_seconds": increment,
        "time_class": game.get("time_class"),
        "raw_json": game
    }

    bronze_records.append(bronze_record)

print("---------------------------------------------------")

print("Total bronze records:", len(bronze_records))
print("First record:\n", bronze_records[0])
