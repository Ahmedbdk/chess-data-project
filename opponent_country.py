import requests
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ChessDataBot/1.0)"
}


def get_player_country(username):
    """
    Fetch country code of a Chess.com player.
    Returns country code (e.g. 'US', 'FR') or None.
    """
    try:
        url = f"https://api.chess.com/pub/player/{username}"
        r = requests.get(url, headers=HEADERS, timeout=10)

        if r.status_code != 200:
            return None

        data = r.json()
        country_url = data.get("country")

        if not country_url:
            return None

        return country_url.split("/")[-1]

    except Exception:
        return None


def enrich_opponent_countries(gold_df, max_workers=11):
    """
    Add 'opponent_country' column to a GOLD dataframe.
    """
    unique_opponents = gold_df["opponent"].dropna().unique()
    opponent_country_map = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(get_player_country, unique_opponents)

        for opponent, country in tqdm(
            zip(unique_opponents, results),
            total=len(unique_opponents),
            desc="Fetching opponent countries"
        ):
            opponent_country_map[opponent] = country

    gold_df["opponent_country"] = gold_df["opponent"].map(opponent_country_map)
    return gold_df
