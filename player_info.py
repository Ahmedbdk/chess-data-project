import pandas as pd
import requests
import json
import os
from config import USERNAME, STATE_FILE, DATA_FILE, HEADERS

def get_player_info(username):
    url = f"https://api.chess.com/pub/player/{username}"
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        raise Exception(f"Erreur API archives: {response.status_code}")

    return response.json()

def parse_player_info(info):
    table = []

    row = {
        "player_id": info.get("player_id"),
        "url": info.get("url"),
        "username": info.get("username"),
        "country": info.get("country").split("/")[-1],
        "followers": info.get("followers"),
        "last_online": pd.to_datetime(info.get("last_online"), unit="s"),
        "joined": pd.to_datetime(info.get("joined"), unit="s"),
        "league": info.get("league")
    }
    table.append(row)
    return pd.DataFrame(table)

def extract_player():
    print("Starting Player Info ETL")

    info= get_player_info(USERNAME)
    df = parse_player_info(info)

    return df

