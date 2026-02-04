import time
from extract import extract_main
from transform import transform_main
from enrich import enrich_main
from config import DATA_FILE, USERNAME, PLAYER_FILE
from opponent_country import enrich_opponent_countries
from player_info import extract_player
import os
import pandas as pd

start_time = time.perf_counter()

# --- Extract ---
df = extract_main()

if df is not None and not df.empty:
    # --- Transform ---
    df = transform_main(df)

    # --- Enrich ---
    df = enrich_main(df)
    df = enrich_opponent_countries(df)
    


    # --- Merge avec CSV existant si besoin ---
    if os.path.exists(DATA_FILE):
        existing = pd.read_csv(DATA_FILE, parse_dates=["end_datetime"])
        df = pd.concat([existing, df]).drop_duplicates(subset="uuid").sort_values("end_datetime")

    # --- Save final CSV ---
    df.to_csv(DATA_FILE, index=False)
    print(f"CSV updated: {DATA_FILE}")
    
    missing_count = df['opponent_country'].isna().sum()
    print(f"Nombre de pays manquants: {missing_count}")
    
df_player = extract_player()

if df_player is not None and not df_player.empty:
    # --- Save final CSV ---
    df_player.to_csv(PLAYER_FILE, index=False)
    print(f"CSV updated: {PLAYER_FILE}")
    
end_time = time.perf_counter()
duration = end_time - start_time
print(f"\n Temps total d’exécution: {duration:.2f} secondes ({duration/60:.2f} minutes)")