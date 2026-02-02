from extract import extract_main
from transform import transform_main
from enrich import enrich_main
from config import DATA_FILE, USERNAME
from opponent_country import enrich_opponent_countries
import os
import pandas as pd

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
    print(f"✔ CSV updated: {DATA_FILE}")
    missing_count = df['opponent_country'].isna().sum()
    print(f"Nombre de pays manquants: {missing_count}")
