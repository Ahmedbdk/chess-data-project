import pandas as pd
import numpy as np
import os
from config import USERNAME, OUTPUT_DIR

def enrich_main(df: pd.DataFrame) -> pd.DataFrame:
    

    # Player-centric columns
    df["player"] = USERNAME
    df["opponent"] = np.where(df["white"] == USERNAME,
                                   df["black"],
                                   df["white"])
    df["result"] = np.where(df["white"] == USERNAME,
                                 df["result_white"],
                                 df["result_black"])
    df["opponent_result"] = np.where(df["white"] == USERNAME,
                                 df["result_black"],
                                 df["result_white"])
    df["player_elo"] = np.where(df["white"] == USERNAME,
                                     df["white_elo"],
                                     df["black_elo"])
    df["opponent_elo"] = np.where(df["white"] == USERNAME,
                                       df["black_elo"],
                                       df["white_elo"])
    df["player_color"] = np.where(df["white"] == USERNAME,
                                       "white", "black")
    df["player_accuracy"] = np.where(df["white"] == USERNAME,
                                          df["accuracy_white"],
                                          df["accuracy_black"])

    # Extract opening code from ECO URL if exists
    df["opening"] = df["eco"].str.split("openings/").str[1]

    # Keep only the desired columns
    df = df[[
        "uuid",
        "player",
        "player_elo",
        "opponent",
        "opponent_elo",
        "player_color",
        "result",
        "opponent_result",
        "time_class",
        "time_control",
        "opening",
        "rated",
        "end_datetime",
        "player_accuracy",
        "url"
    ]]
    print(df['opponent_result'])
    return df
