import pandas as pd
import numpy as np

def transform_main(df: pd.DataFrame) -> pd.DataFrame:
    
    df['white'] = df['white'].str.lower()
    df['black'] = df['black'].str.lower()

    df['accuracy_white'] = df['accuracy_white'].fillna(np.nan)
    df['accuracy_black'] = df['accuracy_black'].fillna(np.nan)

    df['rated'] = df['rated'].astype(bool)


    df['white_elo'] = df['white_elo'].astype('Int64')
    df['black_elo'] = df['black_elo'].astype('Int64')


    df = df.drop_duplicates(subset='uuid')

    return df
