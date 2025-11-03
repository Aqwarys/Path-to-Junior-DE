import pandas as pd
from datetime import datetime
import re


def clean_anime_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.drop_duplicates(inplace=True)
    df.dropna(subset=["score", "rating"],inplace=True)

    df = extract_dates(df)
    return df




def data_parsing(df: pd.DataFrame) -> pd.DataFrame:
    df["trailer"] = df["trailer"].apply(
        lambda x: x.get("embed_url") if isinstance(x, dict) else None
    )

    df["aired"] = df["aired"].apply(
        lambda x: x.get("string") if isinstance(x, dict) else None
    )

    df["genres"] = df["genres"].apply(
        lambda genres_list: ", ".join(genre["name"] for genre in genres_list)
        if isinstance(genres_list, list) and genres_list else None
    )


    columns_order = [
        "mal_id", "url", "trailer", "title", "title_japanese", "type",
        "source", "episodes", "status", "aired", "duration", "rating",
        "score", "members", "favorites", "genres"
    ]

    final_columns = [col for col in columns_order if col in df.columns]

    return df[final_columns]


def extract_dates(df: pd.DataFrame) -> pd.DataFrame:
    df["start_date_dt"] = pd.to_datetime(
        df['aired'],
        format="%b %d, %Y",
        errors='coerce'
    )
    df["start_date"] = (
        df["start_date_dt"]
        .dt.strftime("%d.%m.%Y")
        .replace('NaT', None)
    )

    df = df.drop(columns=['start_date_dt'], errors='ignore')
    return df