import pandas as pd
from datetime import datetime


def clean_anime_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df.drop_duplicates(inplace=True)
    df.dropna(subset=["score", "rating", "season", "year"],inplace=True)

    df["start_date", "end_date"] = df['duration'].apply(extract_dates)

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
        "score", "members", "favorites", "season", "year", "genres"
    ]

    final_columns = [col for col in columns_order if col in df.columns]

    return df[final_columns]




def extract_dates(s):
    import re

    dates = re.findall(r'([A-Za-z]{3} \d{1,2}, \d{4})', s)
    if len(dates) == 2:
        start = datetime.strftime(dates[0], "%b %d, %Y").strftime("%d.%m.%Y")
        end = datetime.strftime(dates[1], "%b %d, %Y").strftime("%d.%m.%Y")
        return pd.Series([start, end])
    return pd.Series([None, None])
