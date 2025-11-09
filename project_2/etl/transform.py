import pandas as pd
from pathlib import Path


from logger import log


def data_cleaning(file_path: Path) -> pd.DataFrame:
    if not Path(file_path).exists():
        raise Exception(f"File {file_path} does not exist")

    df = pd.read_json(file_path, orient="records", encoding='utf-8')
    df = data_parsing(df)
    df.drop_duplicates(inplace=True)
    df.dropna(subset=["score", "rating", "episodes"], inplace=True)
    try:
        # Extracting year from aired and converting it to int
        df["year"] = df["aired"].str.extract(r"(\d{4})").astype("Int64")
        df["popularity_score"] = df["score"] * (df["members"] / 10000)
        # Converting episodes to int, title and title_japanese to string
        df = df.astype({
            "episodes": "Int64",
            "title": "string",
            "title_japanese": "string",
        })

        df = data_validation(df)


    except Exception as e:
        log.info(f"Error: {e}")
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


def data_validation(df: pd.DataFrame) -> pd.DataFrame:
    invalid_score = df[(df["score"] < 0) | (df["score"] > 10)]
    invalid_episodes = df[df["episodes"] < 0]

    if not invalid_score.empty:
        log.info(f"Invalid score: {invalid_score}")
        df.drop(invalid_score.index, inplace=True)
    if not invalid_episodes.empty:
        log.info(f"Invalid episodes: {invalid_episodes}")
        df.drop(invalid_episodes.index, inplace=True)
    return df


def aggregate_by_type_year(df: pd.DataFrame) -> pd.DataFrame:
    agg_df = (
        df.groupby(["type", "year"])
            .agg(
                avg_score=("score", "mean"),
                total_members=("members", "sum"),
                shows_count=("mal_id", "count"),
            )
            .reset_index()
    )

    return agg_df
