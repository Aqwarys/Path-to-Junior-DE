import csv
import requests
import time

MAIN_URL = "https://api.jikan.moe/v4/"


def get_data(path):
    print("Data getting...")

    response = requests.get(f"{MAIN_URL}{path}")
    response.raise_for_status()

    anime_list = response.json()
    page = 1
    while anime_list['pagination']['has_next_page'] and page < 20:
        time.sleep(0.5)
        response = requests.get(f"{MAIN_URL}{path}?page={anime_list['pagination']['current_page'] + 1}")
        response.raise_for_status()
        anime_list['data'] += response.json()['data']
        anime_list['pagination']['has_next_page'] = response.json()['pagination']['has_next_page']
        page += 1
        print(page)

    print("Data got")
    return data_parsing(anime_list["data"])


def data_parsing(anime_list):
    print("Data parsing...")
    data = []
    for anime in anime_list:
        genres = ""
        for genre in anime["genres"]:
            genres += f'{genre["name"]}, '
        data.append(
            {
                "mal_id": anime["mal_id"],
                "url": anime["url"],
                "trailer": anime["trailer"]["embed_url"],
                "title": anime["title"],
                "title_japanese": anime["title_japanese"],
                "type": anime["type"],
                "source": anime["source"],
                "episodes": anime["episodes"],
                "status": anime["status"],
                "aired": anime["aired"]["string"],
                "duration": anime["duration"],
                "rating": anime["rating"],
                "score": anime["score"],
                "members": anime["members"],
                "favorites": anime["favorites"],
                "season": anime["season"],
                "year": anime["year"],
                "genres": genres,
            }
        )

    print("Data parsed")
    return save_csv(*data)


def save_csv(*args):
    print("Data saving...")

    if not args:
        return "No data to save"

    fieldnames = args[0].keys()
    with open(FILEPATH, "w", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(args)
    print("Data saved")


FILEPATH = f"pandas&numpy/data/sub/{str(input('Enter file name: '))}.csv"
# print(get_data("top/anime?sfw"))
# print(get_data("top/anime?type=movie"))
print(get_data("seasons/2016/fall"))
