import pandas as pd
import os.path
import json
import re

config = {
    "srcURL": "https://raw.githubusercontent.com/rashida048/Datasets/master/movie_dataset.csv",
    "saveFile": "movie_dataset.pkl",
    "dbName": "movies.db"
}

# /************************************************************************/
def raw_fetch(srcURL: str = config["srcURL"]) -> pd.DataFrame:
    fid = os.path.basename(srcURL)
    if not os.path.exists(fid):
        df = pd.read_csv(srcURL)
        df.to_csv(fid)
    else:
        df = pd.read_csv(fid)
    return df


# /************************************************************************/
def cleanse(raw: pd.DataFrame) -> pd.DataFrame:
    # 1. Drop nonrequired features
    newCols = [
        "title",
        "release_date",
        "budget",
        "revenue",
        "popularity",
        "runtime",
        "director",
        "genres",
        "production_companies",
    ]
    dropCols = [col for col in raw.columns.values if col not in newCols]
    clean = raw.drop(columns=dropCols)
    # 2. Address any NaNs
    clean.fillna(value="Unknown", inplace=True)
    # 3. Transform 'genres' string blob to string array
    genres = clean["genres"]
    for bad in ["Science Fiction", "TV Movie"]:
        genres = genres.str.replace(bad, bad.replace(" ", "_"))
    for k, genre in enumerate(genres):
        genres[k] = genre.split(" ")
    clean["genres"] = genres
    # 4. Fix the unicode escapes in 'director'
    clean["director"] = clean["director"].apply(lambda x: fix_unicode(x))
    # 5. Transform 'production_companies' string blob to string array
    companies = clean["production_companies"].transform(lambda x: fix_companies(x))
    clean["production_companies"] = companies
    # 6. Fix runtime error (unknown -> -1)
    clean["runtime"] = clean["runtime"].transform(
        lambda x: -1.0 if x == "Unknown" else x
    )
    # 7. Convert release date to a datetime datatype
    release = pd.to_datetime(clean['release_date'], format='%Y-%m-%d', errors='coerce')
    clean['release_date'] = release
    # Kick it back
    return clean


# /************************************************************************/
def fix_unicode(name: str) -> str:
    for ugly in re.findall(r"\\u....", name):
        new = bytes(ugly, "ascii").decode("unicode-escape")
        name = name.replace(ugly, new)
    return name


# /************************************************************************/
def fix_companies(blob: str) -> list:
    # Note: The company list looks suspiciously like a "JSON dump"
    # Let's use that to our advantage!
    myList = []
    for company in json.loads(blob):
        myList.append(company["name"])
    return myList if myList else ["Unknown"]


# /************************************************************************/
def info(df: pd.DataFrame):
    print(df.info())
    genres = sorted(df["genres"].explode().unique())
    directors = sorted(df["director"].unique())
    companies = sorted(df["production_companies"].explode().unique())
    sizes = [len(df["title"]), len(genres), len(directors), len(companies)]
    names = ["Titles", "Genres", "Directors", "Production Companies"]
    infoStr = ""
    for size, name in zip(sizes, names):
        infoStr += f"{size:d} {name}, "
    print(infoStr[:-2])


# /************************************************************************/

if __name__ == "__main__":

    df = cleanse(raw_fetch())
    info(df)
    df.to_pickle(config["saveFile"])
