import os

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

API = os.getenv("API")
TMDB_API_TOKEN = os.getenv("TMDB_API_TOKEN")

def _base_url(endpoint: str) -> str:
    return f"{API}{endpoint}"

def _transform_data(data: dict) -> pd.DataFrame:
    df = pd.DataFrame({"id": []})
    for item in data["results"]:
        df["id"] = item["id"]

def get_movies_id(page:int = 1) -> pd.DataFrame:
    url = _base_url(f"movie/changes?page={page}")
    headers = {"Authorization": f"Bearer {TMDB_API_TOKEN}"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        movies_id = _transform_data(data)
        return movies_id
    else:
        return []

def dataframe_to_csv(df: pd.DataFrame, filename: str) -> None:
    df.to_csv(filename, index=False)