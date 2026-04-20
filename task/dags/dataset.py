import os
from datetime import datetime

import pandas as pd
import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from dotenv import load_dotenv

load_dotenv()
API = os.getenv("API")
TMDB_API_TOKEN = os.getenv("TMDB_API_TOKEN")

def _base_url(endpoint: str) -> str:
    return f"{API}{endpoint}"

def _transform_data(data: dict) -> pd.DataFrame:
    return pd.DataFrame({"id": [item["id"] for item in data.get("results", [])]})


def _transform_data_movie(data: dict) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "id": data["id"],
                "title": data["original_title"],
                "overview": data["overview"],
                "release_date": data["release_date"],
                "vote_average": data["vote_average"],
                "vote_count": data["vote_count"],
                "revenue": data["revenue"],
                "original_language": data["original_language"],
                "genres": "| ".join(out["name"] for out in data["genres"]),
                "budget": data["budget"],
                "production_companies": "| ".join(
                    out["name"] for out in data["production_companies"]
                ),
            }
        ]
    )

def get_movies_id(page:int = 1) -> pd.DataFrame:
    url = _base_url(f"movie/changes?page={page}")
    headers = {"Authorization": f"Bearer {TMDB_API_TOKEN}"}
    response = requests.get(url, headers=headers)

    response.raise_for_status()
    data = response.json()
    movies_id = _transform_data(data)
    return movies_id


def get_movie_by_id(movie_id: str) -> pd.DataFrame:
    url = _base_url(f"movie/{movie_id}?language=en-US")
    headers = {"Authorization": f"Bearer {TMDB_API_TOKEN}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    out_data = _transform_data_movie(data)
    return out_data


def dataframe_to_csv(df: pd.DataFrame, filename: str) -> None:
    df.to_csv(filename, index=False)

def recollect_data() -> pd.DataFrame:
    id_movies = get_movies_id()
    movies = [get_movie_by_id(movie_id) for movie_id in id_movies["id"].to_list()]
    if not movies:
        return pd.DataFrame()

    return pd.concat(movies, ignore_index=True)

with DAG(
    dag_id="dataset",
    start_date= datetime(2024, 6, 1),
    catchup=False,
    tags=["task"]
) as dag:

    tarea_data = PythonOperator(
        task_id="recollect_data",
        python_callable=recollect_data
    )

