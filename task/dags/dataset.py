import os

from datetime import datetime
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

load_dotenv()
API = os.getenv("API")
TMDB_API_TOKEN = os.getenv("TMDB_API_TOKEN")

def _base_url(endpoint: str) -> str:
    return f"{API}{endpoint}"

def _transform_data(data: dict) -> pd.DataFrame:
    df = pd.DataFrame({"id": []})
    for item in data["results"]:
        df[len(df),"id"] = item["id"]

    return df


def _transform_data_movie(data:dict)-> pd.DataFrame:
    df = pd.DataFrame({
        "id": data["id"],
        "title": data["original_title"],
        "overview": data["overview"],
        "release_date": data["release_date"],
        "vote_average": data["vote_average"],
        "vote_count": data["vote_count"],
        "revenue": data["revenue"],
        "original_language": data["original_language"],
        "genres": "| ".join(out["name"] for out in data["genres"]),
        "budget":data["budget"],
        "production_companies": "| ".join(out["production_companies"] for out in data["production_companies"])
    })
    return df

def get_movies_id(page:int = 1) -> pd.DataFrame:
    url = _base_url(f"movie/changes?page={page}")
    headers = {"Authorization": f"Bearer {TMDB_API_TOKEN}"}
    response = requests.get(url, headers=headers)

    response.raise_for_status()
    data = response.json()
    movies_id = _transform_data(data)
    return movies_id


def get_movie_by_id(id:str):
    url = _base_url(f"movie/{id}?language=en-US' ")
    headers = {"Authorization": f"Bearer {TMDB_API_TOKEN}"}
    response = requests.get(url, headers)
    response.raise_for_status()
    data =  response.json()
    out_data = _transform_data_movie(data)
    return out_data


def dataframe_to_csv(df: pd.DataFrame, filename: str) -> None:
    df.to_csv(filename, index=False)

def recollect_data():
    id_movies = get_movies_id()
    df_out = pd.DataFrame()
    for id in id_movies["id"].to_list():
        movie = get_movie_by_id(id)
        df_out.append(movie)

    return df_out
        

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
    
