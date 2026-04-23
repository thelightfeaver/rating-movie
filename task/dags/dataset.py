import os
from datetime import datetime
from io import BytesIO

import boto3
import pandas as pd
import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from dotenv import load_dotenv

load_dotenv()
API = os.getenv("API")
TMDB_API_TOKEN = os.getenv("TMDB_API_TOKEN")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")



def _get_client_s3():
    return boto3.resource(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

def _create_bucket_s3(s3, bucket_name: str) -> None:
    if not s3.Bucket(bucket_name) in s3.buckets.all():
        s3.create_bucket(Bucket=bucket_name)

def _base_url(endpoint: str) -> str:
    return f"{API}{endpoint}"

def _exist_file(filename: str) -> bool:
    s3 = _get_client_s3()
    bucket = s3.Bucket(S3_BUCKET_NAME)
    return any(obj.key == filename for obj in bucket.objects.all())

def _get_headers() -> dict:
    return {"Authorization": f"Bearer {TMDB_API_TOKEN}"}

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
                "popularity": data["popularity"],
                "runtime": data["runtime"],
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
    url = _base_url(f"movie/popular?language=en-US&page={page}")
    headers = _get_headers()
    response = requests.get(url, headers=headers)
    response.encoding = "utf-8"

    response.raise_for_status()
    data = response.json()
    movies_id = _transform_data(data)
    return movies_id, data["total_pages"]


def get_movie_by_id(movie_id: str) -> pd.DataFrame:
    url = _base_url(f"movie/{movie_id}?language=en-US")
    headers = _get_headers()
    response = requests.get(url, headers=headers)
    response.encoding = "utf-8"
    response.raise_for_status()
    data = response.json()
    out_data = _transform_data_movie(data)
    return out_data


def dataframe_to_csv(df: pd.DataFrame, filename: str) -> None:
    df.to_csv(filename, index=False)

def _save_data(df: pd.DataFrame, filename: str) -> None:
    s3 = _get_client_s3()

    bucket = s3.Bucket(S3_BUCKET_NAME)
    bucket.put_object(Key=filename, Body=df.to_parquet(index=False), ContentType="application/octet-stream")

def _read_data(filename: str) -> pd.DataFrame:
    s3 = _get_client_s3()
    obj = s3.Object(S3_BUCKET_NAME, filename)
    return pd.read_parquet(BytesIO(obj.get()["Body"].read()))

def recollect_data(**context) -> pd.DataFrame:
    id_movies, total_pages = get_movies_id()
    movies = []

    # Obtener todas las id de las películas en las páginas restantes
    for page in range(2, 3):
        print(f"Fetching movie IDs for page {page} of {total_pages}...")
        try:
            id_movies_page, _ = get_movies_id(page)
            id_movies = pd.concat([id_movies, id_movies_page], ignore_index=True)
        except requests.HTTPError as e:
            print(f"Error fetching movie IDs for page {page}: {e}")
            continue

    # Obtener los detalles de cada película utilizando las ID
    for movie_id in id_movies["id"].to_list():
        print(f"Fetching details for movie with ID {movie_id}...")
        try:
            movies.append(get_movie_by_id(movie_id))
        except requests.HTTPError as e:
            print(f"Error fetching movie with ID {movie_id}: {e}")
            continue

    if not movies:
        return pd.DataFrame()

    if _exist_file("raw_data.parquet"):
        existing_df = _read_data("raw_data.parquet")
        movies.append(existing_df)

    df = pd.concat(movies, ignore_index=True)

    _save_data(df, "raw_data.parquet")
    row_count = len(df)
    context["ti"].xcom_push(key="row", value=f"Total rows collected: {row_count}")
    print(f"Total rows collected: {row_count}")
    print("Data recollected and saved successfully.")

def clean_data(**context) -> None:
    df = _read_data("raw_data.parquet")
    _save_data(df, "cleaned_data.parquet")
    row_count = len(df)
    # Eliminar duplicada y datos inrevelevantes
    df.drop_duplicates(inplace=True)
    df = df[(df["vote_count"] > 0) & (df["vote_average"] > 0) & (df["popularity"] > 0) & (df["runtime"] > 0) & (df["revenue"] > 0) & (df["budget"] > 0)]
    df.drop(columns=["id"], inplace=True)
    for col in ["title", "overview", "original_language", "genres", "production_companies"]:
        df[col] = df[col].str.lower().str.strip()
    _save_data(df, "cleaned_data.parquet")
    context["ti"].xcom_push(key="row", value=f"Total rows cleaned: {row_count}")
    print(f"Total rows cleaned: {row_count}")

def feature_data(**context) -> None:
    df = _read_data("cleaned_data.parquet")
    df = df[["genres","budget", "popularity", "revenue", "runtime", "vote_average", "vote_count"]]
    _save_data(df, "featured_data.parquet")
    row_count = len(df)
    context["ti"].xcom_push(key="row", value=f"Total rows featured: {row_count}")
    print(f"Total rows featured: {row_count}")
    print("Data featured and saved successfully.")

# Versionar el bucket de S3 para mantener un historial de cambios en los datos
s3 = _get_client_s3()
_create_bucket_s3(s3, S3_BUCKET_NAME)
s3.meta.client.put_bucket_versioning(
    Bucket=S3_BUCKET_NAME,
    VersioningConfiguration={"Status": "Enabled"}
)

with DAG(
    dag_id="dataset",
    start_date= datetime(2026, 4, 22, 6, 0, 0),
    catchup=False,
    tags=["task"]
) as dag:

    extract_data = PythonOperator(
        task_id="extract",
        python_callable=recollect_data
    )

    clean_data = PythonOperator(
        task_id="clean",
        python_callable=clean_data,
    )

    feature_data = PythonOperator(
        task_id="feature",
        python_callable=feature_data,
    )

    extract_data >> clean_data >> feature_data


