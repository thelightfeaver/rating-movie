import gzip
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
TMDB_FILE_URL = os.getenv("TMDB_FILE_URL")


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

def _get_url_today_filename() -> str:
    today = datetime.now().strftime("%m_%d_%Y")
    return f"{TMDB_FILE_URL}/movie_ids_{today}.json.gz"

def _get_headers() -> dict:
    return {"Authorization": f"Bearer {TMDB_API_TOKEN}"}

def _parse_gzip_json(content):
    data = []

    with gzip.GzipFile(fileobj=BytesIO(content)) as f:
        for line in f:
            data.append(json.loads(line.decode("utf-8")))

    return pd.DataFrame(data)

def download_tmdb_export(url: str) -> bytes:
    response = requests.get(url, stream=True)
    response.raise_for_status()

    return response.content

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
    url = _get_url_today_filename()
    content = download_tmdb_export(url)
    df = _parse_gzip_json(content)
    return df


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
    # Limitar a 100,000 IDs para evitar problemas de memoria y tiempo de ejecución
    id_movies = get_movies_id()
    id_movies = id_movies[["id"]]
    id_movies = id_movies.iloc[:100000]
    counter = 0
    counter_error = 0
    len_id_movies = len(id_movies)
    movies = []

    print(f"Total movie IDs fetched: {len(id_movies)}")

    # Usar ThreadPoolExecutor para hacer solicitudes concurrentes y limitar la tasa a 10 por segundo
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=25) as executor:
        futures = {executor.submit(get_movie_by_id, movie_id): movie_id for movie_id in id_movies["id"].to_list()}

        for i, future in enumerate(as_completed(futures), start=1):
            # Obtenemos el resultado de la solicitud
            result = future.result()

            # validar que el resultado no sea None antes de agregarlo a la lista de películas
            if result is not None:
                movies.append(result)

            # Limitar la tasa de solicitudes a 10 por segundo
            if i % 40 == 0:
                elapsed = time.time() - start_time
                if elapsed < 1:
                    time.sleep(1 - elapsed)
                start_time = time.time()

            print(f"Procesados: {i}/{len_id_movies}")

    row_count = len(movies)
    if not movies:
        return pd.DataFrame()

    if _exist_file("raw_data.parquet"):
        existing_df = _read_data("raw_data.parquet")
        movies.append(existing_df)

    df = pd.concat(movies, ignore_index=True)

    _save_data(df, "raw_data.parquet")
    context["ti"].xcom_push(key="row", value=f"Total rows collected: {row_count}")
    print(f"Total rows collected: {row_count}")
    print(f"Total errors encountered: {counter_error}")
    print(f"Total successful fetches: {counter - counter_error}")
    print("Data recollected and saved successfully.")

def clean_data(**context) -> None:
    df = _read_data("raw_data.parquet")
    row_count = len(df)
    # Eliminar duplicada y datos inrevelevantes
    df.drop_duplicates(inplace=True)
    df = df[(df["vote_count"] > 0) & (df["vote_average"] > 0) & (df["popularity"] > 0) & (df["runtime"] > 0) & (df["revenue"] > 0) & (df["budget"] > 0)]
    df.drop(columns=["id"], inplace=True)
    for col in ["title", "overview", "original_language", "genres", "production_companies"]:
        df[col] = df[col].str.lower()
    _save_data(df, "cleaned_data.parquet")
    context["ti"].xcom_push(key="row", value=f"Total rows cleaned: {row_count}")
    print(f"Total rows cleaned: {row_count}")

def feature_data(**context) -> None:
    df = _read_data("cleaned_data.parquet")
    df = df[
        ["genres",
         "budget",
         "popularity",
         "revenue",
         "runtime",
         "vote_average",
         "vote_count"]]
    _save_data(df, "featured_data.parquet")
    row_count = len(df)
    context["ti"].xcom_push(key="row", value=f"Total rows featured: {row_count}")
    print(f"Total rows featured: {row_count}")
    print("Data featured and saved successfully.")

def validation_clean_data() -> None:
    df = _read_data("cleaned_data.parquet")
    assert not df.duplicated().any(), "There are duplicated rows in the cleaned data."
    assert (df["vote_count"] > 0).all(), "There are rows with vote_count <= 0 in the cleaned data."
    assert (df["vote_average"] > 0).all(), "There are rows with vote_average <= 0 in the cleaned data."
    assert (df["popularity"] > 0).all(), "There are rows with popularity <= 0 in the cleaned data."
    assert (df["runtime"] > 0).all(), "There are rows with runtime <= 0 in the cleaned data."
    assert (df["revenue"] > 0).all(), "There are rows with revenue <= 0 in the cleaned data."
    assert (df["budget"] > 0).all(), "There are rows with budget <= 0 in the cleaned data."
    print("Cleaned data validation passed.")

def validation_feature_data() -> None:
    df = _read_data("featured_data.parquet")
    expected_columns = {"genres", "budget", "popularity", "revenue", "runtime", "vote_average", "vote_count"}
    assert set(df.columns) == expected_columns, f"Featured data does not have the expected columns. Expected: {expected_columns}, Found: {set(df.columns)}"

    print("Featured data validation passed.")
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

    validation_clean = PythonOperator(
        task_id="validation_clean",
        python_callable=validation_clean_data,
    )

    feature_data = PythonOperator(
        task_id="feature",
        python_callable=feature_data,
    )

    validation_feature = PythonOperator(
        task_id="validation_feature",
        python_callable=validation_feature_data,
    )

    extract_data >> clean_data >> validation_clean >> feature_data >> validation_feature


