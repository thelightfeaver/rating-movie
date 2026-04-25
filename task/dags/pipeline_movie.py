import gzip
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from io import BytesIO

import boto3
import duckdb
import pandas as pd
import psutil
import psycopg2
import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()
API = os.getenv("API")
TMDB_API_TOKEN = os.getenv("TMDB_API_TOKEN")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
TMDB_FILE_URL = os.getenv("TMDB_FILE_URL")
BATCH_FOLDER = os.getenv("BATCH_FOLDER", "batches")

MINIO = {
    "endpoint": "host.docker.internal:9000",
    "access_key": AWS_ACCESS_KEY_ID,
    "secret_key":AWS_SECRET_ACCESS_KEY,
    "bucket": S3_BUCKET_NAME
}

POSTGRES_CONFIG = {
    "host": "db-superset",
    "port": 5432,
    "dbname": "superset",
    "user": "superset",
    "password": "superset"
}


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
    today = (datetime.now() - timedelta(days=1)).strftime("%m_%d_%Y")
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
    return {
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


def get_movies_id() -> pd.DataFrame:
    url = _get_url_today_filename()
    content = download_tmdb_export(url)
    df = _parse_gzip_json(content)
    return df["id"].tolist()

def get_movie_by_id(movie_id: str) -> pd.DataFrame:
    url = _base_url(f"movie/{movie_id}?language=en-US")
    headers = _get_headers()

    try:
        response = requests.get(url, headers=headers)
        response.encoding = "utf-8"
        if response.status_code == 429:
            time.sleep(10)
            return None
        
        data = response.json()
        out_data = _transform_data_movie(data)
        return out_data
    except Exception as _:
        return None

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

def _process_batch(batch_movies: list) -> pd.DataFrame:
    """Convertir lista de dicts a DataFrame."""
    if not batch_movies:
        return pd.DataFrame()
    return pd.DataFrame(batch_movies)

def _get_adaptive_batch_size() -> int:
    """Tamaño batch basado en memoria disponible. Max 2GB → 500, Min 512MB → 100."""
    mem_available_mb = psutil.virtual_memory().available / 1024 / 1024
    batch_size = max(100, min(1000, int(mem_available_mb / 5)))  # 1 registro ≈ 5KB
    return batch_size

def _save_batch_to_minio(batch_df: pd.DataFrame, batch_num: int) -> str:
    """Guardar batch en MinIO y retornar key."""
    batch_key = f"{BATCH_FOLDER}/batch_{batch_num:06d}.parquet"
    s3 = _get_client_s3()
    bucket = s3.Bucket(S3_BUCKET_NAME)
    bucket.put_object(Key=batch_key, Body=batch_df.to_parquet(index=False), ContentType="application/octet-stream")
    return batch_key

def _merge_and_cleanup_batches() -> None:
    """Reunir todos batches, guardar en raw_data.parquet, limpiar batches."""
    s3 = _get_client_s3()
    bucket = s3.Bucket(S3_BUCKET_NAME)

    batch_keys = [obj.key for obj in bucket.objects.all() if obj.key.startswith(BATCH_FOLDER)]
    if not batch_keys:
        print(f"No batches en {BATCH_FOLDER}")
        return

    print(f"Reuniendo {len(batch_keys)} batches...")
    all_dfs = []

    for batch_key in sorted(batch_keys):
        obj = s3.Object(S3_BUCKET_NAME, batch_key)
        df = pd.read_parquet(BytesIO(obj.get()["Body"].read()))
        all_dfs.append(df)

    merged_df = pd.concat(all_dfs, ignore_index=True)
    _save_data(merged_df, "raw_data.parquet")
    print(f"Guardado: raw_data.parquet ({len(merged_df)} filas)")

    # Limpiar batches
    for batch_key in batch_keys:
        s3.Object(S3_BUCKET_NAME, batch_key).delete()
    print(f"Limpiados {len(batch_keys)} batches")

def recollect_data(**context) -> None:
    """Recolectar películas con batching adaptativo. Guardar batches en MinIO."""
    id_movies = get_movies_id()
    id_movies = id_movies[:300000]
    total_movies = len(id_movies)
    movies_batch = []
    total_rows = 0
    batch_count = 0

    batch_size = _get_adaptive_batch_size()
    print(f"Total movie IDs: {total_movies}")
    print(f"Batch size adaptativo: {batch_size}")

    start_time = time.time()
    with ThreadPoolExecutor(max_workers=25) as executor:
        futures = {executor.submit(get_movie_by_id, id): id for id in id_movies}

        for i, future in enumerate(as_completed(futures), start=1):
            result = future.result()

            if result is not None:
                movies_batch.append(result)

            # Procesar batch cuando alcanza batch_size o es el último
            if len(movies_batch) >= batch_size or i == total_movies:
                if movies_batch:
                    batch_df = _process_batch(movies_batch)
                    batch_num = batch_count + 1
                    batch_key = _save_batch_to_minio(batch_df, batch_num)
                    batch_count += 1
                    total_rows += len(batch_df)
                    print(f"Batch {batch_num}: {len(batch_df)} filas → {batch_key}")
                    movies_batch = []

            # Rate limit
            if i % 40 == 0:
                elapsed = time.time() - start_time
                if elapsed < 1:
                    time.sleep(1 - elapsed)
                start_time = time.time()

            progress_step = max(1, total_movies // 10)
            if i % progress_step == 0 or i == total_movies:
                print(f"Procesados: {i}/{total_movies} ({i*100//total_movies}%)")

    # Reunir todos batches en raw_data.parquet y limpiar
    _merge_and_cleanup_batches()

    context["ti"].xcom_push(key="row", value=f"Total rows collected: {total_rows}")
    print(f"Total batches procesados: {batch_count}")
    print(f"Total rows collected: {total_rows}")
    print("Data recollected and saved successfully.")

def clean_data(**context) -> None:
    df = _read_data("raw_data.parquet")

    # Eliminar duplicada y datos inrevelevantes
    df = df.drop_duplicates(subset='id')
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
    df = df[df['release_date'].notna()]
    df.dropna(inplace=True)
    df.drop(columns=["id"], inplace=True)
    for col in ["title", "overview", "original_language", "genres", "production_companies"]:
        df[col] = df[col].str.lower()

    # Guardar el DataFrame limpio en S3 como un archivo Parquet
    _save_data(df, "cleaned_data.parquet")
    row_count = len(df)
    context["ti"].xcom_push(key="row", value=f"Total rows cleaned: {row_count}")
    print(f"Total rows cleaned: {row_count}")

def feature_data(**context) -> None:
    # Cargar el DataFrame limpio desde S3
    df = _read_data("cleaned_data.parquet")

    # Seleccionar solo las columnas relevantes para el análisis y modelado
    df = df[
        ["genres",
         "budget",
         "popularity",
         "revenue",
         "runtime",
         "vote_average",
         "vote_count"]]

    # Guardar el DataFrame con las características seleccionadas en S3 como un archivo Parquet
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

def load_database() -> None:
    con = duckdb.connect()

    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    con.execute(f"""
        SET s3_endpoint='{MINIO["endpoint"]}';
        SET s3_access_key_id='{MINIO["access_key"]}';
        SET s3_secret_access_key='{MINIO["secret_key"]}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)

    query = f"""
        SELECT * FROM read_parquet('s3://{MINIO['bucket']}/cleaned_data.parquet')
    """

    df = con.execute(query).df()

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS data (
            title TEXT,
            overview TEXT,
            release_date DATE,
            vote_average DOUBLE PRECISION,
            vote_count INTEGER,
            popularity DOUBLE PRECISION,
            runtime INTEGER,
            revenue BIGINT,
            original_language TEXT,
            genres TEXT,
            budget BIGINT,
            production_companies TEXT
        );
    """)

    columns = list(df.columns)
    insert_query = f"""
        INSERT INTO data ({', '.join(columns)})
        VALUES %s
    """

    data_tuples = list(df.itertuples(index=False, name=None))

    execute_values(cursor, insert_query, data_tuples)

    conn.commit()
    cursor.close()
    conn.close()


# Versionar el bucket de S3 para mantener un historial de cambios en los datos
s3 = _get_client_s3()
_create_bucket_s3(s3, S3_BUCKET_NAME)
s3.meta.client.put_bucket_versioning(
    Bucket=S3_BUCKET_NAME,
    VersioningConfiguration={"Status": "Enabled"}
)

with DAG(
    dag_id="pipeline_movie",
    start_date= datetime(2026, 4, 22, 6, 0, 0),
    catchup=False,
    tags=["data"]
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

    load = PythonOperator(
        task_id="load",
        python_callable=load_database
    )

    extract_data >> clean_data >> validation_clean >> feature_data >> validation_feature >> load
