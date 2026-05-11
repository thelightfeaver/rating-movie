from pathlib import Path
import pandas as pd
import mlflow
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
import typer
from loguru import logger
from tqdm import tqdm
import duckdb

from src.config import MODELS_DIR, PROCESSED_DATA_DIR, MINIO

app = typer.Typer()


@app.command()
def main(
    # ---- REPLACE DEFAULT PATHS AS APPROPRIATE ----
    features_path: Path = PROCESSED_DATA_DIR / "features.csv",
    labels_path: Path = PROCESSED_DATA_DIR / "labels.csv",
    model_path: Path = MODELS_DIR / "model.pkl",
    # -----------------------------------------
):

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
        SELECT * FROM read_parquet('s3://{MINIO["bucket"]}/c_data.parquet')
    """

    df = con.execute(query).df()
    df = pd.read_parquet("")
    x = df.drop(["hit"])
    y = df["hit"]
    x_train, y_train, x_test, y_test = train_test_split(x, y)
    lr = RandomForestClassifier()
    lr.fit(x_train, y_train)


if __name__ == "__main__":
    app()
