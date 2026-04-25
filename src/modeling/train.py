from pathlib import Path
import pandas as pd
import mlflow
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
import typer
from loguru import logger
from tqdm import tqdm

from src.config import MODELS_DIR, PROCESSED_DATA_DIR

app = typer.Typer()


@app.command()
def main(
    # ---- REPLACE DEFAULT PATHS AS APPROPRIATE ----
    features_path: Path = PROCESSED_DATA_DIR / "features.csv",
    labels_path: Path = PROCESSED_DATA_DIR / "labels.csv",
    model_path: Path = MODELS_DIR / "model.pkl",
    # -----------------------------------------
):
    
    mlflow.autolog()
    df = pd.read_parquet("")
    df["success"] = df["revenue"] > df["bugdet"] * 2
    x = df.drop(["success"])
    y = df["success"]
    x_train, y_train, x_test, y_test = train_test_split(x, y)
    lr = LogisticRegression()
    lr.fit(x_train,y_train)


if __name__ == "__main__":
    app()
