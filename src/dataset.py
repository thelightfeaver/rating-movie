from pathlib import Path

import requests
import typer
from loguru import logger
from tqdm import tqdm

from src.config import API, PROCESSED_DATA_DIR, RAW_DATA_DIR, TMDB_API_TOKEN

app = typer.Typer()


@app.command()
def main(
    # ---- REPLACE DEFAULT PATHS AS APPROPRIATE ----
    input_path: Path = RAW_DATA_DIR / "dataset.csv",
    output_path: Path = PROCESSED_DATA_DIR / "dataset.csv",
    # ----------------------------------------------
):
    print(f"Using API: {API}")
    print(f"Using TMDB API Token: {TMDB_API_TOKEN}")


if __name__ == "__main__":
    app()
