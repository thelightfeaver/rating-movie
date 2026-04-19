from pathlib import Path

import requests
from loguru import logger
from tqdm import tqdm
import typer

from src.config import PROCESSED_DATA_DIR, RAW_DATA_DIR, API, TMDB_API_TOKEN

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
