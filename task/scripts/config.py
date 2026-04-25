import os

from dotenv import load_dotenv

load_dotenv()

API = os.getenv("API")
TMDB_API_TOKEN = os.getenv("TMDB_API_TOKEN")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
TMDB_FILE_URL = os.getenv("TMDB_FILE_URL")
BATCH_FOLDER = os.getenv("BATCH_FOLDER", "batches")
