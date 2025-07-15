import os

from fastapi import FastAPI

from constants import S3_BUCKET, S3_FOLDER_DAILY_DATA
from utils.s3 import read_daily_ohlc_from_s3

app = FastAPI()
@app.get("/")
async def root() -> dict:
    # df = read_daily_ohlc_from_s3(ticker="A")
    # aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    # aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    # aws_region = os.getenv("AWS_REGION")
    return {"message": "Hello World 10", 
        "S3_BUCKET": S3_BUCKET,
        "S3_FOLDER_DAILY_DATA": S3_FOLDER_DAILY_DATA,
        # "aws_access_key": aws_access_key,
        # "aws_secret_key": aws_secret_key,
        # "aws_region": aws_region,
        "os_environ": os.environ
    }
