from fastapi import FastAPI
from constants import S3_BUCKET, S3_FOLDER_DAILY_DATA
from utils.s3 import read_daily_ohlc_from_s3

app = FastAPI()
@app.get("/")
async def root():
    df = read_daily_ohlc_from_s3(ticker="A")
    return {"message": "Hello World 9", 
        "S3_BUCKET": S3_BUCKET,
        "S3_FOLDER_DAILY_DATA": S3_FOLDER_DAILY_DATA,
        "df": df.index[-1]
    }
