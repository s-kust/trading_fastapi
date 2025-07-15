from dotenv import load_dotenv
from fastapi import FastAPI

from constants import S3_BUCKET, S3_FOLDER_DAILY_DATA
from utils.s3 import read_daily_ohlc_from_s3

load_dotenv()
app = FastAPI()
@app.get("/")
async def root() -> dict:
    df = read_daily_ohlc_from_s3(ticker="A")
    
    return {"message": "Hello World 11", 
        "S3_BUCKET": S3_BUCKET,
        "S3_FOLDER_DAILY_DATA": S3_FOLDER_DAILY_DATA,
        "df": str(df.index[-1])
    }
