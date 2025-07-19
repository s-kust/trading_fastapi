import logging
from logging.config import dictConfig

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi_utilities import repeat_at

from constants import S3_BUCKET, S3_FOLDER_DAILY_DATA
from utils.e2e import update_ohlc_rsi_chart
from utils.logging import log_config

dictConfig(log_config)
app_logger = logging.getLogger("app")
load_dotenv(".env")

app = FastAPI()


@app.get("/")
async def root() -> dict:
    ticker = "GLD"
    update_ohlc_rsi_chart(ticker=ticker)

    return {
        "message": f"Hello World RSI, {ticker=}",
        "S3_BUCKET": S3_BUCKET,
        "S3_FOLDER_DAILY_DATA": S3_FOLDER_DAILY_DATA,
    }


@app.on_event("startup")
@repeat_at(cron="0 5 * * 1-5")  # Monday to Friday at 05:00
async def update_ohlc_rsi_charts_for_tickers() -> None:
    tickers = ["GLD", "COPX"]
    for ticker in tickers:
        log_msg_1 = f"update_ohlc_rsi_charts_for_tickers - {ticker=} - starting"
        app_logger.info(log_msg_1)
        update_ohlc_rsi_chart(ticker=ticker)
        log_msg_2 = f"update_ohlc_rsi_charts_for_tickers - {ticker=} - finished OK"
        app_logger.info(log_msg_2)
