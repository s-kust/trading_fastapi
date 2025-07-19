import logging
from contextlib import asynccontextmanager
from logging.config import dictConfig

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
from fastapi import FastAPI

from constants import S3_BUCKET, S3_FOLDER_DAILY_DATA
from utils.e2e import update_ohlc_rsi_chart
from utils.e2e.jobs import update_ohlc_rsi_charts_for_tickers
from utils.logging import log_config

dictConfig(log_config)
app_logger = logging.getLogger("app")
load_dotenv(".env")

app = FastAPI()


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore
    scheduler = AsyncIOScheduler()
    # Monday to Friday at 05:00
    trigger = CronTrigger(
        year="*",
        month="*",
        day="*",
        day_of_week="mon-fri",
        hour="5",
        minute="0",
        second="0",
    )
    scheduler.add_job(update_ohlc_rsi_charts_for_tickers, trigger)
    scheduler.start()
    yield
    scheduler.shutdown()


@app.get("/")
async def root() -> dict:
    ticker = "GLD"
    update_ohlc_rsi_chart(ticker=ticker)

    return {
        "message": f"Hello World RSI, {ticker=}",
        "S3_BUCKET": S3_BUCKET,
        "S3_FOLDER_DAILY_DATA": S3_FOLDER_DAILY_DATA,
    }
