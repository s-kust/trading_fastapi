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
    print("************ LIFESPAN FUNCTION STARTED - BEFORE TRY ************")
    try:
        scheduler = AsyncIOScheduler()
        # Monday to Friday at 05:00
        trigger = CronTrigger(
            year="*",
            month="*",
            day="*",
            day_of_week="mon-sun",
            hour="5",
            minute="0",
            second="0",
        )
        scheduler.add_job(update_ohlc_rsi_charts_for_tickers, trigger)
        scheduler.start()
        log_msg = "Inside lifespan: " + str(scheduler.get_jobs())
        print(log_msg)
        app_logger.info(log_msg)
        yield
    except Exception as e:
        print(f"************ EXCEPTION IN LIFESPAN: {e} ************")
        import traceback

        traceback.print_exc()  # Print full traceback for debugging
        # Re-raise the exception if you want FastAPI to fail startup
        # raise e
    finally:
        # This part runs after yield, regardless of exception during startup
        print("************ LIFESPAN FUNCTION SHUTDOWN INITIATED ************")
        if (
            "scheduler" in locals() and scheduler.running
        ):  # Check if scheduler was started
            scheduler.shutdown()
        print("************ LIFESPAN FUNCTION SHUTDOWN COMPLETE ************")


@app.get("/")
async def root() -> dict:
    ticker = "GLD"
    # update_ohlc_rsi_chart(ticker=ticker)
    log_msg = f"Inside root: {ticker=}"
    app_logger.info(log_msg)

    return {
        "message": f"Hello World RSI, {ticker=}",
        "S3_BUCKET": S3_BUCKET,
        "S3_FOLDER_DAILY_DATA": S3_FOLDER_DAILY_DATA,
    }
