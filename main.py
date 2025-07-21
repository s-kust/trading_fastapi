import logging
import os
from logging.config import dictConfig
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from constants import LOCAL_IMG_DIRECTORY, TICKERS_TO_FOLLOW
from utils.derived_columns import (
    get_last_rsi_value,
    get_min_price_for_indicator_threshold,
)
from utils.e2e import update_ohlc_rsi_chart
from utils.logging import log_config
from utils.s3 import read_daily_ohlc_from_s3

dictConfig(log_config)
app_logger = logging.getLogger("app")
load_dotenv(".env")

app = FastAPI()


app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def root(request: Request) -> Any:
    ticker = "GLD"
    df = read_daily_ohlc_from_s3(ticker=ticker)
    if df is None or df.empty:
        raise ValueError(f"read_daily_ohlc_from_s3 for {ticker=} failed")
    next_day_threshold_price, indicator_value, msg = (
        get_min_price_for_indicator_threshold(
            df=df,
            indicator_func=get_last_rsi_value,
            indicator_threshold=85,
            n_prices=14,
            price_column="Close",
        )
    )
    print(f"{next_day_threshold_price=}")
    print(f"{indicator_value=}")
    print(f"{msg=}")

    return templates.TemplateResponse(
        name="main.html",
        context={
            "tickers_to_process": TICKERS_TO_FOLLOW,
            "request": request,
        },
    )


@app.get("/rsi/{ticker}", response_class=HTMLResponse)
async def show_rsi_chart(request: Request, ticker: str) -> Any:
    rsi_threshold = 85
    ticker = ticker.upper()
    if ticker not in TICKERS_TO_FOLLOW:
        raise HTTPException(
            status_code=422,
            detail=f"Ticker {ticker.upper()} is not in TICKERS_TO_FOLLOW",
        )
    img_path_filename = LOCAL_IMG_DIRECTORY + f"{ticker}_RSI.png"
    if not os.path.exists(img_path_filename):
        update_ohlc_rsi_chart(ticker=ticker)
    df = read_daily_ohlc_from_s3(ticker=ticker)
    if df is None or df.empty:
        raise ValueError(f"read_daily_ohlc_from_s3 for {ticker=} failed")
    next_day_threshold_price, calculated_rsi_val, msg = (
        get_min_price_for_indicator_threshold(
            df=df,
            indicator_func=get_last_rsi_value,
            indicator_threshold=rsi_threshold,
            n_prices=14,
            price_column="Close",
        )
    )
    next_day_threshold_price = round(float(next_day_threshold_price), 2)
    calculated_rsi_val = round(float(calculated_rsi_val), 2)
    return templates.TemplateResponse(
        name="img_rsi.html",
        context={
            "ticker": ticker,
            "next_day_threshold_price": next_day_threshold_price,
            "rsi_threshold": rsi_threshold,
            "calculated_rsi_val": calculated_rsi_val,
            "msg": msg,
            "request": request,
        },
    )


@app.get("/rsi_update/{ticker}", response_class=RedirectResponse)
async def rsi_update(ticker: str) -> Any:
    ticker = ticker.upper()
    if ticker not in TICKERS_TO_FOLLOW:
        raise HTTPException(
            status_code=422,
            detail=f"Ticker {ticker.upper()} is not in TICKERS_TO_FOLLOW",
        )
    update_ohlc_rsi_chart(ticker=ticker)
    redirect_url = f"/rsi/{ticker}"
    return RedirectResponse(redirect_url, status_code=301)
