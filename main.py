import logging
import os
from logging.config import dictConfig
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from constants import LOCAL_IMG_DIRECTORY, TICKERS_TO_FOLLOW
from utils.e2e import update_ohlc_rsi_chart
from utils.logging import log_config

dictConfig(log_config)
app_logger = logging.getLogger("app")
load_dotenv(".env")

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def root(request: Request) -> Any:
    return templates.TemplateResponse(
        name="main.html",
        context={
            "tickers_to_process": TICKERS_TO_FOLLOW,
            "request": request,
        },
    )


@app.get("/rsi/{ticker}", response_class=HTMLResponse)
async def show_rsi_chart(request: Request, ticker: str) -> Any:
    if ticker not in TICKERS_TO_FOLLOW:
        raise HTTPException(
            status_code=422,
            detail=f"Ticker {ticker.upper()} is not in TICKERS_TO_FOLLOW",
        )
    img_path_filename = LOCAL_IMG_DIRECTORY + f"{ticker}_RSI.png"
    if not os.path.exists(img_path_filename):
        update_ohlc_rsi_chart(ticker=ticker)
    return templates.TemplateResponse(
        name="img_rsi.html",
        context={
            "ticker": ticker,
            "img_path_filename": img_path_filename,
            "request": request,
        },
    )
