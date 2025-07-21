import logging
import os
import urllib

# import socket
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


def my_url_for(request: Request, name: str, **path_params: Any) -> str:
    url = request.url_for(name, **path_params)
    parsed = list(urllib.parse.urlparse(url))
    # parsed[0] = 'https'  # Change the scheme to 'https' (Optional)
    parsed[1] = "my_domain.com"  # Change the domain name
    return urllib.parse.urlunparse(parsed)


app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
templates.env.globals["my_url_for"] = my_url_for


@app.get("/", response_class=HTMLResponse)
async def root(request: Request) -> Any:
    # current_host = get_ip()
    # print(f"{current_host=}")
    # print(f"{type(current_host)=}")
    return templates.TemplateResponse(
        name="main.html",
        context={
            "tickers_to_process": TICKERS_TO_FOLLOW,
            "request": request,
        },
    )


@app.get("/rsi/{ticker}", response_class=HTMLResponse)
async def show_rsi_chart(request: Request, ticker: str) -> Any:
    ticker = ticker.upper()
    if ticker not in TICKERS_TO_FOLLOW:
        raise HTTPException(
            status_code=422,
            detail=f"Ticker {ticker.upper()} is not in TICKERS_TO_FOLLOW",
        )
    img_path_filename = LOCAL_IMG_DIRECTORY + f"{ticker}_RSI.png"
    print(f"{img_path_filename=}")
    if not os.path.exists(img_path_filename):
        update_ohlc_rsi_chart(ticker=ticker)
    img_path_inside_static_dir = "/images/" + ticker + "_RSI.png"
    return templates.TemplateResponse(
        name="img_rsi.html",
        context={
            "ticker": ticker,
            "img_path": img_path_inside_static_dir,
            "request": request,
        },
    )
