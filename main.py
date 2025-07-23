import logging
import os
from logging.config import dictConfig
from typing import Any, Dict, Union

from dotenv import load_dotenv
from fastapi import FastAPI, Form, HTTPException, Request
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
    if not os.path.exists(img_path_filename):
        update_ohlc_rsi_chart(ticker=ticker)
    return templates.TemplateResponse(
        name="img_rsi.html",
        context={
            "ticker": ticker,
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
    print("Inside rsi_update - 1")
    update_ohlc_rsi_chart(ticker=ticker)
    print("Inside rsi_update - 2")
    redirect_url = f"/rsi/{ticker}"
    return RedirectResponse(redirect_url, status_code=301)


@app.get("/render_form_last_rsi", response_class=HTMLResponse)
async def read_form(request: Request) -> Any:
    """
    Renders the HTML form, passing the list of available tickers.
    """
    return templates.TemplateResponse(
        "last_rsi_form.html", {"request": request, "tickers": TICKERS_TO_FOLLOW}
    )


@app.post("/get_min_price_for_rsi_threshold")
async def submit_data(
    request: Request,
    ticker: str = Form(...),
    col_name: str = Form("Close"),
    period: int = Form(14),
    threshold: int = Form(85),
) -> Any:
    """
    Receives data from the form and prints it.
    Also, includes a basic validation for the ticker against the available list.
    """
    if ticker not in TICKERS_TO_FOLLOW:
        raise HTTPException(
            status_code=422,
            detail=f"Ticker {ticker.upper()} is not in TICKERS_TO_FOLLOW",
        )

    df = read_daily_ohlc_from_s3(ticker=ticker)
    if df is None or df.empty:
        raise ValueError(f"read_daily_ohlc_from_s3 for {ticker=} failed")
    next_day_threshold_price, calculated_rsi_val, msg = (
        get_min_price_for_indicator_threshold(
            df=df,
            indicator_func=get_last_rsi_value,
            indicator_threshold=threshold,
            n_prices=period,
            price_column=col_name,
        )
    )
    next_day_threshold_price = round(float(next_day_threshold_price), 2)
    calculated_rsi_val = round(float(calculated_rsi_val), 2)
    output: Dict[str, Union[str, int, float]] = dict()
    output["ticker"] = ticker
    output["threshold"] = threshold
    output["period"] = period
    output["col_name"] = col_name
    output["df_last_index"] = df.index[-1]
    output["df_last_price"] = round(df[col_name].iloc[-1], 2)
    output["next_day_threshold_price"] = next_day_threshold_price
    output["calculated_rsi_val"] = calculated_rsi_val
    output["msg"] = msg
    return templates.TemplateResponse(
        "min_price_for_rsi_threshold.html",
        {"request": request, "output": output, "tickers": TICKERS_TO_FOLLOW},
    )
