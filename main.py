import logging
from logging.config import dictConfig
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

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
    ticker = "GLD"
    # update_ohlc_rsi_chart(ticker=ticker)
    log_msg = f"Inside root: {ticker=}"
    app_logger.info(log_msg)

    return templates.TemplateResponse(
        name="img_rsi.html", context={"ticker": ticker, "request": request}
    )
