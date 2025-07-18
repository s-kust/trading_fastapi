import json
import logging
from datetime import datetime
from logging import Logger
from logging.config import dictConfig
from typing import Any, Callable


# Custom JSON formatter
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "module": record.module,
            "line": record.lineno,
            "message": record.getMessage(),
        }

        # Add exception info if available
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_record)


# Define the logging configuration
log_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"json": {"()": JsonFormatter}},
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "json",
            "stream": "ext://sys.stdout",
        },
        "rotating_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "DEBUG",
            "formatter": "json",
            "filename": "fastapi.log",
            "maxBytes": 2097152,  # 2 MB
            "backupCount": 3,
        },
    },
    "loggers": {
        "app": {
            "handlers": ["console", "rotating_file"],
            "propagate": False,
            "level": "DEBUG",
        },
        "root": {"handlers": ["console", "rotating_file"]},
    },
}

dictConfig(log_config)
app_logger = logging.getLogger("app")


def get_app_logger() -> Logger:
    return app_logger


def execute_and_log(func: Callable, params: dict) -> Any:
    """
    Executes a given function with its parameters (passed as a dictionary of key-value pairs),
    logs the result if successful, or logs and re-raises any exceptions that occur.

    Args:
        func (callable): The function to be executed.
        params (dict): A dictionary of keyword arguments to pass to the function.

    Returns:
        Any: The result of the executed function.

    Raises:
        Exception: Re-raises any exception caught during function execution.
    """
    func_name = func.__name__
    log_msg = f"Attempting to execute function: '{func_name}' with parameters: {params}"
    app_logger.info(log_msg)
    try:
        result = func(**params)  # Unpack the dictionary as keyword arguments
        log_msg = f"Function '{func_name}' executed successfully. Result: {result}"
        app_logger.info(log_msg)
        return result
    except Exception as e:
        log_msg = (
            f"An exception occurred during execution of function '{func_name}': {e}"
        )
        app_logger.error(
            log_msg,
            exc_info=True,
        )
        raise  # Re-raise the exception after logging it
