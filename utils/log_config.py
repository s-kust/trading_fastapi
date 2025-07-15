import json
import logging
from datetime import datetime


# Custom JSON formatter
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        print(f"Inside format original levelname: {record.levelname}")
        log_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
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
            "maxBytes": 10485760,  # 10 MB
            "backupCount": 3,
        },
    },
    "loggers": {
        "app": {
            "handlers": ["console", "rotating_file"],
            "propagate": False,
        },
        "root": {"handlers": ["console", "rotating_file"]},
    },
}
