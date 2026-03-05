import os
from datetime import datetime
from typing import Dict


_EMOJIS = {
    "INFO": "ℹ️",
    "WARNING": "⚠️",
    "ERROR": "❌",
    "DEBUG": "🐛",
}


class Logger:
    def __init__(self, name: str) -> None:
        self.name = name

    def _log(self, level: str, message: str) -> None:
        emoji = _EMOJIS.get(level, "")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{timestamp} | {level} {emoji} | {self.name} | {message}")

    def debug(self, message: str) -> None:
        if os.getenv("MODE") == "PROD":
            return
        self._log("DEBUG", message)

    def info(self, message: str) -> None:
        self._log("INFO", message)

    def warning(self, message: str) -> None:
        self._log("WARNING", message)

    def error(self, message: str) -> None:
        self._log("ERROR", message)


_loggers: Dict[str, Logger] = {}


def get_logger(name: str = __name__) -> Logger:
    if name not in _loggers:
        _loggers[name] = Logger(name)
    return _loggers[name]

