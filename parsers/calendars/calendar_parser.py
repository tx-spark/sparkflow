import re
from abc import ABC, abstractmethod

from models.calendar import Calendar


class CalendarParser(ABC):

    _DATE_PATTERN: re.Pattern = re.compile(r"(\w+)\s+(\d+),\s+(\d{4})")
    _MONTH_MATCH_IDX: int = 1
    _DAY_MATCH_IDX: int = 2
    _YEAR_MATCH_IDX: int = 3

    _MONTH_MAP: dict[str, int] = {
        "january": 1,
        "february": 2,
        "march": 3,
        "april": 4,
        "may": 5,
        "june": 6,
        "july": 7,
        "august": 8,
        "september": 9,
        "october": 10,
        "november": 11,
        "december": 12,
    }

    _ORDINAL_MAP: dict[str, int] = {
        "first": 1,
        "second": 2,
        "third": 3,
        "fourth": 4,
        "fifth": 5,
        "sixth": 6,
        "seventh": 7,
        "eighth": 8,
        "ninth": 9,
        "tenth": 10,
    }

    @abstractmethod
    def parse(self, data: str) -> Calendar:
        raise NotImplementedError()
