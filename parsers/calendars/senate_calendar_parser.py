from datetime import datetime

from bs4 import BeautifulSoup

from models.calendar import Calendar
from models.chamber import Chamber
from models.subcalendar import Subcalendar

from .calendar_parser import CalendarParser


class SenateCalendarParser(CalendarParser):

    def parse(self, data: str) -> Calendar:
        soup: BeautifulSoup = BeautifulSoup(data, "html.parser")

        return Calendar(
            chamber=Chamber.HOUSE,
            calendar_type=self._extract_calendar_type(soup),
            calendar_date=self._extract_calendar_date(soup),
            subcalendars=self._extract_subcalendars(soup),
        )

    def _extract_calendar_type(self, soup: BeautifulSoup) -> str:
        return ""

    def _extract_calendar_date(self, soup: BeautifulSoup) -> datetime | None:
        return datetime.min

    def _extract_subcalendars(self, soup: BeautifulSoup) -> list[Subcalendar]:
        return []
