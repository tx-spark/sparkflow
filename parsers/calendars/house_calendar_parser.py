import re
from datetime import datetime
from typing import Sequence

from bs4 import BeautifulSoup, Tag
from bs4.element import PageElement

from models.calendar import Calendar
from models.chamber import Chamber
from models.subcalendar import Subcalendar

from .calendar_parser import CalendarParser


class HouseCalendarParser(CalendarParser):

    _DATE_PATTERN: re.Pattern = re.compile(r"\w+day,\s+(\w+)\s+(\d+),\s+(\d{4})")
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

    def parse(self, data: str) -> Calendar:
        soup: BeautifulSoup = BeautifulSoup(data, "html.parser")

        return Calendar(
            chamber=Chamber.HOUSE,
            calendar_type=self._extract_calendar_type(soup),
            calendar_date=self._extract_calendar_date(soup),
            subcalendars=self._extract_subcalendars(soup),
        )

    def _extract_calendar_type(self, soup: BeautifulSoup) -> str:
        if len(p_tags := soup.find_all("p")) > 0:
            for p_tag in p_tags:
                tag_text: str = p_tag.find("span").get_text(strip=True)

                if "*" in tag_text:
                    return tag_text.replace("*", "").upper()

        title_tag = soup.find("title")
        if title_tag is not None:
            return title_tag.get_text(strip=True).split(" - ")[0].upper()

        return ""

    def _extract_calendar_date(self, soup: BeautifulSoup) -> datetime | None:
        match: re.Match[str] | None = self._DATE_PATTERN.search(
            self._get_date_string(soup)
        )

        if match is not None:
            month_name: str = match.group(self._MONTH_MATCH_IDX)
            month: int | None = self._MONTH_MAP.get(month_name.lower())

            day: int = 1 if month is None else int(match.group(self._DAY_MATCH_IDX))
            year: int = int(match.group(self._YEAR_MATCH_IDX))

            return datetime(year, 1 if month is None else month, day)

        return None

    def _get_date_string(self, soup: BeautifulSoup) -> str:
        if len(p_tags := soup.find_all("p")) > 0:
            for p_tag in p_tags:
                tag_text: str = p_tag.find("span").get_text(strip=True)

                if self._DATE_PATTERN.search(tag_text) is not None:
                    return tag_text
        else:
            title_tag = soup.find("title")
            if title_tag is not None:
                return title_tag.get_text(strip=True).split(" - ")[1]

        return ""

    def _extract_subcalendars(self, soup: BeautifulSoup) -> list[Subcalendar]:
        if soup.find("p") is None:
            return self._parse_table_subcalendars(soup)

        return self._parse_paragraph_subcalendars(soup)

    def _parse_table_subcalendars(self, soup: BeautifulSoup) -> list[Subcalendar]:
        a_tags = soup.find_all("a")

        return [
            Subcalendar(
                reading_count=1,
                subcalendar_type="",
                bill_ids=[
                    a_tag.get_text(strip=True).replace("\xa0", " ").upper()
                    for a_tag in a_tags
                ],
            )
        ]

    def _parse_paragraph_subcalendars(self, soup: BeautifulSoup) -> list[Subcalendar]:
        toplevel_table = soup.find("body").find("div").find("table")

        # There's an extra blank row at the end of every table
        *rows, _ = toplevel_table.find_all("tr", recursive=False)

        return [self._parse_row_subcalendar(row) for row in rows]

    def _parse_row_subcalendar(self, row: Tag) -> Subcalendar:
        header_text = row.find("p").find("span").get_text()

        header_components: list[str] = [
            elem.strip() for elem in header_text.split("\n") if elem != ""
        ]
        subcalendar_type, *remaining_components = header_components

        if len(remaining_components) > 0:
            *_, reading_str = remaining_components
            ordinal, *_ = reading_str.split(" ")
            reading_count: int = self._ORDINAL_MAP.get(ordinal.lower(), 1)
        else:
            reading_count: int = 1

        a_tags = row.find_all("a")

        return Subcalendar(
            reading_count=reading_count,
            subcalendar_type=subcalendar_type.replace("*", "").strip(),
            bill_ids=[
                a_tag.get_text(strip=True).replace("\xa0", " ").upper()
                for a_tag in a_tags
            ],
        )
