import re
from datetime import datetime
from itertools import pairwise

from bs4 import BeautifulSoup, Tag

from models.calendar import Calendar
from models.chamber import Chamber
from models.subcalendar import Subcalendar

from .calendar_parser import CalendarParser


class SenateCalendarParser(CalendarParser):

    def parse(self, data: str) -> Calendar:
        soup: BeautifulSoup = BeautifulSoup(data, "html.parser")

        return Calendar(
            chamber=Chamber.SENATE,
            calendar_type=self._extract_calendar_type(soup),
            calendar_date=self._extract_calendar_date(soup),
            subcalendars=self._extract_subcalendars(soup),
        )

    def _extract_calendar_type(self, soup: BeautifulSoup) -> str:
        if len(p_tags := soup.find_all("p")) > 0:
            title_tag, *_ = p_tags

            return title_tag.find("span").get_text(strip=True).upper()
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

        return ""

    def _extract_subcalendars(self, soup: BeautifulSoup) -> list[Subcalendar]:
        tables = (
            soup.find("body")
            .find("center")
            .find_all(
                "table",
                recursive=False,
            )
        )

        raw_parsed_tables: list[list[str] | None] = [
            self._parse_table(table) for table in tables
        ]
        valid_table_contents: list[list[str]] = [
            table for table in raw_parsed_tables if table is not None
        ]

        header_indices: list[int] = [
            idx
            for idx, table_contents in enumerate(valid_table_contents)
            if self._is_subcalendar_header(table_contents)
        ]

        if len(header_indices) == 0:
            bill_ids: list[str] = [
                bill_id
                for table_content in valid_table_contents
                for bill_id in table_content
                if len(table_content) == 1
            ]
            return [
                Subcalendar(
                    reading_count=1,
                    subcalendar_type="",
                    bill_ids=bill_ids,
                )
            ]

        header_indices.append(len(valid_table_contents))

        raw_subcalendars: list[Subcalendar] = [
            self._parse_subcalendar(
                valid_table_contents[start_idx],
                valid_table_contents[start_idx + 1 : end_idx],
            )
            for start_idx, end_idx in pairwise(header_indices)
        ]

        return [
            subcalendar
            for subcalendar in raw_subcalendars
            if len(subcalendar.bill_ids) > 0
        ]

    def _parse_table(self, table: Tag) -> list[str] | None:
        if len(a_tags := table.find_all("a")) > 0:
            return [a_tag.get_text(strip=True) for a_tag in a_tags]

        if len(p_tags := table.find_all("p")) > 1:
            return [p_tag.get_text(strip=True) for p_tag in p_tags]

        return None

    def _is_subcalendar_header(self, table_contents: list[str]) -> bool:
        if len(table_contents) != 2:
            return False

        first_line, second_line = table_contents

        if not any(chamber.name.upper() in first_line.upper() for chamber in Chamber):
            return False

        return all(paren in second_line for paren in "()")

    def _parse_subcalendar(
        self, header_contents: list[str], bill_contents: list[list[str]]
    ) -> Subcalendar:
        subcalendar_type, reading_str = header_contents

        return Subcalendar(
            reading_count=self._get_reading_count(reading_str),
            subcalendar_type=subcalendar_type,
            bill_ids=[
                bill_id for bill_content in bill_contents for bill_id in bill_content
            ],
        )

    def _get_reading_count(self, reading_str: str) -> int:
        for ordinal_str, ordinal_int in self._ORDINAL_MAP.items():
            if ordinal_str in reading_str.lower():
                return ordinal_int

        return 1
