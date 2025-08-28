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

    def parse(self, data: str) -> Calendar:
        soup: BeautifulSoup = BeautifulSoup(data, "html.parser")

        calendar_type: str = self._extract_calendar_type(soup)
        calendar_date: datetime | None = self._extract_calendar_date(soup)

        # Extract subcalendars
        subcalendars = self._extract_subcalendars(data, calendar_type)

        return Calendar(
            chamber=Chamber.HOUSE,
            calendar_type=calendar_type,
            calendar_date=calendar_date,
            subcalendars=subcalendars,
        )

    def _extract_calendar_type(self, soup: BeautifulSoup) -> str:
        p_tags = soup.find_all("p")
        if len(p_tags) > 0:
            _, title_tag, *__ = p_tags
            return title_tag.find("span").get_text(strip=True).replace("*", "").upper()

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
        p_tags = soup.find_all("p")

        if len(p_tags) > 0:
            _, __, date_tag, *___ = p_tags
            return date_tag.find("span").get_text(strip=True)
        else:
            title_tag = soup.find("title")
            if title_tag is not None:
                return title_tag.get_text(strip=True).split(" - ")[1]

        return ""

    def _extract_bill_ids_from_text(self, text: str) -> list[str]:
        """Extract and format bill IDs from text content.

        Args:
            text: The text content to search for bill IDs

        Returns:
            List of formatted bill IDs with proper spacing (e.g., ['HB 17', 'SB 10'])
        """
        # Find all bill IDs using regex pattern
        bill_pattern = r"Bill=([A-Z]+\s*\d+)"
        bill_matches = re.findall(bill_pattern, text)

        # Ensure proper spacing in bill IDs
        bill_ids = [
            re.sub(r"([A-Z]+)(\d+)", r"\1 \2", bill.replace(" ", ""))
            for bill in bill_matches
        ]

        return bill_ids

    def _extract_subcalendars(self, data: str, calendar_type: str) -> list[Subcalendar]:
        # Check if this is a prefiled amendments calendar
        if "PREFILED AMENDMENTS" in calendar_type:
            return self._extract_prefiled_amendments_subcalendars(data)

        # Check if this is a memorial calendar
        elif "CONGRATULATORY AND MEMORIAL CALENDAR" in calendar_type:
            return self._extract_memorial_calendar_subcalendars(data)

        # Otherwise handle as daily calendar
        return self._extract_daily_calendar_subcalendars(data)

    def _extract_prefiled_amendments_subcalendars(self, data: str) -> list[Subcalendar]:
        """Extract subcalendars from prefiled amendments format."""
        # Extract all bill IDs from the data
        bill_ids = self._extract_bill_ids_from_text(data)

        if bill_ids:
            return [
                Subcalendar(
                    reading_count=1,
                    subcalendar_type="",
                    bill_ids=bill_ids,
                )
            ]

        return []

    def _extract_memorial_calendar_subcalendars(self, data: str) -> list[Subcalendar]:
        """Extract subcalendars from memorial calendar format."""
        subcalendars = []

        # Look for sections marked with asterisks like "CONGRATULATORY RESOLUTIONS"
        # Pattern matches: ********** SECTION NAME **********
        # Handle HTML tags that might be mixed in
        section_pattern = r"\*{10}\s*([^*<]+?)(?:<[^>]*>)?\s*\*{10}"
        sections = re.findall(section_pattern, data)

        for section_name in sections:
            section_name = section_name.strip()

            # Find the content after this section header
            section_start_pattern = rf"\*{{10}}\s*{re.escape(section_name)}.*?\*{{10}}"
            section_match = re.search(section_start_pattern, data, re.DOTALL)

            if section_match:
                # Get content from after this section until the next section or end
                content_start = section_match.end()

                # Look for next section or end of content
                next_section_match = re.search(
                    r"\*{10}\s*[^*]+?\s*\*{10}", data[content_start:]
                )
                if next_section_match:
                    content_end = content_start + next_section_match.start()
                else:
                    content_end = len(data)

                section_content = data[content_start:content_end]

                # Extract bill IDs from this section
                bill_ids = self._extract_bill_ids_from_text(section_content)

                if bill_ids:
                    subcalendars.append(
                        Subcalendar(
                            reading_count=1,
                            subcalendar_type=section_name,
                            bill_ids=bill_ids,
                        )
                    )

        return subcalendars

    def _extract_daily_calendar_subcalendars(self, data: str) -> list[Subcalendar]:
        """Extract subcalendars from daily calendar format."""
        subcalendars = []

        # Split the content by major sections
        parts = re.split(
            r"\*{10}\s*(MAJOR STATE CALENDAR|GENERAL STATE CALENDAR)\s*\*{10}", data
        )

        i = 1
        while i < len(parts):
            if i + 1 < len(parts):
                section_type = parts[i].strip()
                section_content = parts[i + 1]

                # Parse subsections within this calendar section
                # Look for bill type headers like "HOUSE BILLS" or "SENATE BILLS"
                bill_type_sections = self._parse_bill_type_sections(section_content)

                # Create subcalendars for each bill type section found
                for bill_type, bills in bill_type_sections.items():
                    if bills:
                        subcalendars.append(
                            Subcalendar(
                                reading_count=2,
                                subcalendar_type=section_type,
                                bill_ids=bills,
                            )
                        )

            i += 2

        return subcalendars

    def _parse_bill_type_sections(self, section_content: str) -> dict[str, list[str]]:
        """Parse bill type sections and return bills grouped by type.

        Args:
            section_content: Content of a calendar section

        Returns:
            Dictionary mapping bill types to lists of bill IDs
        """
        bill_sections = {}

        # Split by bill type headers (HOUSE BILLS, SENATE BILLS, etc.)
        # This regex captures the bill type and includes everything until the next bill type or end
        bill_type_pattern = r"([A-Z]+\s+BILLS)\s*\n.*?(?=(?:[A-Z]+\s+BILLS)|$)"
        matches = re.findall(bill_type_pattern, section_content, re.DOTALL)

        # If no matches found with the above pattern, try a simpler approach
        if not matches:
            # Look for bill type headers and split content accordingly
            bill_type_splits = re.split(r"([A-Z]+\s+BILLS)", section_content)

            # Process the splits - every odd index is a bill type, every even index is content
            for i in range(1, len(bill_type_splits), 2):
                if i + 1 < len(bill_type_splits):
                    bill_type = bill_type_splits[i].strip()
                    bill_content = bill_type_splits[i + 1]

                    # Extract bills from this section
                    bills = self._extract_bill_ids_from_text(bill_content)
                    if bills:
                        bill_sections[bill_type] = bills
        else:
            # Process matches from the more complex pattern
            for bill_type in matches:
                # Find the content for this bill type
                bill_type_match = re.search(
                    rf"{re.escape(bill_type)}\s*.*?(?=(?:[A-Z]+\s+BILLS)|$)",
                    section_content,
                    re.DOTALL,
                )
                if bill_type_match:
                    bill_content = bill_type_match.group(0)
                    bills = self._extract_bill_ids_from_text(bill_content)
                    if bills:
                        bill_sections[bill_type.strip()] = bills

        return bill_sections
