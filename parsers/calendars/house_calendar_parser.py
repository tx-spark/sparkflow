import re
from datetime import datetime

from models.calendar import Calendar
from models.chamber import Chamber
from models.subcalendar import Subcalendar

from .calendar_parser import CalendarParser


class HouseCalendarParser(CalendarParser):

    def parse(self, data: str) -> Calendar:
        # Extract calendar type
        calendar_type = self._extract_calendar_type(data)

        # Extract calendar date
        calendar_date = self._extract_calendar_date(data)

        # Extract subcalendars
        subcalendars = self._extract_subcalendars(data)

        return Calendar(
            chamber=Chamber.HOUSE,
            calendar_type=calendar_type,
            calendar_date=calendar_date,
            subcalendars=subcalendars,
        )

    def _extract_calendar_type(self, data: str) -> str:
        """Extract the calendar type from the HTML dynamically."""
        # Look for title tag first
        title_match = re.search(r"<title>(.*?)</title>", data, re.IGNORECASE)
        if title_match:
            title_text = title_match.group(1).strip()
            # Extract calendar type from title - remove date information
            # Examples: "Prefiled Amendments Calendar - Thursday, August 21, 2025"
            #          "Daily House Calendar - Monday, August 25, 2025"
            calendar_type_match = re.search(
                r"^(.*?)\s*(?:-\s*\w+day,|\s*-)", title_text
            )
            if calendar_type_match:
                calendar_type = calendar_type_match.group(1).strip()
                # Normalize and format the calendar type
                return self._normalize_calendar_type(calendar_type)

        # Fallback: Look for calendar type in the body content
        # Look for centered text that appears to be a calendar title
        # Pattern for calendar titles (usually in center-aligned spans or divs)
        body_patterns = [
            r'align="?center"?[^>]*>([^<]*(?:calendar|amendments)[^<]*)<',
            r"text-align:\s*center[^>]*>([^<]*(?:calendar|amendments)[^<]*)<",
            r">\s*([A-Z][^<]*(?:CALENDAR|Calendar|AMENDMENTS|Amendments)[^<]*)\s*<",
        ]

        for pattern in body_patterns:
            matches = re.findall(pattern, data, re.IGNORECASE)
            for match in matches:
                # Clean up HTML entities and extra whitespace
                clean_text = re.sub(r"&\w+;", " ", match).strip()
                clean_text = re.sub(r"\s+", " ", clean_text)
                if len(clean_text) > 5 and (
                    "calendar" in clean_text.lower()
                    or "amendment" in clean_text.lower()
                ):
                    return self._normalize_calendar_type(clean_text)

        # Final fallback: Default to daily house calendar
        return "DAILY HOUSE CALENDAR"

    def _normalize_calendar_type(self, text: str) -> str:
        """Normalize calendar type text to standard format."""
        # Remove HTML entities and clean up
        text = re.sub(r"&\w+;", " ", text)
        text = re.sub(r"\s+", " ", text).strip().upper()

        # Normalize common variations
        if "PRE-FILED" in text or "PREFILED" in text:
            return "LIST OF PRE-FILED AMENDMENTS"
        elif "DAILY" in text and "HOUSE" in text:
            return "DAILY HOUSE CALENDAR"
        elif "HOUSE" in text and "CALENDAR" in text:
            return "DAILY HOUSE CALENDAR"
        elif "AMENDMENT" in text:
            return "LIST OF PRE-FILED AMENDMENTS"
        else:
            # Return the cleaned text as-is if we can't categorize it
            return text

    def _extract_calendar_date(self, data: str) -> datetime:
        """Extract the calendar date from the HTML."""
        # Look for date pattern like "Monday, August 25, 2025"
        date_pattern = r"(\w+day),\s+(\w+)\s+(\d+),\s+(\d+)"
        match = re.search(date_pattern, data)
        if match:
            month_name = match.group(2)
            day = int(match.group(3))
            year = int(match.group(4))

            # Convert month name to number
            month_map = {
                "January": 1,
                "February": 2,
                "March": 3,
                "April": 4,
                "May": 5,
                "June": 6,
                "July": 7,
                "August": 8,
                "September": 9,
                "October": 10,
                "November": 11,
                "December": 12,
            }
            month = month_map.get(month_name, 1)

            return datetime(year, month, day)

        return datetime.now()

    def _extract_bill_ids_from_text(
        self, text: str, bill_type_filter: str | None = None
    ) -> list[str]:
        """Extract and format bill IDs from text content.

        Args:
            text: The text content to search for bill IDs
            bill_type_filter: Optional filter for bill type ('HB', 'SB', etc.)

        Returns:
            List of formatted bill IDs with proper spacing (e.g., ['HB 17', 'SB 10'])
        """
        # Find all bill IDs using regex pattern
        bill_pattern = r"Bill=([A-Z]+\s*\d+)"
        bill_matches = re.findall(bill_pattern, text)

        # Filter by bill type if specified
        if bill_type_filter:
            bill_matches = [
                bill
                for bill in bill_matches
                if bill.replace(" ", "").startswith(bill_type_filter)
            ]

        # Ensure proper spacing in bill IDs
        bill_ids = [
            re.sub(r"([A-Z]+)(\d+)", r"\1 \2", bill.replace(" ", ""))
            for bill in bill_matches
        ]

        return bill_ids

    def _extract_subcalendars(self, data: str) -> list[Subcalendar]:
        """Extract subcalendars from the HTML."""
        # Get the calendar type to determine parsing method
        calendar_type = self._extract_calendar_type(data)

        # Check if this is a prefiled amendments calendar
        if "PRE-FILED AMENDMENTS" in calendar_type:
            return self._extract_prefiled_amendments_subcalendars(data)

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

                # Look for HOUSE BILLS and SENATE BILLS subsections
                house_bills = []
                senate_bills = []

                # Split by HOUSE BILLS and SENATE BILLS
                if "HOUSE BILLS" in section_content:
                    house_section_match = re.search(
                        r"HOUSE BILLS.*?(?=SENATE BILLS|$)", section_content, re.DOTALL
                    )
                    if house_section_match:
                        house_section = house_section_match.group(0)
                        house_bills = self._extract_bill_ids_from_text(
                            house_section, "HB"
                        )

                if "SENATE BILLS" in section_content:
                    senate_section_match = re.search(
                        r"SENATE BILLS.*", section_content, re.DOTALL
                    )
                    if senate_section_match:
                        senate_section = senate_section_match.group(0)
                        senate_bills = self._extract_bill_ids_from_text(
                            senate_section, "SB"
                        )

                # Add subcalendars for house bills first, then senate bills
                if house_bills:
                    subcalendars.append(
                        Subcalendar(
                            reading_count=2,
                            subcalendar_type=section_type,
                            bill_ids=house_bills,
                        )
                    )

                if senate_bills:
                    subcalendars.append(
                        Subcalendar(
                            reading_count=2,
                            subcalendar_type=section_type,
                            bill_ids=senate_bills,
                        )
                    )

            i += 2

        return subcalendars
