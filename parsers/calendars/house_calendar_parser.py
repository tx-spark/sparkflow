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
        elif "CONGRATULATORY" in text and "MEMORIAL" in text:
            return "CONGRATULATORY AND MEMORIAL CALENDAR"
        elif "SUPPLEMENTAL" in text and "HOUSE" in text:
            return "SUPPLEMENTAL HOUSE CALENDAR"
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

    def _extract_subcalendars(self, data: str) -> list[Subcalendar]:
        """Extract subcalendars from the HTML."""
        # Get the calendar type to determine parsing method
        calendar_type = self._extract_calendar_type(data)

        # Check if this is a prefiled amendments calendar
        if "PRE-FILED AMENDMENTS" in calendar_type:
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
