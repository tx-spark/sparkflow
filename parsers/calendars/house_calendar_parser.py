import re
from datetime import datetime
from typing import Optional

from bs4 import BeautifulSoup, Tag

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
        """Extract the calendar type from the HTML using BeautifulSoup."""
        try:
            soup = BeautifulSoup(data, "html.parser")

            # Strategy 1: Look for calendar type in the second <p> tag (works for most calendars)
            p_tags = soup.find_all("p")
            if len(p_tags) >= 2:
                second_p = p_tags[1]
                if isinstance(second_p, Tag):
                    span = second_p.find("span")

                    if span and isinstance(span, Tag) and span.get_text(strip=True):
                        calendar_type_text = span.get_text(strip=True)

                        # Clean up and extract just the calendar type (remove asterisks and extra content)
                        lines = calendar_type_text.split("\n")
                        for line in lines:
                            clean_line = line.strip().strip("*").strip()
                            if clean_line and len(clean_line) > 5:
                                # Skip lines that are just asterisks or very short
                                if not re.match(r"^[\*\s]*$", clean_line):
                                    return self._normalize_calendar_type(clean_line)

            # Strategy 2: Look for calendar type in centered table cells (works for prefiled amendments)
            # Find all elements that might contain calendar type text
            calendar_keywords = ["calendar", "amendment", "resolution"]
            for element in soup.find_all(["td", "span", "div"], align="center"):
                text = element.get_text(strip=True)
                # Clean up HTML entities like &nbsp;
                text = text.replace("\xa0", " ").replace("&nbsp;", " ")

                # Check if this looks like a calendar type
                text_lower = text.lower()
                if any(keyword in text_lower for keyword in calendar_keywords):
                    # Skip very short text or text that looks like dates
                    if len(text) > 5 and not re.match(r"^\w+day,", text):
                        return self._normalize_calendar_type(text)

            # Strategy 3: Fallback - look for any element with calendar-related text
            for element in soup.find_all(["span", "td", "div"]):
                text = element.get_text(strip=True)
                text = text.replace("\xa0", " ").replace("&nbsp;", " ")
                text_lower = text.lower()

                if ("calendar" in text_lower or "amendment" in text_lower) and len(
                    text
                ) > 5:
                    # Skip dates and very generic text
                    if not re.match(r"^\w+day,", text) and "by" not in text_lower:
                        clean_text = text.strip().strip("*").strip()
                        if len(clean_text) > 5:
                            return self._normalize_calendar_type(clean_text)

        except Exception:
            # If BeautifulSoup fails, fall back to default
            pass

        # Final fallback: Default to daily house calendar
        return "DAILY HOUSE CALENDAR"

    def _normalize_calendar_type(self, text: str) -> str:
        """Normalize calendar type text to standard format."""
        # Remove HTML entities and clean up
        text = re.sub(r"&\w+;", " ", text)
        text = re.sub(r"\s+", " ", text).strip().upper()

        # Define calendar type patterns and their normalized forms
        calendar_patterns = {
            r".*(?:PRE-FILED|PREFILED).*AMENDMENT.*": "LIST OF PRE-FILED AMENDMENTS",
            r".*CONGRATULATORY.*MEMORIAL.*CALENDAR.*": "CONGRATULATORY AND MEMORIAL CALENDAR",
            r".*SUPPLEMENTAL.*HOUSE.*CALENDAR.*": "SUPPLEMENTAL HOUSE CALENDAR",
            r".*DAILY.*HOUSE.*CALENDAR.*": "DAILY HOUSE CALENDAR",
            r".*HOUSE.*CALENDAR.*": "DAILY HOUSE CALENDAR",
            r".*AMENDMENT.*": "LIST OF PRE-FILED AMENDMENTS",
        }

        # Find the first matching pattern
        for pattern, normalized_type in calendar_patterns.items():
            if re.match(pattern, text):
                return normalized_type

        # Return the cleaned text as-is if no pattern matches
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
