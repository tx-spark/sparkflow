import re
from datetime import datetime

from models.calendar import Calendar
from models.chamber import Chamber
from models.subcalendar import Subcalendar

from .calendar_parser import CalendarParser


class HouseCalendarParser(CalendarParser):

    def parse(self, data: str) -> Calendar:
        # Extract calendar type
        calendar_type = "DAILY HOUSE CALENDAR"

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

    def _extract_subcalendars(self, data: str) -> list[Subcalendar]:
        """Extract subcalendars from the HTML."""
        subcalendars = []

        # Find all bill IDs using regex - include space in the pattern
        bill_pattern = r"Bill=([A-Z]+\s*\d+)"
        bill_matches = re.findall(bill_pattern, data)

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
                        house_bill_matches = re.findall(bill_pattern, house_section)
                        house_bills = [
                            bill for bill in house_bill_matches if bill.startswith("HB")
                        ]
                        # Ensure proper spacing in bill IDs
                        house_bills = [
                            re.sub(r"([A-Z]+)(\d+)", r"\1 \2", bill.replace(" ", ""))
                            for bill in house_bills
                        ]

                if "SENATE BILLS" in section_content:
                    senate_section_match = re.search(
                        r"SENATE BILLS.*", section_content, re.DOTALL
                    )
                    if senate_section_match:
                        senate_section = senate_section_match.group(0)
                        senate_bill_matches = re.findall(bill_pattern, senate_section)
                        senate_bills = [
                            bill
                            for bill in senate_bill_matches
                            if bill.startswith("SB")
                        ]
                        # Ensure proper spacing in bill IDs
                        senate_bills = [
                            re.sub(r"([A-Z]+)(\d+)", r"\1 \2", bill.replace(" ", ""))
                            for bill in senate_bills
                        ]

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
