from datetime import datetime
from pathlib import Path
from unittest import TestCase

from models.calendar import Calendar
from models.chamber import Chamber
from models.subcalendar import Subcalendar
from parsers.calendars import CalendarParser, HouseCalendarParser


class TestHouseCalendarParser(TestCase):

    _PROJECT_PATH: Path = Path(__file__).parent.parent
    _TEST_PATH: Path = _PROJECT_PATH / "tests"
    _ASSETS_PATH: Path = _TEST_PATH / "assets"

    _HOUSE_CALENDAR_PARSER: Path = _ASSETS_PATH / "house"

    def setUp(self) -> None:
        self._parser: CalendarParser = HouseCalendarParser()

    def test_parse_daily_calendar(self) -> None:
        daily_calendar_path: Path = self._HOUSE_CALENDAR_PARSER / "daily.htm"

        with open(daily_calendar_path, "r", encoding="utf-8") as fs:
            contents: str = fs.read()

        expected: Calendar = Calendar(
            chamber=Chamber.HOUSE,
            calendar_type="DAILY HOUSE CALENDAR",
            calendar_date=datetime(2025, 8, 25),
            subcalendars=[
                Subcalendar(
                    reading_count=2,
                    subcalendar_type="MAJOR STATE CALENDAR",
                    bill_ids=["HB 17", "HB 16"],
                ),
                Subcalendar(
                    reading_count=2,
                    subcalendar_type="MAJOR STATE CALENDAR",
                    bill_ids=["SB 10"],
                ),
                Subcalendar(
                    reading_count=2,
                    subcalendar_type="GENERAL STATE CALENDAR",
                    bill_ids=["HB 27", "HB 23"],
                ),
                Subcalendar(
                    reading_count=2,
                    subcalendar_type="GENERAL STATE CALENDAR",
                    bill_ids=["SB 15", "SB 18"],
                ),
            ],
        )

        assert self._parser.parse(contents) == expected
