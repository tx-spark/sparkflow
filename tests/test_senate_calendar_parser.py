from datetime import datetime
from pathlib import Path
from unittest import TestCase

from models.calendar import Calendar
from models.chamber import Chamber
from models.subcalendar import Subcalendar
from parsers.calendars import CalendarParser, SenateCalendarParser


class TestHouseCalendarParser(TestCase):

    _PROJECT_PATH: Path = Path(__file__).parent.parent
    _TEST_PATH: Path = _PROJECT_PATH / "tests"
    _ASSETS_PATH: Path = _TEST_PATH / "assets"

    _SENATE_CALENDAR_PARSER: Path = _ASSETS_PATH / "senate"

    def setUp(self) -> None:
        self._parser: CalendarParser = SenateCalendarParser()

    def test_parse_daily_calendar(self) -> None:
        regular_calendar_path: Path = self._SENATE_CALENDAR_PARSER / "regular.htm"

        with open(regular_calendar_path, "r", encoding="utf-8") as fs:
            contents: str = fs.read()

        expected: Calendar = Calendar(
            chamber=Chamber.SENATE,
            calendar_type="REGULAR ORDER OF BUSINESS",
            calendar_date=datetime(2025, 9, 2),
            subcalendars=[
                Subcalendar(
                    reading_count=2,
                    subcalendar_type="SENATE BILLS",
                    bill_ids=["SB 9", "SB 7", "SB 17", "SB 4"],
                ),
                Subcalendar(
                    reading_count=2,
                    subcalendar_type="HOUSE BILLS",
                    bill_ids=["HB 17"],
                ),
            ],
        )

        assert self._parser.parse(contents) == expected
