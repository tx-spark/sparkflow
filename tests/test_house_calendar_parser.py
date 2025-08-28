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

    def test_parse_prefiled_amendments(self) -> None:
        prefiled_amendments_path: Path = (
            self._HOUSE_CALENDAR_PARSER / "prefiled_amendments.htm"
        )

        with open(prefiled_amendments_path, "r", encoding="utf-8") as fs:
            contents: str = fs.read()

        expected: Calendar = Calendar(
            chamber=Chamber.HOUSE,
            calendar_type="PREFILED AMENDMENTS CALENDAR",
            calendar_date=datetime(2025, 8, 21),
            subcalendars=[
                Subcalendar(
                    reading_count=1,
                    subcalendar_type="",
                    bill_ids=["HB 1"],
                ),
            ],
        )

        assert self._parser.parse(contents) == expected

    def test_parse_supplemental_calendar(self) -> None:
        supplemental_path: Path = self._HOUSE_CALENDAR_PARSER / "supplemental.htm"

        with open(supplemental_path, "r", encoding="utf-8") as fs:
            contents: str = fs.read()

        expected: Calendar = Calendar(
            chamber=Chamber.HOUSE,
            calendar_type="SUPPLEMENTAL HOUSE CALENDAR",
            calendar_date=datetime(2025, 8, 15),
            subcalendars=[
                Subcalendar(
                    reading_count=2,
                    subcalendar_type="MAJOR STATE CALENDAR",
                    bill_ids=["HB 4", "HB 1", "HB 2", "HB 18", "HB 19", "HB 20"],
                ),
            ],
        )

        assert self._parser.parse(contents) == expected

    def test_parse_memorial_calendar(self) -> None:
        memorial_path: Path = self._HOUSE_CALENDAR_PARSER / "memorial.htm"

        with open(memorial_path, "r", encoding="utf-8") as fs:
            contents: str = fs.read()

        expected: Calendar = Calendar(
            chamber=Chamber.HOUSE,
            calendar_type="CONGRATULATORY AND MEMORIAL CALENDAR",
            calendar_date=datetime(2025, 6, 1),
            subcalendars=[
                Subcalendar(
                    reading_count=1,
                    subcalendar_type="CONGRATULATORY RESOLUTIONS",
                    bill_ids=[
                        "HCR 158",
                        "HCR 159",
                        "HCR 160",
                        "HCR 161",
                        "HCR 162",
                        "HCR 163",
                        "HCR 164",
                        "HR 174",
                        "HR 427",
                        "HR 790",
                        "HR 810",
                        "HR 857",
                        "HR 866",
                        "HR 882",
                        "HR 883",
                        "HR 962",
                        "HR 966",
                        "HR 1001",
                        "HR 1045",
                        "HR 1085",
                        "HR 1097",
                    ],
                ),
            ],
        )

        assert self._parser.parse(contents) == expected

    def test_parse_items_for_consideration(self) -> None:
        consideration_path: Path = self._HOUSE_CALENDAR_PARSER / "consideration.htm"

        with open(consideration_path, "r", encoding="utf-8") as fs:
            contents: str = fs.read()

        expected: Calendar = Calendar(
            chamber=Chamber.HOUSE,
            calendar_type="ITEMS ELIGIBLE FOR CONSIDERATION",
            calendar_date=datetime(2025, 6, 1),
            subcalendars=[
                Subcalendar(
                    reading_count=1,
                    subcalendar_type="CONFERENCE COMMITTEE",
                    bill_ids=[
                        "SB 1637",
                        "SB 2878",
                        "HB 2885",
                        "HB 2017",
                        "HB 5246",
                        "SB 8",
                        "SB 2308",
                        "SB 1405",
                        "HB 119",
                        "SB 3059",
                        "SB 15",
                        "SB 568",
                        "SB 2900",
                        "HB 3642",
                        "HB 3909",
                        "SB 268",
                        "HB 493",
                        "SB 2217",
                        "HB 2516",
                        "SB 650",
                        "HB 2963",
                        "SB 1610",
                        "HB 705",
                        "SB 2972",
                        "SB 1540",
                        "HB 40",
                        "SB 2753",
                    ],
                ),
            ],
        )

        assert self._parser.parse(contents) == expected
