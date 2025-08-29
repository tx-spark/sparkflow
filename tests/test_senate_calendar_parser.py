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

    def test_parse_regular_calendar(self) -> None:
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

    def test_parse_regular_calendar2(self) -> None:
        regular_calendar_path: Path = self._SENATE_CALENDAR_PARSER / "regular2.htm"

        with open(regular_calendar_path, "r", encoding="utf-8") as fs:
            contents: str = fs.read()

        expected: Calendar = Calendar(
            chamber=Chamber.SENATE,
            calendar_type="REGULAR ORDER OF BUSINESS",
            calendar_date=datetime(2025, 2, 5),
            subcalendars=[
                Subcalendar(
                    reading_count=2,
                    subcalendar_type="SENATE JOINT RESOLUTIONS",
                    bill_ids=["SJR 36"],
                ),
                Subcalendar(
                    reading_count=2,
                    subcalendar_type="SENATE BILLS",
                    bill_ids=["SB 2"],
                ),
            ],
        )

        assert self._parser.parse(contents) == expected

    def test_parse_returned(self) -> None:
        returned_path: Path = self._SENATE_CALENDAR_PARSER / "returned.htm"

        with open(returned_path, "r", encoding="utf-8") as fs:
            contents: str = fs.read()

        expected: Calendar = Calendar(
            chamber=Chamber.SENATE,
            calendar_type="SENATE BILLS RETURNED FROM HOUSE WITH AMENDMENTS",
            calendar_date=datetime(2025, 8, 27),
            subcalendars=[
                Subcalendar(
                    reading_count=1,
                    subcalendar_type="",
                    bill_ids=["SB 3", "SB 16"],
                ),
            ],
        )

        assert self._parser.parse(contents) == expected

    def test_parse_floor_action(self) -> None:
        floor_action_path: Path = self._SENATE_CALENDAR_PARSER / "floor_action.htm"

        with open(floor_action_path, "r", encoding="utf-8") as fs:
            contents: str = fs.read()

        expected: Calendar = Calendar(
            chamber=Chamber.SENATE,
            calendar_type="SENATE FLOOR ACTION",
            calendar_date=datetime(2025, 8, 27),
            subcalendars=[
                Subcalendar(
                    reading_count=1,
                    subcalendar_type="",
                    bill_ids=[
                        "SB 3",
                        "SB 16",
                        "HB 8",
                        "HB 25",
                        "HB 26",
                        "HB 48",
                        "HB 149",
                        "HB 192",
                        "HB 254",
                    ],
                ),
            ],
        )

        assert self._parser.parse(contents) == expected

    def test_parse_floor_action2(self) -> None:
        floor_action_path: Path = self._SENATE_CALENDAR_PARSER / "floor_action2.htm"

        with open(floor_action_path, "r", encoding="utf-8") as fs:
            contents: str = fs.read()

        expected: Calendar = Calendar(
            chamber=Chamber.SENATE,
            calendar_type="SENATE FLOOR ACTION",
            calendar_date=datetime(2025, 8, 27),
            subcalendars=[
                Subcalendar(
                    reading_count=1,
                    subcalendar_type="",
                    bill_ids=[
                        "HB 8",
                        "HB 25",
                        "HB 26",
                        "HB 192",
                        "SB 54",
                    ],
                ),
            ],
        )

        assert self._parser.parse(contents) == expected

    def test_parse_referred(self) -> None:
        referred_path: Path = self._SENATE_CALENDAR_PARSER / "referred.htm"

        with open(referred_path, "r", encoding="utf-8") as fs:
            contents: str = fs.read()

        expected: Calendar = Calendar(
            chamber=Chamber.SENATE,
            calendar_type="BILLS REFERRED TODAY",
            calendar_date=datetime(2025, 8, 27),
            subcalendars=[
                Subcalendar(
                    reading_count=1,
                    subcalendar_type="",
                    bill_ids=[
                        "HB 8",
                        "HB 25",
                        "HB 26",
                        "HB 48",
                        "HB 149",
                        "HB 192",
                        "HB 254",
                    ],
                ),
            ],
        )

        assert self._parser.parse(contents) == expected

    def test_parse_intent(self) -> None:
        intent_path: Path = self._SENATE_CALENDAR_PARSER / "intent.htm"

        with open(intent_path, "r", encoding="utf-8") as fs:
            contents: str = fs.read()

        expected: Calendar = Calendar(
            chamber=Chamber.SENATE,
            calendar_type="NOTICE OF INTENT",
            calendar_date=datetime(2025, 4, 9),
            subcalendars=[
                Subcalendar(
                    reading_count=1,
                    subcalendar_type="",
                    bill_ids=[
                        "SJR 4",
                        "SJR 40",
                        "SJR 81",
                        "SCR 37",
                        "SCR 39",
                        "SB 22",
                        "SB 32",
                        "SB 33",
                        "SB 36",
                        "SB 38",
                        "SB 95",
                        "SB 209",
                        "SB 249",
                        "SB 311",
                        "SB 326",
                        "SB 365",
                        "SB 458",
                        "SB 609",
                        "SB 660",
                        "SB 664",
                        "SB 693",
                        "SB 732",
                        "SB 745",
                        "SB 760",
                        "SB 762",
                        "SB 779",
                        "SB 783",
                        "SB 785",
                        "SB 868",
                        "SB 871",
                        "SB 883",
                        "SB 921",
                        "SB 955",
                        "SB 993",
                        "SB 996",
                        "SB 1008",
                        "SB 1057",
                        "SB 1067",
                        "SB 1151",
                        "SB 1171",
                        "SB 1210",
                        "SB 1255",
                        "SB 1265",
                        "SB 1267",
                        "SB 1271",
                        "SB 1307",
                        "SB 1313",
                        "SB 1316",
                        "SB 1318",
                        "SB 1321",
                        "SB 1332",
                        "SB 1365",
                        "SB 1426",
                        "SB 1470",
                        "SB 1484",
                        "SB 1494",
                        "SB 1559",
                        "SB 1592",
                        "SB 1596",
                        "SB 1598",
                        "SB 1637",
                        "SB 1677",
                        "SB 1706",
                        "SB 1758",
                        "SB 1762",
                        "SB 1786",
                        "SB 1809",
                        "SB 1818",
                        "SB 1822",
                        "SB 1841",
                        "SB 1871",
                        "SB 1967",
                        "SB 2064",
                        "SB 2077",
                        "SB 2112",
                        "SB 2148",
                        "SB 2320",
                        "SB 2406",
                        "SB 2407",
                    ],
                ),
            ],
        )

        assert self._parser.parse(contents) == expected
