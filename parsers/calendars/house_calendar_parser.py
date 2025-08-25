from datetime import datetime

from models.calendar import Calendar
from models.chamber import Chamber
from models.subcalendar import Subcalendar

from .calendar_parser import CalendarParser


class HouseCalendarParser(CalendarParser):

    def parse(self, data: str) -> Calendar:
        return Calendar(
            chamber=Chamber.HOUSE,
            calendar_type="",
            calendar_date=datetime.now(),
            subcalendars=[],
        )
