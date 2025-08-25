from dataclasses import dataclass
from datetime import datetime

from .chamber import Chamber
from .subcalendar import Subcalendar


@dataclass(frozen=True)
class Calendar:
    chamber: Chamber
    calendar_type: str
    calendar_date: datetime
    subcalendars: list[Subcalendar]
