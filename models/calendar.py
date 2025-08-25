from dataclasses import dataclass
from datetime import datetime

from .chamber import Chamber


@dataclass(frozen=True)
class Calendar:
    chamber: Chamber
    calendar_type: str
    calendar_date: datetime
    bill_ids: list[str]
