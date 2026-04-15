from dataclasses import dataclass


@dataclass(frozen=True)
class Subcalendar:
    reading_count: int
    subcalendar_type: str
    bill_ids: list[str]
