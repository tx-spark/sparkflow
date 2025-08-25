from abc import ABC, abstractmethod

from models.calendar import Calendar


class CalendarParser(ABC):

    @abstractmethod
    def parse(self, data: str) -> Calendar:
        raise NotImplementedError()
