from abc import ABC, abstractmethod


class CalendarParser(ABC):

    @abstractmethod
    def parse(self, data: str):
        pass
