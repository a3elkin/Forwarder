from enum import Enum
from typing import Callable


class Marketplaces(Enum):
    WB = 'Wildberries'
    OZON = 'Ozon'

class Exporter:
    marketplace: Marketplaces
    authorization: dict
    log_info: Callable
    log_error: Callable

    def __init__(self) -> None:
        self.marketplace = None
        self.authorization = None
        self.log_info = None
        self.log_error = None


    def export(self, data: dict) -> bool:
        return False
    