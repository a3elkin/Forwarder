from enum import Enum

class DataType(Enum):
    CSV = 'CSV file'
    XML = 'XML file'

class CSV_DATA:
    fields: list
    data: list

    def __init__(self) -> None:
        self.fields = []
        self.data = []
