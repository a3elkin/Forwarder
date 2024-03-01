from typing import NamedTuple
from importer import Importer
from exporter import Exporter

class ParseResponse(NamedTuple):
    success: bool = False
    error_message: str = ''

class ImportResponse(NamedTuple):
    parse_response: ParseResponse = None
    importer: Importer = None

class ExportResponse(NamedTuple):
    parse_response: ParseResponse = None
    exporter: Exporter = None
