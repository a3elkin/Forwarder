from typing import NamedTuple

class QueryResponse(NamedTuple):
    success: bool = False
    status: str = ''
    error_message: str = ''
    data: str = ''
    response: dict = {}
