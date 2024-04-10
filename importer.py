from datatypes import DataType, CSV_DATA
import requests
import csv
from io import StringIO
from typing import Callable

class Importer:
    url: str
    datatype: DataType
    delimiter: str
    csv_data: CSV_DATA
    log_info: callable
    log_error: callable

    def __init__(self, url: str, datatype: DataType, delimiter: str = None, log_info: Callable[[str], None] = None, log_error: Callable[[str], None] = None) -> None:
        self.url = url
        self.datatype = datatype
        self.delimiter = delimiter
        self.csv_data = CSV_DATA
        self.log_info = log_info
        self.log_error = log_error

    def get_data(self, schema: dict, prefix: str) -> dict:
        result = {}
        if self.datatype == DataType.CSV:
            if 'code' not in schema or 'amount' not in schema:
                return []
            if schema['code'] > len(self.csv_data.fields) or schema['amount'] > len(self.csv_data.fields):
                return []
            
            for row in self.csv_data.data:
                code = prefix + str(row[schema['code']-1]).strip()
                amount = int(row[schema['amount']-1])                
                if code and amount>=0:
                    result[code] = {'amount': amount} #{code: {amount: amount}} - maybe I'll need anything other than amount
            
        return result

    def import_data(self) -> bool:
        if self.log_info:
            self.log_info(f"Starting import from {self.url}")

        headers = {'Content-Type': 'application/csv', 'Cache-Control': 'no-cache'}
    
        try:
            r = requests.get(self.url, headers = headers)
        except Exception as ex:
            if self.log_error:
                self.log_error(f"Request error: {str(ex)}")
            return False
        
        if r.status_code != 200:
            if self.log_error:
                self.log_error(f"Request error: status {r.status_code}, message {r.text}")
            return False
        
        if self.log_info:
            self.log_info("Download completed successfully")
        
        if self.datatype == DataType.CSV:
            self.csv_data.fields = []
            self.csv_data.data = []

            f = StringIO(r.text)
            csv_reader = csv.reader(f, delimiter=self.delimiter)
            line_count = 0
            for row in csv_reader:
                if line_count == 0:
                    self.csv_data.fields = row
                else:
                    self.csv_data.data.append(row)
                line_count += 1
            
            if self.log_info:
                self.log_info(f"Records: {str(line_count-1)}")

        return True
