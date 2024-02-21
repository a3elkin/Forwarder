from datatypes import DataType, CSV_DATA
import requests
import sys
import logging
import csv

class Importer:
    get_url: str
    csv_data: CSV_DATA

    def __init__(self, get_url) -> None:
        self.get_url = get_url
        self.csv_data = CSV_DATA

        formatter = logging.Formatter(fmt = '%(asctime)s %(levelname)s: %(message)s', datefmt='%d-%b-%y %H:%M:%S')

        def setup_info_logger(name, log_file, level=logging.INFO):
            logger = logging.getLogger(name)
            logger.setLevel(level)

            handler = logging.FileHandler(log_file)
            handler.setFormatter(formatter)
            logger.addHandler(handler)

            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(formatter)
            logger.addHandler(handler)

            return logger

        def setup_err_logger(name, log_file, level=logging.INFO):
            handler = logging.FileHandler(log_file)
            handler.setFormatter(formatter)

            logger = logging.getLogger(name)
            logger.setLevel(level)
            logger.addHandler(handler)

            return logger

        self.log_info = setup_info_logger('info_log','forwarder.log')
        self.log_error = setup_err_logger('error_log','forwarder.err')


    def get_data(self, datatype: DataType, delimiter: str = None) -> bool:
        headers = {'Content-Type': 'application/csv', 'Cache-Control': 'no-cache'}
    
        try:
            r = requests.get(self.get_url, headers = headers)
        except Exception as ex:
            self.log_error.error("Request error: %s", str(ex))
            return False
        
        if r.status_code != 200:
            self.log_error.error("Request error: status %s, message %s", r.status_code, r.text)
            return False
        
        self.log_info.info("Download completed successfully")
        
        if datatype == DataType.CSV:
            self.csv_data.fields = []
            self.csv_data.data = []

            tmp_file = 'csv.tmp' 
            with open(tmp_file, "w") as tmp:
                tmp.write(r.text)
            with open(tmp_file, "r") as tmp:
                csv_reader = csv.reader(tmp, delimiter=delimiter)

                line_count = 0
                for row in csv_reader:
                    if line_count == 0:
                        self.csv_data.fields = row
                    else:
                        self.csv_data.data.append(row)
                    line_count += 1
            
            self.log_info.info("Records: %s", line_count-1)