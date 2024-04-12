import ssl
import sys
import os
import logging
import shelve
from yaml import safe_load
from dotenv import load_dotenv
from datatypes import DataType
from exporter import Marketplaces, Exporter
from data_response import ParseResponse, ImportResponse, ExportResponse
import wildberries
import ozon
from importer import Importer

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

log_info = setup_info_logger('info_log','forwarder.log')
log_error = setup_err_logger('error_log','forwarder.err')

load_dotenv()

all_base = {}

if hasattr(ssl, '_create_unverified_context'):
    ssl._create_default_https_context = ssl._create_unverified_context

def get_exporter(market: Marketplaces, data: dict) -> Exporter:
    if market == Marketplaces.WB:
        return wildberries.Wildberries({'token': os.getenv('WB_TOKEN'), 'warehouseId': os.getenv('WB_WAREHOUSE_ID')}, data=data, log_info=log_info.info, log_error=log_error.error)
    elif market == Marketplaces.OZON:
        return ozon.Ozon({'key': os.getenv('OZON_KEY'), 'clientId': os.getenv('OZON_CLIENT_ID'), 'warehouseId': os.getenv('OZON_WAREHOUSE_ID')}, data=data, log_info=log_info.info, log_error=log_error.error)
    else:
        return None

def import_task(task: dict) -> ImportResponse:
    task_id = task.get('id','')
    task_name = task.get('name','')
    if not task_id:
        return ImportResponse(parse_response=ParseResponse(success=False, error_message=f'ID of task {task_name} is not defined'), importer=None)

    if 'URL' not in task:
        return ImportResponse(parse_response=ParseResponse(success=False, error_message=f'URL of task {task_id} ({task_name}) is not defined'), importer=None)
    
    datatype = None
    delimiter = None
    type_from_config = task.get('type','').upper()

    if  type_from_config == 'CSV':
        datatype = DataType.CSV
        delimiter = task.get('delimiter',';')
    elif type_from_config == 'XML':
        datatype = DataType.XML            

    if not datatype:
        return ImportResponse(parse_response=ParseResponse(success=False, error_message=f'Type of data is not defined in task {task_id} ({task_name})'), importer=None)

    importer = Importer(url=task['URL'], datatype=datatype, delimiter=delimiter, log_info=log_info.info, log_error=log_error.error)
    
    if not importer.import_data():
        return ImportResponse(parse_response=ParseResponse(success=False, error_message='Error while import data!'), importer=None)

    log_info.info('Import completed successfully')
    return ImportResponse(parse_response=ParseResponse(success=True), importer=importer)
    
    
def export_task(task_id: str, export: dict, importer: Importer) -> ExportResponse:
    if not export.get('active', True):
        return ExportResponse(parse_response=ParseResponse(success=True), exporter=None)

    reset_missing = export.get('reset_missing', False)                        

    try:
        market = Marketplaces[export['marketplace']]
        log_info.info(f"Starting task {market.value}")
    except Exception:
        return ExportResponse(parse_response=ParseResponse(success=False, error_message=f'Unkmown code of marketplace {export['marketplace']}'), exporter=None)

    if 'schema' not in export:
        return ExportResponse(parse_response=ParseResponse(success=False, error_message=f'Schema is not defined for {export['marketplace']}'), exporter=None)
    
    schema = dict(export['schema'])
    
    code_column = schema.get('code', 0)
    amount_column = schema.get('amount', 0)
    prefix = schema.get('prefix','')
    
    if code_column <= 0:
        return ExportResponse(parse_response=ParseResponse(success=False, error_message=f'Column for code is not defined in schema in {export['marketplace']}'), exporter=None)
    if amount_column <= 0:
        return ExportResponse(parse_response=ParseResponse(success=False, error_message=f'Column for amount is not defined in schema in {export['marketplace']}'), exporter=None)

    export_data = importer.get_data({'code': code_column, 'amount': amount_column}, prefix=prefix) #{code: {amount: amount}}
    market_code = market.name

    with shelve.open(file_db) as db:
        task_market = f"{task_id}@{market_code}"
        if reset_missing:
            market_db = db.get(task_market, {})                        
            for row in market_db:
                if row not in export_data:
                    if len(row): #not empty code
                        export_data[row] = {'amount': 0}
            
        db[task_market] = dict(export_data)                            
    
    return ExportResponse(parse_response=ParseResponse(success=True), exporter=get_exporter(market, export_data))
    
def parse_task(task: dict) -> ParseResponse:
    import_result: ImportResponse = import_task(task) 
    if not import_result.parse_response.success or not import_result.importer:
        return import_result.parse_response
    
    importer = import_result.importer
    
    if 'export' in task:
        for export in task['export']:
            export_result = export_task(task_id=task['id'], export=dict(export), importer=importer)

            if not export_result.parse_response.success or not export_result.exporter:
                return export_result.parse_response
            
            exporter = export_result.exporter

            if not exporter.export(base=all_base):
                log_info.info("Export finished with errors!")
            else:
                log_info.info('Export completed successfully')
            
    return ParseResponse(success=True)
            

if __name__ == '__main__':
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    else:
        config_file = 'config.yml'
    
    try:
        with open(config_file, 'r') as f:
            config = safe_load(f)
    except Exception as ex:
        log_error.error("Error while loading config file %s: %s", config_file, str(ex))
        sys.exit(10)

    if 'tasks' not in config:
        log_error.error("Section 'tasks' is not found in config file %s", config_file)
        sys.exit(20)
    
    file_db = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'forwarder.db')

    for task in config['tasks']:
        result: ParseResponse = parse_task(task)
        if not result.success:
            log_error.error(result.error_message)
