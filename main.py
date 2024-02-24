import ssl
import sys
import os
import logging
import shelve
from yaml import safe_load
from dotenv import load_dotenv
from datatypes import DataType
from exporter import Marketplaces, Exporter
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

if hasattr(ssl, '_create_unverified_context'):
    ssl._create_default_https_context = ssl._create_unverified_context

def get_exporter(market: Marketplaces) -> Exporter:
    if market == Marketplaces.WB:
        return wildberries.Wildberries({'token': os.getenv('WB_TOKEN'), 'warehouseId': os.getenv('WB_WAREHOUSE_ID')}, log_info=log_info.info, log_error=log_error.error)
    elif market == Marketplaces.OZON:
        return ozon.Ozon({'key': os.getenv('OZON_KEY'), 'clientId': os.getenv('OZON_CLIENT_ID'), 'warehouseId': os.getenv('OZON_WAREHOUSE_ID')}, log_info=log_info.info, log_error=log_error.error)

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

        task_id = dict(task).get('id','')
        if not task_id:
            log_error.error("ID of task %s is not defined", dict(task).get('name',''))
            continue

        if 'URL' in task and 'type' in task:
            datatype = None
            delimiter = None
            type_from_config = str(task['type']).upper()

            if  type_from_config == 'CSV':
                datatype = DataType.CSV
                delimiter = dict(task).get('delimiter',';')
            elif type_from_config == 'XML':
                datatype = DataType.XML            

            if not datatype:
                log_error.error("Type of data is not defined in task %s", dict(task).get('name',''))
                continue

            
            importer = Importer(url=task['URL'], datatype=datatype, delimiter=delimiter, log_info=log_info.info, log_error=log_error.error)
            
            if importer.import_data():
                log_info.info('Import completed successfully')

                if 'export' in task:
                    for export in task['export']:
                        if not dict(export).get('active', True):
                            continue

                        reset_missing = dict(export).get('reset_missing', False)                        

                        try:
                            market = Marketplaces[export['marketplace']]
                            log_info.info(f"Starting task {market.value}")
                        except Exception as ex:
                            log_error.error("Unkmown code of marketplace %s", export['marketplace'])
                            continue                        

                        if 'schema' not in export:
                            log_error.error("Schema is not defined for %s", export['marketplace'])
                            continue
                        
                        code_column = dict(export['schema']).get('code', 0)
                        amount_column = dict(export['schema']).get('amount', 0)
                        prefix = dict(export['schema']).get('prefix','')
                        
                        if code_column <= 0:
                            log_error.error("Column for code is not defined in schema in %s", export['marketplace'])
                            continue
                        if amount_column <= 0:
                            log_error.error("Column for amount is not defined in schema in %s", export['marketplace'])
                            continue

                        export_data = importer.get_data({'code': code_column, 'amount': amount_column}, prefix=prefix) #{code: {amount: amount}}
                        market_code = market.name

                        with shelve.open(file_db) as db:
                            #db = shelve.open(file_db)
                            if reset_missing:
                                market_db = db.get(market_code, {})                        
                                for row in market_db:
                                    if row not in export_data:
                                        if len(row): #not empty code
                                            export_data[row] = {'amount': 0}
                                
                            db[market_code] = dict(export_data)                            
                            #db.close
                        
                        exporter = get_exporter(market)
                        
                        if not exporter.export(export_data):
                            log_info.info("Export finished with errors!")
                        else:
                            log_info.info('Export completed successfully')
                        
