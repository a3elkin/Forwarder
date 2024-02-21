import ssl
from datatypes import DataType
from importer import Importer

if hasattr(ssl, '_create_unverified_context'):
    ssl._create_default_https_context = ssl._create_unverified_context
        
if __name__ == '__main__':
    forwarder = Importer("https://opt.lovemarket.net/upload/opt.lovemarket.csv")
    forwarder.get_data(DataType.CSV, delimiter=';')