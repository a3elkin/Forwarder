from exporter import Exporter, Marketplaces
from typing import Callable
from query_response import QueryResponse
import requests
import json

class Ozon(Exporter):

    def __init__(self, authorization: dict, data: dict, log_info: Callable[[str], None] = None, log_error: Callable[[str], None] = None) -> None:
        super().__init__()
        self.marketplace = Marketplaces.OZON
        self.authorization = authorization
        self.data = data
        self.log_info = log_info
        self.log_error = log_error


    def export(self) -> bool:        
        def send_data(packet: list, num: int) -> bool:

            def execute_request(request_packet: list) -> QueryResponse:
                try:
                    r = requests.post(url, data = json.dumps({'stocks': request_packet}), headers = headers)
                except Exception as ex:
                    return QueryResponse(success=False, status=r.status_code, data=r.text)
                               
                return QueryResponse((r.status_code == 200), status=r.status_code, data=r.text)


            url = 'https://api-seller.ozon.ru/v2/products/stocks'
            headers = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'Api-Key': self.authorization['key'], 'Client-Id': self.authorization['clientId']}

            response = execute_request(packet)
            if not response.success:
                if self.log_error:
                    self.log_error(f"Body {str({'stocks': packet})}")
                    self.log_error(f"Request error: status {response.status}, data {str(response.data)}")
                return False

            error_codes = []
            try:
                json_response = json.loads(response.data)
                error_codes = [record['offer_id'] for record in json_response['result'] if not record['updated']]
            except Exception:
                if self.log_error:
                    self.log_error(f"Error while getting response data of successful executed query")
            
            if error_codes:
                if self.log_info:
                    self.log_info(f"Stocks were not updated {str(error_codes)}")

            if self.log_info:
                self.log_info(f"Packet #{num}: Sending {len(packet) - len(error_codes)} records completed successfully")

            return True

        counter = 0
        packet_number = 1
        api_packet = []
        result = True
        for record in self.data:
            api_packet.append({"offer_id": record, "stock": int(self.data[record]['amount']), "warehouse_id": self.authorization['warehouseId']})                
            counter += 1
            if counter > 99:
                if not send_data(api_packet, packet_number):
                    result = False
                api_packet = []
                counter = 0
                packet_number += 1
        if counter:
            if not send_data(api_packet, packet_number):
                result = False
        
        return result