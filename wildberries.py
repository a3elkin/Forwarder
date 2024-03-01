from exporter import Exporter, Marketplaces
from typing import Callable
from query_response import QueryResponse
import requests
import json

class Wildberries(Exporter):

    def __init__(self, authorization: dict, data: dict, log_info: Callable[[str], None] = None, log_error: Callable[[str], None] = None) -> None:
        super().__init__()
        self.marketplace = Marketplaces.WB
        self.authorization = authorization
        self.data = data
        self.log_info = log_info
        self.log_error = log_error


    def export(self) -> bool:        
        def send_data(packet: list, num: int) -> bool:

            def execute_request(request_packet: list) -> QueryResponse:
                try:
                    r = requests.put(url, data = json.dumps({'stocks': request_packet}), headers = headers)
                except Exception as ex:
                    return QueryResponse(success=False, status=r.status_code, data=r.text)
                
                if r.status_code != 204: #204!
                    return QueryResponse(success=False, status=r.status_code, data=r.text)
                
                return QueryResponse(success=True, status=r.status_code)


            url = f'https://suppliers-api.wildberries.ru/api/v3/stocks/{self.authorization['warehouseId']}'
            headers = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'Authorization': self.authorization['token']}

            response = execute_request(packet)
            if not response.success:
                if self.log_error:
                    self.log_error(f"Body {str({'stocks': packet})}")
                    self.log_error(f"Request error: status {response.status}, data {str(response.data)}")

            if response.status == 409: #there are codes without WB product
                error_data = []
                error_json = json.loads(response.data)
                if isinstance(error_json, list):
                    error_json = error_json[0]
                if 'data' in error_json:
                    error_data = [record['sku'] for record in error_json['data']]                    

                new_packet = [record for record in packet if record['sku'] not in error_data]
                if self.log_error:
                    self.log_error(f"Try with cleared data")
                packet = new_packet
                if len(packet):
                    response = execute_request(packet)
                    if not response.success:
                        if self.log_error:
                            self.log_error(f"Body {str({'stocks': packet})}")
                            self.log_error(f"Request error: status {response.status}, data {str(response.data)}")
                        return False
                    else:
                        if self.log_error:
                            self.log_error(f"Successful!")  
                else:
                    if self.log_error:
                        self.log_error(f"There are no products to send in this packet!")                    
            else:
                return False
                
            if self.log_info:
                self.log_info(f"Packet #{num}: Sending {len(packet)} records completed successfully")

            return True

        counter = 0
        packet_number = 1
        api_packet = []
        result = True
        for record in self.data:
            api_packet.append({"sku": record, "amount": int(self.data[record]['amount'])})                
            counter += 1
            if counter > 990:
                if not send_data(api_packet, packet_number):
                    result = False
                api_packet = []
                counter = 0
                packet_number += 1
        if counter:
            if not send_data(api_packet, packet_number):
                result = False
        
        return result