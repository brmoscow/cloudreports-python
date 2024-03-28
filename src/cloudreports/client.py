"""Client for interacting with the database."""

import json
import requests
from datetime import datetime

class Client(object):
    """Define Client """

    def __init__(self, database, buffer_size=3000, cr_api_url=None, cr_api_token=None, cr_integration_id=None):
        if not isinstance(database, object):
            raise ValueError("Pass a object for database")        
        self._database = database        
        self._buffer_size = buffer_size
        self.data = []
        self.cr_api_url = cr_api_url
        self.cr_api_token = cr_api_token
        self.cr_integration_id = cr_integration_id


    def load_json_data(self, entity_href, entity_id, entity_type, entity_data, event_moment, event_type=None):
        """Send data to Database"""

        item_dic = {
            'entity_href': str(entity_href),
            'entity_id': str(entity_id),
            'entity_type': str(entity_type),
            'entity_data': json.dumps(entity_data),
            'event_moment': str(event_moment),
            'event_type': str(event_type)
        }
        self.data.append(item_dic)

        if len(self.data) > self._buffer_size:
            self.finish_load_json_data()


    def finish_load_json_data(self):
        """Must be called before completion"""
        
        self._database.load_json_data(self.data)
        self.data = []


    def get_integration(self):
        """Get intrgration data from CloudReports platform"""

        if self.cr_api_url is None:
            return {'result': 'error', 'info': 'cr_api_url is None'}
        if self.cr_api_token is None:
            return {'result': 'error', 'info': 'cr_api_token is None'}
        if self.cr_integration_id is None:
            return {'result': 'error', 'info': 'cr_integration_id is None'}        

        headers = {
                'Authorization': f'cr_api_token {self.cr_api_token}'
            }
        r = requests.get(f'{self.cr_api_url}/{self.cr_integration_id}', headers=headers)
        return json.loads(r.content)


    def set_integration(self, load_date=None, load_rows=None, load_percent=None, tables_qty=None):
        """Set intrgration data to CloudReports platform"""

        if self.cr_api_url is None:
            return {'result': 'error', 'info': 'cr_api_url is None'}
        if self.cr_api_token is None:
            return {'result': 'error', 'info': 'cr_api_token is None'}
        if self.cr_integration_id is None:
            return {'result': 'error', 'info': 'cr_integration_id is None'} 

        data = {}
        if load_date is not None and isinstance(load_date, datetime):
            data['load_date'] = str(load_date)
        if load_rows is not None and type(load_rows) is int:
            data['load_rows'] = load_rows
        if load_percent is not None and type(load_percent) is int:
            data['load_percent'] = load_percent 
        if tables_qty is not None and type(tables_qty) is int:
            data['tables_qty'] = tables_qty      

        headers = {
                'Authorization': f'cr_api_token {self.cr_api_token}'
            }
        r = requests.put(f'{self.cr_api_url}/{self.cr_integration_id}', headers=headers, 
            json=data)
        return r


