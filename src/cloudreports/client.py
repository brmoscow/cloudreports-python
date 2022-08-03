"""Client for interacting with the database."""

import json

class Client(object):
    """Define Client """

    def __init__(self, database, buffer_size=3000):
        if not isinstance(database, object):
            raise ValueError("Pass a object for database")        
        self._database = database        
        self._buffer_size = buffer_size
        self.data = []


    def load_json_data(self, entity_id, entity_type, entity_data, event_moment, event_type=None):
        """Send data to Database"""

        item_dic = {
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




