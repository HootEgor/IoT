import json
import logging
from typing import List

import pydantic_core
import requests

from app.entities.processed_agent_data import ProcessedAgentData
from app.interfaces.store_gateway import StoreGateway


class StoreApiAdapter(StoreGateway):
    def __init__(self, api_base_url):
        self.api_base_url = api_base_url

    def save_data(self, processed_agent_data_batch: List[ProcessedAgentData]):
        url = f"{self.api_base_url}/processed_agent_data"
        data = [data.dict() for data in processed_agent_data_batch]

        # Серіалізувати об'єкти datetime до рядків
        for item in data:
            item['agent_data']['timestamp'] = item['agent_data']['timestamp'].isoformat()

        response = requests.post(url, json=data)

        if response.status_code != 200:
            # Handle error if needed
            print(f"Failed to save data: {response.status_code}, {response.text}")
        else:
            print("Data saved successfully.")
