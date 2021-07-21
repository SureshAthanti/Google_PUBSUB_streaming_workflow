import os
import logging
import requests
import json
from google.cloud import pubsub_v1
from google.cloud import storage


class GooglePublishTopic():
    def __init__(self):
        self.project_id = "phonic-command-320418"
        self.topic_id = "streaming-topic"
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'Users/Suresh Athanti/Desktop/Algorithms Practice/cloud_pub-sub/phonic-command-320418-4e8371231974.json'
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)
        self.publish_futures = []

    def get_weather_data():
        API_key = "APT_KEY"
        address = "https://api.openweathermap.org/data/2.5/forecast?id=524901&appid=" + API_key
        resp = requests.get(address)
        result = resp.json()
        weather_data = []
        for i in range(len(result['list'])):
            weather_data.append(result['list'][i]['main'])
        data = str(weather_data)
        return data

    def get_callback(publish_future, data):
        def callback(publish_future):
            from concurrent import futures
            try:
                print(publish_future.result(timeout=60))
            except futures.TimeoutError:
                print(f"Publishing {data} timed out.")
            return callback   
    
    def publish_to_pubsub_topic(self, weather_data):
        from concurrent import futures
        # weather_data = get_weather_data()
        publish_future = self.publish(self.topic_path, weather_data.encode("utf-8"))
        publish_future.add_done_callback(self.get_callback(publish_future, weather_data))
        self.publish_futures.append(publish_future)
        # Wait for all the publish futures to resolve before exiting.
        futures.wait(self.publish_futures, return_when=futures.ALL_COMPLETED)


pub_sub_message = GooglePublishTopic()
weather_data = pub_sub_message.get_weather_data()
pub_sub_message.publish_to_pubsub_topic(weather_data)
