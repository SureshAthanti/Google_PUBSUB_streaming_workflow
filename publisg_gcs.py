import logging
from base64 import b64decode
from json import loads
import pandas as pd
from google.cloud import storage

class GCSStorage():
    def __init__(self, event, context):
        self.event = event
        self.context = context
        self.bucket_name = "streaming-bucket-pubsub"
    
    def get_data_event(self):
        logging.info(
            f"This Function was triggered by messageId {} published at {} to {}"
            .format(context.event_id, context.timestamp, context.resource["name"])

        if "data" in self.event:
            pubsub_data = b64decode(self.event["data"]).decode("utf-8")
            logging.info(pubsub_data)
            return pubsub_data

        else:
            logging.error("Incorrect format")
            return ""

    def transform_dataframe(self,message):
        message = loads(message)
        df = pd.DataFrame([message])
        return df
    
    def upload_to_bucket(self, data, file_name):
        from google.cloud import storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(f"{file_name}.csv")
        blob.upload_from_string(data=df.to_csv(index=False), content_type="text/csv")
        logging.info(f"file uploaded to bucket")

def hello_pubsub(event, context):
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    service_gcs = GCSStorage(event, context)
    message = service_gcs.get_data_event()
    upload_data = service_gcs.transform_dataframe()
    service_gcs.upload_to_bucket(upload_data, "weather_data"+str(datetime.datetime.now()))
