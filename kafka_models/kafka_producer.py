from kafka import KafkaProducer
from kafka_models.configurations import HOST, PORT
import json

class Producer:

    def __init__(self):
        self.Host = HOST
        self.Port = PORT
        self.URI = self.Host + ':' + self.Port

    def get_producer_config(self):
        producer =  KafkaProducer(bootstrap_servers=[self.URI],
                                 value_serializer=lambda x:
                                 json.dumps(x).encode('utf-8'))
        return producer

    def publish_message(self, topic, message):
        producer = self.get_producer_config()
        producer.send(topic, message)

    def publish_list_of_messages(self, messages, topic):
        for message in messages:
            self.publish_message(message=message, topic=topic)

    def publish_message_with_key(self, topic, key, message):
        producer = self.get_producer_config()
        producer.send(topic, key=key, value=message)







