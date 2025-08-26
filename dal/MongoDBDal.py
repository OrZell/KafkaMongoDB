from kafka_models.configurations import MONGO_CONNECT_STRING, MONGO_DB
from pymongo import MongoClient

class MongoDBDal:

    def __init__(self):
        self.connecting_string = MONGO_CONNECT_STRING
        self.connection = None
        self.db = MONGO_DB

    def open_connection(self):
        if self.connection is None:
            self.connection = MongoClient(self.connecting_string)

    def close_connection(self):
        if self.connection:
            self.connection.close()

    def insert_documents(self, topic, documents):
        self.open_connection()
        self.connection[self.db][topic].insert_many(documents)
        self.close_connection()