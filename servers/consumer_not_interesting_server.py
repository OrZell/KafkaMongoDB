from kafka_models.kafka_consumer import Consumer
from fastapi import FastAPI
from dal.MongoDBDal import MongoDBDal
import uvicorn

app = FastAPI()
consumer = Consumer()
dal = MongoDBDal()

@app.get('/')
def get_data():
    events = consumer.consumer_with_auto_commit('not_interesting')
    events = consumer.add_timestamp(events)
    dal.insert_documents(documents=events, topic='not_interesting')

if __name__ == '__main__':
    uvicorn.run(app, host='localhost', port=8002)