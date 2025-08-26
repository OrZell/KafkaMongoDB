from kafka_models.kafka_producer import Producer
from fastapi import FastAPI
# from data.sk import fetch_the_data
from data.Reader import Reader
import uvicorn

app = FastAPI()
prod = Producer()
reader = Reader()

@app.get('/data')
def send_data():
    interesting_data = reader.read_interesting_data()
    not_interesting_data = reader.read_interesting_data()
    prod.publish_list_of_messages(interesting_data, 'interesting')
    prod.publish_list_of_messages(not_interesting_data, 'not_interesting')
    reader.increase_the_counter()


if __name__ == '__main__':
    uvicorn.run(app, host='localhost', port=8000)