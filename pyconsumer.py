from confluent_kafka import Consumer
import json
from opensearchpy import OpenSearch
from opensearch_dsl import Search

####OperSearch####

host = 'localhost'
port = 9200

client = OpenSearch(
    hosts = [{'host': host, 'port': port}],
)

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['cdc'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        break # Or we have continue for listening always
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    print('Received message: {}'.format(msg.value().decode('utf-8')))
    value = msg.value().decode('utf-8')
    data = json.loads(value)
    response = client.index(
    index = 'cdc-events9',
    body = data,
    refresh = True
    )


c.close()
'''
json_data = '{"index" : {"_index":"cdc-events7","_id":"2"}}\n' + data
client.bulk(json_data)
'''

