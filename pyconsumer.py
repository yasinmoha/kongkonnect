from confluent_kafka import Consumer
import json
from opensearchpy import OpenSearch


####OperSearch####

host = 'localhost'
port = 9200

client = OpenSearch(
    hosts = [{'host': host, 'port': port}],
)


def gendata():
    for row in data.splitlines():
        yield { "index": {
            "_index":"cdc-events14",
            "_source":row
        }
        }

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
    value = msg.value().decode('UTF-8')
    data = json.loads(value)
    client.bulk(gendata())
c.close()


