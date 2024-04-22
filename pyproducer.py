import json
from uuid import uuid4
#from kafka import KafkaProducer
from confluent_kafka import Producer

#Read the Json file 
with open('stream.jsonl' , 'r') as f:
    json_data = f.read()
    print(json_data)
    data = json.dumps(json_data)

def delivery_report(errmsg, msg):
    if errmsg is not None:
        print("Delivery failed for Message: {} : {}".format(msg.key(), errmsg))
        return
    print('Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

kafka_topic_name = "cdc"
print("Starting Kafka Producer") 
conf = {
    'bootstrap.servers' : 'localhost:9092'
    }
print("connecting to Kafka topic...")
producer1 = Producer(conf)
producer1.poll(0)

try:
    producer1.produce(topic=kafka_topic_name, key=str(uuid4()), value=data, on_delivery=delivery_report)
    producer1.flush()

except Exception as ex:
    print("Exception happened :",ex)

    print("\n Stopping Kafka Producer")   

