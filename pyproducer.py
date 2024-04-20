import json
from uuid import uuid4
#from kafka import KafkaProducer
from confluent_kafka import Producer
import time

print("Starting Kafka Producer") 
conf = {
    'bootstrap.servers' : 'localhost:9092'
    }
print("connecting to Kafka topic...")


kafka_topic_name = "cdc"
 
def delivery_report(errmsg, msg):
    if errmsg is not None:
        print("Delivery failed for Message: {} : {}".format(msg.key(), errmsg))
        return
    print('Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
    

    
with open('stream.jsonl' , 'r') as f:
    for line in f:
        json_obj = json.loads(line.strip())
        producer1 = Producer(conf)
        producer1.poll(0)

        try:
            producer1.produce(kafka_topic_name, json.dumps(json_obj).encode('UTF-8'), on_delivery=delivery_report)
            producer1.flush()
    
        except Exception as ex:
            print("Exception happened :",ex)

            print("\n Stopping Kafka Producer")   
     