from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(bootstrap_servers='localhost:9093',
                                 auto_offset_reset='earliest',
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))

consumer.subscribe(['tweets'])

for msg in consumer:
     print (msg)