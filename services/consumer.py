from kafka import KafkaConsumer
from json import loads
consumer = KafkaConsumer(
    'name_of_topic',
     bootstrap_servers=['http://localhost:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     group_id='test-consumer-group',
     value_deserializer=lambda x: loads(x.decode('ISO-8859-1')))

for message in consumer:
     print(message.value)
