import pika
import uuid, json

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='test_queue')

channel.exchange_declare(exchange='aemen_direct',
                         exchange_type='direct',
                         durable=True)

channel.queue_bind(exchange='aemen_direct',
                       queue='test_queue',
                       routing_key='aemen_direct')

message_headers = {"number-of-retries": 0}
props = pika.BasicProperties(message_id=str(uuid.uuid4()), headers=message_headers)

input_params = {
    "cmd": "C:\\Users\\inviter\\Desktop\\Deepika\\DOT_NET\\readJson.py",
    "jsonPath": "C:\\Users\\inviter\\Desktop\\Deepika\\DOT_NET\\sampleJson.json"
}
input_params = json.dumps(input_params)
byte_input_params = str.encode(input_params)

for i in range(0, 300):
    channel.basic_publish(exchange='aemen_direct',
                          properties=props,
                          routing_key='aemen_direct',
                          body=byte_input_params)
    print("Sent Message: " + str(i))
connection.close()