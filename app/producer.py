import pika
import json

def publish_message(queue_name: str, message: dict):
    # Connecting to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare queue (creates if not exists)
    channel.queue_declare(queue=queue_name)

    # Publish message
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=json.dumps(message)
    )

    print(f" [x] Sent message to Queue {queue_name} with \nmessage : {message}")
    connection.close()
