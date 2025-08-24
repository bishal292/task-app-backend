import pika
import json

def callback(ch, method, properties, body):
    message = json.loads(body)
    print(f" [x] Received message: {message}")

    if message["action"] == "create":
        print("Processing new task creation...")
    elif message["action"] == "update":
        print("Processing task update...")
    elif message["action"] == "delete":
        print("Processing task deletion...")
    elif message["action"] == "read":
        print("Reading Task...")

def start_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue="task_queue")

    channel.basic_consume(queue="task_queue", on_message_callback=callback, auto_ack=True)
    print(" [*] Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    start_consumer()
