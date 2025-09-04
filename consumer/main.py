import pika

class RabbitMQConsumer:
    def __init__(
        self,
        callback,
        host="localhost",
        port=5672,
        username="guest",
        password="guest",
        queue="data_queue",
    ):

        params = pika.ConnectionParameters(
            host=host,
            port=port,
            credentials=pika.PlainCredentials(username, password)
        )
        self.channel = pika.BlockingConnection(params).channel()
        self.channel.queue_declare(queue=queue, durable=True)
        self.channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=True)
        self.port = port


    def start(self):
        print(f"Listening RabbitMQ on port: {self.port}")
        self.channel.start_consuming()


def callback(ch, method, properties,body):
    print(body)


if __name__ == "__main__":
    RabbitMQConsumer(callback).start()
