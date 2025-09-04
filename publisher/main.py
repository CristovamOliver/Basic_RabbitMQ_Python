import pika, json


class RabbitMQPublisher:
    def __init__(
        self,
        host="localhost",
        port=5672,
        username="guest",
        password="guest",
        exchange="data_exchange"
    ):
        params = pika.ConnectionParameters(
            host=host,
            port=port,
            credentials=pika.PlainCredentials(username, password)
        )
        self.channel = pika.BlockingConnection(params).channel()
        self.exchange = exchange

    def send(self, message: dict):
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key="",
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)
        )


if __name__ == "__main__":
    RabbitMQPublisher().send({"Ola": "mundo"})
