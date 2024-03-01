from producer_interface import mqProducerInterface
import pika
import os

class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str):
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.setupRMQConnection()

    def setupRMQConnection(self):
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)

        # Establish Channel
        self.channel = self.connection.channel()

        # Create the exchange if not already present
        self.exchange = self.channel.exchange_declare(exchange=f"{self.exchange_name}")

    def publishOrder(self, message: str):
        # Basic Publish to Exchange
        self.channel.basic_publish(
            exchange=f"{self.exchange_name}",
            routing_key=f"{self.routing_key}",
            body=f"{message}"
        )
        print(f"{message}")

        # Close Channel
        self.channel.close()

        # Close Connection
        self.connection.close()
        
