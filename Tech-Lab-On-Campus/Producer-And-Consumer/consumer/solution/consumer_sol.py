import os

import pika
from consumer_interface import mqConsumerInterface


class mqConsumer(mqConsumerInterface):
    def __init__(
        self, binding_key: str, exchange_name: str, queue_name: str
    ) -> None:
        # Save parameters to class variables
        self.m_binding_key = binding_key
        self.m_queue_name = queue_name
        self.m_exchange_name = exchange_name
        # Call setupRMQConnection
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.m_connection = pika.BlockingConnection(parameters=con_params)

        # Establish Channel
        self.m_channel = self.m_connection.channel()

        # Create Queue if not already present
        self.m_channel.queue_declare(queue=self.m_queue_name)

        # Create the exchange if not already present
        self.m_channel.exchange_declare(self.m_exchange_name)

        # Bind Binding Key to Queue on the exchange
        self.m_channel.queue_bind(
            queue=self.m_queue_name,
            routing_key=self.m_binding_key,
            exchange=self.m_exchange_name,
        )

        # Set-up Callback function for receiving messages
        self.m_channel.basic_consume(
            self.m_queue_name, self.on_message_callback, auto_ack=False
        )

    def on_message_callback(
        self, channel, method_frame, header_frame, body
    ) -> None:
        # Acknowledge Message 
        channel.basic_ack(method_frame.delivery_tag, False)

        # Print Message
        print(f" [x] Received Message: {body}")

    def startConsuming(self) -> None:
        
        # Print " [*] Waiting for messages. To exit press CTRL+C"
        print(" [*] Waiting for messages. To exit press CTRL+C")

        # Start consuming messages
        self.m_channel.start_consuming()
    
    def __del__(self) -> None:
        print(f"Closing RMQ connection on destruction")
        self.m_channel.close()
        self.m_connection.close()
