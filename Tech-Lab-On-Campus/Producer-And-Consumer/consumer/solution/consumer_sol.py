from consumer_interface import mqConsumerInterface
import pika
import os
class mqConsumer (mqConsumerInterface):
    def __init__(self, binding_key, exchange_name, queue_name):
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.channel = None
        self.connection = None
        self.setupRMQConnection()
        pass

    def setupRMQConnection(self):
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters = con_params)

        # Establish Channel
        self.channel = self.connection.channel()

        # Create Queue if not already present
        #DO I NEED IF STATEMENT
        if (self.queue_name != None):
            self.channel.queue_declare(queue = self.queue_name)
    
        # Create the exchange if not already present
        if (self.exchange_name != None):
            self.exchange = self.channel.exchange_declare(exchange = self.exchange_name)

        # Bind Binding Key to Queue on the exchange
        self.channel.queue_bind(
            queue = self.queue_name,
            routing_key = self.binding_key,
            exchange = self.exchange_name,
        )

        # Set-up Callback function for receiving messages
        self.channel.basic_consume(
            self.queue_name, self.on_message_callback, auto_ack = False
        )    

    def on_message_callback(self, channel, method_frame, header_frame, body):
        channel.basic_ack(method_frame.delivery_tag, False) #acknowldge message
        print(f"{body}")
        
    def startConsuming(self):
        print("[*] Waiting for messages. To exit press CTRL+C")
        pass

    
    def __del__(self):
        print("Closing RMQ connection on destruction")
        # Close channel
        self.channel.close()
        # Close connection
        self.connection.close()
        pass
