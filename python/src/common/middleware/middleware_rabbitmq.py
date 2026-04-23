import pika
import random
import string
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareDeleteError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name)
        self.queue_name = queue_name
    
    def start_consuming(self, on_message_callback):
        try:
            def callback(ch, method, properties, body):
                def ack():
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                def nack():
                    ch.basic_nack(delivery_tag=method.delivery_tag)
                
                on_message_callback(body, ack, nack)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=False)
            self.channel.start_consuming()
        except pika.exceptions.ConnectionClosed:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except pika.exceptions.ConnectionClosed:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def send(self, message):
        try:
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        except pika.exceptions.ConnectionClosed:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def close(self):
        try:
            self.channel.close()
            self.connection.close()
        except:
            raise MessageMiddlewareCloseError()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
        self.routing_keys = routing_keys
        self.exchange_name = exchange_name

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.queue_name = result.method.queue
        for routing_key in self.routing_keys:
            self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=routing_key)
            

    
    def start_consuming(self, on_message_callback):
        try:
            def callback(ch, method, properties, body):
                def ack():
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                def nack():
                    ch.basic_nack(delivery_tag=method.delivery_tag)
                
                on_message_callback(body, ack, nack)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=False)
            self.channel.start_consuming()
        except pika.exceptions.ConnectionClosed:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    
    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except pika.exceptions.ConnectionClosed:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def send(self, message):
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)
        except pika.exceptions.ConnectionClosed:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def close(self):
        try:
            self.channel.close()
            self.connection.close()
        except:
            raise MessageMiddlewareCloseError()