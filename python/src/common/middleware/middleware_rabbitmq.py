import pika
import random
import string
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareDeleteError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name, channel = None):
        if channel is None:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.channel = connection.channel()
        else:
            self.channel = channel
        self.channel.queue_declare(queue=queue_name)
        self.queue_name = queue_name
    
    def start_consuming(self, on_message_callback, on_message_callback_exchange = None, queue_name_exchange = None):
        try:
            declare_callback_fn = _declare_callback(on_message_callback)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=declare_callback_fn, auto_ack=False)
            
            if on_message_callback_exchange is not None and queue_name_exchange is not None:
                declare_callback_fn_exchange = _declare_callback(on_message_callback_exchange)
                self.channel.basic_consume(queue=queue_name_exchange, on_message_callback=declare_callback_fn_exchange, auto_ack=False)
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
        except:
            raise MessageMiddlewareCloseError()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys, channel = None, connection = None):
        if channel is None:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.channel = self.connection.channel()
        else:
            self.connection = connection
            self.channel = channel
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
        self.routing_keys = routing_keys
        self.exchange_name = exchange_name

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.queue_name = result.method.queue
        for routing_key in self.routing_keys:
            self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=routing_key)
            

    
    def start_consuming(self, on_message_callback):
        try:
            declare_callback_fn = _declare_callback(on_message_callback)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=declare_callback_fn, auto_ack=False)
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
        
    def get_queue_name(self):
        return self.queue_name
        
def _declare_callback(on_message_callback):
    def callback(ch, method, properties, body):
        def ack():
            ch.basic_ack(delivery_tag=method.delivery_tag)

        def nack():
            ch.basic_nack(delivery_tag=method.delivery_tag)
        
        on_message_callback(body, ack, nack)
    return callback