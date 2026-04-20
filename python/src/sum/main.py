import os
import logging
import threading
import pika
from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=MOM_HOST))
        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE, channel
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_fruit = {}
        self.control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE], channel, connection
        )

    def _process_data(self, client_uuid,fruit, amount):
        logging.info(f"Process data")
        self.amount_by_fruit[(client_uuid,fruit)] = self.amount_by_fruit.get(
            (client_uuid, fruit), fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_uuid):
        logging.info(f"Broadcasting data messages")
        for (client_uuid, _),final_fruit_item in self.amount_by_fruit.items():
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize(
                        [client_uuid,final_fruit_item.fruit, final_fruit_item.amount]
                    )
                )

        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_uuid]))


    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof_client(*fields)
        ack()
    
    def process_control_message(self, message, ack, nack):
        logging.info(f"Process control message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 1:
            self._process_eof(*fields)
        ack()

    def _process_eof_client(self, client_uuid):
        logging.info(f"Broadcasting EOF message to sums")
        self.control_exchange.send(message_protocol.internal.serialize([client_uuid]))

    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage, self.process_control_message, self.control_exchange.get_queue_name())

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
