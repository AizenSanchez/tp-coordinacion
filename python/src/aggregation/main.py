import os
import logging
import bisect

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.client_fruit_top = {}
        self.client_sums_eof_received = {}

    def _process_data(self, client_uuid, fruit, amount):
        logging.info("Processing data message")
        if client_uuid not in self.client_fruit_top:
            self.client_fruit_top[client_uuid] = []
        fruit_top = self.client_fruit_top[client_uuid]
        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                updated_fruit = fruit_top.pop(i) + fruit_item.FruitItem(
                    fruit, amount
                )
                bisect.insort(fruit_top, updated_fruit)
                self.client_fruit_top[client_uuid] = fruit_top
                return
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))
        self.client_fruit_top[client_uuid] = fruit_top
        

    def _process_eof(self, client_uuid):
        logging.info("Received EOF")
        if client_uuid not in self.client_sums_eof_received:
            self.client_sums_eof_received[client_uuid] = 1
        else:
            self.client_sums_eof_received[client_uuid] = self.client_sums_eof_received[client_uuid] + 1
        if self.client_sums_eof_received[client_uuid] != SUM_AMOUNT:
            return
        logging.info("Received all EOFs, processing top")
        fruit_chunk = list(self.client_fruit_top[client_uuid][-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        self.output_queue.send(message_protocol.internal.serialize([client_uuid, fruit_top]))
        del self.client_fruit_top[client_uuid]

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
