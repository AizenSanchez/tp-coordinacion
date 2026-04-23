import os
import logging
import heapq
import sys
from common import middleware, message_protocol, fruit_item
import signal 

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        signal.signal(signal.SIGTERM, self._graceful_exit)
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.top_by_client = {}        

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        [client_uuid, fruit_top] = message_protocol.internal.deserialize(message)
        if client_uuid not in self.top_by_client:
            self.top_by_client[client_uuid] = (fruit_top, 1)
        else:
            current_top, count = self.top_by_client[client_uuid]
            merged_top = self._merge_tops(current_top, fruit_top)
            self.top_by_client[client_uuid] = (merged_top, count + 1)
        
        if self.top_by_client[client_uuid][1] == AGGREGATION_AMOUNT:
            fruit_top = self.top_by_client[client_uuid][0]
            self.output_queue.send(message_protocol.internal.serialize([client_uuid, fruit_top]))
            del self.top_by_client[client_uuid]
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)

    def _merge_tops(self, top1, top2):
        result = []
        result = self._add_to_heap(result, top1)
        result = self._add_to_heap(result, top2)
        result = list(map(lambda fruit_item: (fruit_item.fruit, fruit_item.amount), [heapq.heappop(result) for _ in range(min(TOP_SIZE, len(result)))]))
        result.reverse()
        return result
    
    def _add_to_heap(self, heap, list_of_fruits):
        for fruit, amount in list_of_fruits:
            if len(heap) < TOP_SIZE:
                heapq.heappush(heap, fruit_item.FruitItem(fruit, amount))
            else:
                heapq.heappushpop(heap, fruit_item.FruitItem(fruit, amount))
        return heap
    
    def _graceful_exit(self, signum, frame):
        logging.info("Received termination signal, stopping consuming")
        self.input_queue.stop_consuming()

    def close(self):
        logging.info("Closing join filter")
        self.input_queue.close()
        self.output_queue.close()
        logging.info("Join filter closed")

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()
    join_filter.close()
    return 0


if __name__ == "__main__":
    main()
