import os
import logging
import threading
from common import middleware, message_protocol, fruit_item
import signal

from sum_control_layer import SumControlLayer

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
        
        signal.signal(signal.SIGTERM, self._graceful_exit)
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        self.dict_data: dict[str, tuple[dict[str, fruit_item.FruitItem], int]] = {}
        self.dict_data_lock = threading.Lock()
        self.thread_control = threading.Thread(target=self._control_thread)
        self.thread_control.start()
        self.sum_control = None

        self.control_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_CONTROL_EXCHANGE}_{ID}"]
        )
    
    def _process_data(self, client_uuid,fruit, amount):
        logging.info(f"Process data")
        self.dict_data_lock.acquire()
        if client_uuid not in self.dict_data:
            self.dict_data[client_uuid] = ({}, 0)
        (dict_by_client, count_message) = self.dict_data[client_uuid]       
        dict_by_client[fruit] = dict_by_client.get(fruit, fruit_item.FruitItem(fruit, 0)) + fruit_item.FruitItem(fruit, int(amount))
        self.dict_data[client_uuid] = (dict_by_client, count_message+ 1)
        self.dict_data_lock.release()

    def _process_eof(self, client_uuid, total_count):
        logging.info(f"Process EOF, sending control message to control layer")
        self.control_output_exchange.send(message_protocol.internal.serialize([client_uuid, total_count]))

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        elif len(fields) == 2:
            self._process_eof(*fields)
        else:
            logging.info(f"Received invalid message, ignoring")
            
        ack()
    
    def _control_thread(self):
        self.sum_control = SumControlLayer(self.dict_data_lock, self.dict_data)
        self.sum_control.start()
        self.sum_control.close()



    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage)

    def _graceful_exit(self, signum, frame):
        logging.info("Received termination signal, stopping consuming")
        if self.sum_control is not None:
            self.sum_control.stop()
        self.input_queue.stop_consuming()
        

    def close(self):
        logging.info("Closing sum filter")
        self.input_queue.close()
        self.control_output_exchange.close()
        self.thread_control.join()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    sum_filter.close()
    return 0


if __name__ == "__main__":
    main()
