import hashlib
import os
import logging
import threading
from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumControlLayer:
    def __init__(self, dict_data_lock, dict_data):
        self.dict_data_lock: threading.Lock = dict_data_lock
        self.dict_data: dict[str, tuple[dict[str, fruit_item.FruitItem], int]] = dict_data
        self.data_output_exchanges: list[middleware.MessageMiddlewareExchangeRabbitMQ] = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        
        self.control_output_exchanges: list[middleware.MessageMiddlewareExchangeRabbitMQ] = []
        for i in range(SUM_AMOUNT):
            control_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_CONTROL_EXCHANGE}_{i}"]
            )
            self.control_output_exchanges.append(control_output_exchange)

        self.control_input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_CONTROL_EXCHANGE}_{ID}"]
        )
        self.data_global_count: dict[str, tuple[int, int]] = {}
    
    def _ask_for_local_counts(self, client_uuid, data_total_count):
        logging.info(f"Asking for local counts from sums")
        self.data_global_count[client_uuid] = (0, 0)
        for control_output_exchange in self.control_output_exchanges:
            control_output_exchange.send(message_protocol.internal.serialize([client_uuid, data_total_count, ID]))
    
    def _send_local_count(self, client_uuid, data_total_count, sum_id):
        logging.info(f"Sending local count to sum {sum_id}")
        self.dict_data_lock.acquire()
        _, count_message = self.dict_data.get(client_uuid, ({}, 0))
        self.dict_data_lock.release()
        control_output_exchange = self.control_output_exchanges[sum_id]
        control_output_exchange.send(message_protocol.internal.serialize([client_uuid, data_total_count, count_message, ID]))

    def _procces_global_count(self, client_uuid, data_total_count, count_message, sum_id):
        logging.info(f"Received local count from sum {sum_id}, processing global count")
        (data_count, sums_recieved) = self.data_global_count[client_uuid]
        self.data_global_count[client_uuid] = (data_count + count_message, sums_recieved + 1)
        if self.data_global_count[client_uuid][1] == SUM_AMOUNT and self.data_global_count[client_uuid][0] == data_total_count:
             logging.info(f"Received all count control messages, broadcasting EOF to sums")
             for eof_output_exchange in self.control_output_exchanges:
                eof_output_exchange.send(message_protocol.internal.serialize([client_uuid]))
        elif self.data_global_count[client_uuid][1] == SUM_AMOUNT:
            logging.info(f"Received all count control messages, but count does not match")
    
    def _send_data_messages(self, client_uuid):
        logging.info(f"Routing data messages")
        self.dict_data_lock.acquire()
        dict_by_client, _ = self.dict_data[client_uuid]
        self.dict_data_lock.release()
        for final_fruit_item in dict_by_client.values():
            data_output_exchange = self._select_data_output_exchange(
                client_uuid, final_fruit_item.fruit
            )
            data_output_exchange.send(
                message_protocol.internal.serialize(
                    [client_uuid, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )
        
        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_uuid]))
        
        self.dict_data_lock.acquire()
        del self.dict_data[client_uuid]
        self.dict_data_lock.release()

    def _select_data_output_exchange(self, client_uuid, fruit):
        hash_input = f"{client_uuid}:{fruit}".encode("utf-8")
        hash_value = int(hashlib.sha256(hash_input).hexdigest(), 16)
        selected_index = hash_value % AGGREGATION_AMOUNT
        return self.data_output_exchanges[selected_index]

    def _process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 1:
            self._send_data_messages(*fields)
        if len(fields) == 2:
            self._ask_for_local_counts(*fields) 
        elif len(fields) == 3:
            self._send_local_count(*fields)
        elif len(fields) == 4:
            self._procces_global_count(*fields)
        ack()

    def start(self):
        self.control_input_exchange.start_consuming(self._process_control_message)
