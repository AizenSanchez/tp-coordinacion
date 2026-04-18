import uuid

from common import message_protocol


class MessageHandler:

    def __init__(self):
        self.uuid = str(uuid.uuid4())
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.uuid, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.uuid])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 2:
            [client_uuid, fruit_top] = fields
            if client_uuid == self.uuid:
                return fruit_top
        return None
