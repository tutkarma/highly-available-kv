class Message:
    def __init__(self, from_node: 'DynamoNode', to_node: 'DynamoNode', msg_id=None):
        self.from_node = from_node
        self.to_node = to_node
        self.msg_id = msg_id

    def __str__(self):
        return self.__class__.__name__


class ResponseMessage(Message):
    def __init__(self, req):
        super(ResponseMessage, self).__init__(req.to_node, req.from_node, req.msg_id)
        self.response_to = req


class Timer(Message):
    def __init__(self, node, reason, callback=None):
        super(Timer, self).__init__(node, node)
        self.reason = reason
        self.callback = callback
