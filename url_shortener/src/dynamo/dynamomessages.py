import typing as t

from message import Message, ResponseMessage

_show_metadata = False


def _show_value(value: str, metadata) -> str:
    if _show_metadata:
        try:
            return '{0}@[{1}]'.format(value, ','.join([str(x) for x in metadata]))
        except TypeError:
            return '{0}@{1}'.format(value, metadata)
    else:
        return value


class DynamoRequestMessage(Message):
    def __init__(self, from_node, to_node, key, msg_id=None):
        super(DynamoRequestMessage, self).__init__(from_node, to_node, msg_id)
        self.key = key

    def __str__(self):
        return '{0}({1}=?)'.format(self.__class__.__name__, self.key)


class DynamoResponseMessage(ResponseMessage):
    def __init__(self, req, value, metadata):
        super(DynamoResponseMessage, self).__init__(req)
        self.key = req.key
        self.value = value
        self.metadata = metadata

    def __str__(self):
        return '{0}({1}={2})'.format(self.__class__.__name__, self.key, _show_value(self.value, self.metadata))


class ClientPut(DynamoRequestMessage):
    def __init__(self, from_node, to_node, key, value, metadata, msg_id=None):
        super(ClientPut, self).__init__(from_node, to_node, key, msg_id=msg_id)
        self.value = value
        self.metadata = metadata

    def __str__(self):
        return 'ClientPut({0}={1})'.format(self.key, _show_value(self.value, self.metadata))


class ClientPutRsp(DynamoResponseMessage):
    def __init__(self, req, metadata=None):
        if metadata is None:
            metadata = req.metadata
        super(ClientPutRsp, self).__init__(req, req.value, metadata)


class PutReq(DynamoRequestMessage):
    def __init__(self, from_node, to_node, key, value, metadata, msg_id=None, handoff=None):
        super(PutReq, self).__init__(from_node, to_node, key, msg_id)
        self.value = value
        self.metadata = metadata
        self.handoff = handoff

    def __str__(self):
        if self.handoff is None:
            return 'PutReq({0}={1})'.format(self.key, _show_value(self.value, self.metadata))
        else:
            return ('PutReq({0}={1}, handoff=({2}))'.format(
                     self.key,
                     _show_value(self.value, self.metadata),
                     ','.join([str(x) for x in self.handoff])))


class PutRsp(DynamoResponseMessage):
    def __init__(self, req):
        super(PutRsp, self).__init__(req, req.value, req.metadata)


class ClientGet(DynamoRequestMessage):
    pass


class ClientGetRsp(DynamoResponseMessage):
    pass


class GetReq(DynamoRequestMessage):
    pass


class GetRsp(DynamoResponseMessage):
    pass


class PingReq(Message):
    pass


class PingRsp(ResponseMessage):
    pass
