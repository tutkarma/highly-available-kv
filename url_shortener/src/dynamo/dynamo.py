import copy
import random

from dynamomessages import (
    ClientPut,
    ClientGet,
    ClientPutRsp,
    ClientGetRsp,
    PutReq,
    GetReq,
    PutRsp,
    GetRsp,
    DynamoRequestMessage,
    PingReq,
    PingRsp
)
from framework import Framework
from hashtable import HashTable
from node import Node
from vectorclock import VectorClock


class DynamoNode(Node):
    time_priority = 20
    T = 10
    N = 3
    W = 2
    R = 2
    nodelist = []
    chash = HashTable(nodelist, T)

    def __init__(self):
        super(DynamoNode, self).__init__()
        self.local_store = {}
        self.pending_put_rsp = {}
        self.pending_put_msg = {}
        self.pending_get_rsp = {}
        self.pending_get_msg = {}
        self.pending_req = {PutReq: {}, GetReq: {}}
        self.failed_nodes = []

        DynamoNode.nodelist.append(self)
        DynamoNode.chash = HashTable(DynamoNode.nodelist, DynamoNode.T)

    def store(self, key, value, metadata):
        self.local_store[key] = (value, metadata)

    def retrieve(self, key):
        return self.local_store.get(key, (None, None))

    def rcv_clientput(self, msg):
        preference_list, avoided = DynamoNode.chash.find_nodes(msg.key, DynamoNode.N, self.failed_nodes)

        avoided = avoided[:DynamoNode.N]
        non_extra_count = DynamoNode.N - len(avoided)

        if self not in preference_list:
            coordinator = preference_list[0]
            Framework.forward_message(msg, coordinator)
        else:
            seqno = self.generate_seq_num()
            metadata = copy.deepcopy(msg.metadata)
            metadata.update(self.name, seqno)
            self.pending_put_rsp = set()
            self.pending_put_msg = msg
            reqcount = 0
            for i, node in enumerate(preference_list):
                handoff = avoided if (i >= non_extra_count) else None
                putmsg = PutReq(self, node, msg.key, msg.value, metadata, msg_id=seqno, handoff=handoff)
                Framework.send_message(putmsg)
                reqcount += 1
                if reqcount >= DynamoNode.N:
                    break

    def rcv_put(self, putmsg):
        self.store(putmsg.key, putmsg.value, putmsg.metadata)
        if putmsg.handoff is not None:
            for failed_node in putmsg.handoff:
                self.failed_nodes.append(failed_node)
                if failed_node not in self.pending_handoffs:
                    self.pending_handoffs[failed_node].add(putmsg.key)
        putrsp = PutRsp(putmsg)
        Framework.send_message(putrsp)

    def rcv_putrsp(self, putrsp):
        seqno = putrsp.msg_id
        if seqno in self.pending_put_rsp:
            self.pending_put_rsp[seqno].add(putrsp.from_node)
            orig_msg = self.pending_put_msg[seqno]
            del self.pending_req[PutReq][seqno]
            del self.pending_put_rsp[seqno]
            del self.pending_put_msg[seqno]
            client_putrsp = ClientPutRsp(orig_msg)
            Framework.send_message(client_putrsp)

    def rcv_clientget(self, msg):
        preference_list = DynamoNode.chash.find_nodes(msg.key, DynamoNode.N, self.failed_nodes)[0]
        if self not in preference_list:
            coordinator = preference_list[0]
            Framework.send_message(msg, coordinator)
        else:
            seqno = self.generate_seq_num()
            self.pending_req[GetReq][seqno] = set()
            self.pending_get_rsp[seqno] = set()
            self.pending_get_msg[seqno] = msg
            reqcount = 0
            for node in preference_list:
                getmsg = GetReq(self, node, msg.key, msg_id=seqno)
                self.pending_req[GetReq][seqno].add(getmsg)
                Framework.send_message(getmsg)
                reqcount += 1
                if reqcount >= DynamoNode.N:
                    break

    def rcv_get(self, getmsg):
        value, metadata = self.retrieve(getmsg.key)
        getrsp = GetRsp(getmsg, value, metadata)
        Framework.send_message(getrsp)

    def rcv_getrsp(self, getrsp):
        seqno = getrsp.msg_id
        if seqno in self.pending_get_rsp:
            self.pending_get_rsp[seqno].add((getrsp.from_node, getrsp.value, getrsp.metadata))
            if len(self.pending_get_rsp[seqno]) >= DynamoNode.R:
                results = set([(value, metadata) for (node, value, metadata) in self.pending_get_rsp[seqno]])
                orig_msg = self.pending_get_msg[seqno]
                del self.pending_req[GetReq][seqno]
                del self.pending_get_rsp[seqno]
                del self.pending_get_msg[seqno]
                client_getrsp = ClientGetRsp(orig_msg,
                                             [value for (value, metadata) in results],
                                             [metadata for (value, metadata) in results])
                Framework.send_message(client_getrsp)

    def rsp_timer_pop(self, reqmsg):
        self.failed_nodes.append(reqmsg.to_node)
        failed_requests = Framework.cancel_timers_to(reqmsg.to_node)
        failed_requests.append(reqmsg)
        for failedmsg in failed_requests:
            self.retry_request(failedmsg)

    def retry_request(self, reqmsg):
        if not isinstance(reqmsg, DynamoRequestMessage):
            return

        preference_list = DynamoNode.chash.find_nodes(reqmsg.key, DynamoNode.N, self.failed_nodes)[0]
        kls = reqmsg.__class__
        if kls in self.pending_req and reqmsg.msg_id in self.pending_req[kls]:
            for node in preference_list:
                if node not in [req.to_node for req in self.pending_req[kls][reqmsg.msg_id]]:
                    newreqmsg = copy.deepcopy(reqmsg)
                    newreqmsg.to_node = node
                    self.pending_req[kls][reqmsg.msg_id].add(newreqmsg)
                    Framework.send_message(newreqmsg)

    def retry_failed_node(self, _):
        if self.failed_nodes:
            node = self.failed_nodes.pop(0)
            pingmsg = PingReq(self, node)
            Framework.send_message(pingmsg)
        TimeManager.start_timer(self, reason='retry', priority=15, callback=self.retry_failed_node)

    def rcv_pingreq(self, pingmsg):
        pingrsp = PingRsp(pingmsg)
        Framework.send_message(pingrsp)

    def rcv_pingrsp(self, pingmsg):
        revocered_node = pingmsg.from_node
        while recovered_node in self.failed_nodes:
            self.failed_nodes.remove(recovered_node)
        if recovered_node in self.pending_handoffs:
            for key in self.pending_handoffs[recovered_node]:
                value, metadata = self.retrieve(key)
                putmsg = PutReq(self, recovered_node, key, value, metadata)
                Framework.send_message(putmsg)
            del self.pending_handoffs[recovered_node]

    def rcvmsg(self, msg):
        if isinstance(msg, ClientPut):
            self.rcv_clientput(msg)
        elif isinstance(msg, PutReq):
            self.rcv_put(msg)
        elif isinstance(msg, PutRsp):
            self.rcv_putrsp(msg)
        elif isinstance(msg, ClientGet):
            self.rcv_clientget(msg)
        elif isinstance(msg, GetReq):
            self.rcv_get(msg)
        elif isinstance(msg, GetRsp):
            self.rcv_getrsp(msg)
        elif isinstance(msg, PingReq):
            self.rcv_pingreq(msg)
        elif isinstance(msg, PingRsp):
            self.rcv_pingrsp(msg)
        else:
            raise TypeError(f'Unexpected message type {msg.__class__}')


class DynamoClientNode(Node):
    timer_priority = 17

    def __init__(self, name=None):
        super(DynamoClientNode, self).__init__(name)
        self.last_msg = None

    def put(self, key, metadata, value, destnode=None):
        if destnode is None:
            destnode = random.choice(DynamoNode.nodelist)
        if len(metadata) == 1 and metadata[0] is None:
            metadata = VectorClock()
        else:
            metadata = VectorClock.converge(metadata)
        putmsg = ClientPut(self, destnode, key, value, metadata)
        Framework.send_message(putmsg)
        return putmsg

    def get(self, key, destnode=None):
        if destnode is None:
            destnode = random.choice(DynamoNode.nodelist)
        getmsg = ClientGet(self, destnode, key)
        Framework.send_message(getmsg)
        return getmsg

    def rsp_timer_pop(self, reqmsg):
        if isinstance(reqmsg, ClientPut):
            self.put(reqmsg.key, [reqmsg.metadata], reqmsg.value)
        elif isinstance(reqmsg, ClientGet):
            self.get(reqmsg.key)

    def rcvmsg(self, msg):
        self.last_msg = msg
