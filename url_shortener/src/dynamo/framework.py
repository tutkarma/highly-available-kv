import copy
from collections import deque

from timer import TimerManager
from message import ResponseMessage


class Framework:
    cuts = []
    queue = deque([])
    pending_timers = {}

    @classmethod
    def send_message(cls, msg, expect_reply=True):
        cls.queue.append(msg)
        if (expect_reply and
            not isinstance(msg, ResponseMessage) and
            'rsp_timer_pop' in msg.from_node.__class__.__dict__ and
            callable(msg.from_node.__class__.__dict__['rsp_timer_pop'])):
            cls.pending_timers[msg] = TimerManager.start_timer(msg.from_node, reason=msg, callback=Framework.rsp_timer_pop)

    @classmethod
    def rsp_timer_pop(cls, reqmsg):
        del cls.pending_timers[reqmsg]
        reqmsg.from_node.rsp_timer_pop(reqmsg)

    @classmethod
    def forward_message(cls, msg, new_to_node):
        fwd_msg = copy.deepcopy(msg)
        fwd_msg.intermediate_node = fwd_msg.to_node
        fwd_msg.original_msg = msg
        fwd_msg.to_node = new_to_node
        cls.queue.append(fwd_msg)

