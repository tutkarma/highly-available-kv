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

    @classmethod
    def remove_req_timer(cls, reqmsg):
        if reqmsg in cls.pending_timers:
            TimerManager.cancel_timer(cls.pending_timers[reqmsg])
            del cls.pending_timers[reqmsg]

    @classmethod
    def schedule(cls, msgs_to_process=None, timers_to_process=None):
        if msgs_to_process is None:
            msgs_to_process = 32768
        if timers_to_process is None:
            timers_to_process = 32768

        while cls._work_to_do():
            while cls.queue:
                msg = cls.queue.popleft()
                print(msg)
                if isinstance(msg, ResponseMessage):
                    try:
                        reqmsg = msg.response_to.original_msg
                    except:
                        reqmsg = msg.response_to
                    cls.remove_req_timer(reqmsg)

                msg.to_node.rcvmsg(msg)
            msgs_to_process -= 1
            if msgs_to_process == 0:
                return

        if TimerManager.pending_count() > 0 and timers_to_process > 0:
            TimerManager.pop_timer()
            timers_to_process -= 1
        if timers_to_process == 0:
            return

    @classmethod
    def _work_to_do(cls):
        if cls.queue:
            return True
        if TimerManager.pending_count() > 0:
            return True
        return False