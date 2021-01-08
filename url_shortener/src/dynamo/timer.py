import typing as t

from message import Timer


DEFAULT_PRIORITY = 10

def _priority(tmsg: Timer) -> int:
    priority = DEFAULT_PRIORITY
    node = tmsg.from_node
    if 'timer_priority' in node.__class__.__dict__:
        priority = int(node.__class__.__dict__['timer_priority'])
    return priority


class TimerManager:
    pending: t.List[t.Tuple[int, Timer]] = []

    @classmethod
    def pending_count(cls) -> int:
        return len(cls.pending)

    @classmethod
    def reset(cls):
        cls.pending = []

    @classmethod
    def start_timer(cls, node, reason=None, callback=None, priority=None) -> Timer:
        if node.failed:
            return None

        tmsg = Timer(node, reason, callback)
        if priority is None:
            priority = _priority(tmsg)

        for i, timer in enumerate(cls.pending):
            if priority > timer[0]:
                cls.pending.insert(i, (priority, tmsg))
                return tmsg
        cls.pending.append((priority, tmsg))
        return tmsg

    @classmethod
    def cancel_timer(cls, tmsg):
        for (this_prio, this_tmsg) in cls.pending:
            if this_tmsg == tmsg:
                cls.pending.remove((this_prio, this_tmsg))
                return

    @classmethod
    def pop_timer(cls):
        while True:
            _, tmsg = cls.pending.pop(0)
            if tmsg.from_node.failed:
                continue
            if tmsg.callback is None:
                tmsg.from_node.timer_pop(tmsg.reason)
            else:
                tmsg.callback(tmsg.reason)
            return
