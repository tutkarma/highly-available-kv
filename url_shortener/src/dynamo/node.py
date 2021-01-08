from __future__ import annotations
import typing as t


class Node:
    count: int  = 0
    node: t.Dict[t.Any, Node] = {}
    name: t.Dict[Node, t.Any] = {}

    @classmethod
    def reset(cls):
        cls.count = 0
        cls.node = {}
        cls.name = {}

    @classmethod
    def next_name(cls):
        if cls.count < 26:
            name = chr(ord('A') + cls.count)
        elif cls.count < (26**2):
            hi = cls.count / 26
            lo = cls.count % 26
            name = chr(ord('A') + hi - 1) + chr(ord('A') + lo)
        else:
            raise NotImplemented
        cls.count += 1
        return name

    def __init__(self, name: str = None):
        if name is None:
            self.name = Node.next_name()
        else:
            self.name = name

        self.next_seq_num = 0
        self.included = True
        self.failed = False
        Node.node[self.name] = self
        Node.name[self] = self.name

    def __str__(self):
        return self.name

    def fail(self):
        self.failed = True

    def recover(self):
        self.failed = False

    def remove(self):
        self.included = False

    def restore(self):
        self.included = True

    def generate_seq_num(self):
        self.next_seq_num += 1
        return self.next_seq_num

    def rcvmsg(self, msg):
        raise NotImplemented

    def timer_pop(self, reason=None):
        raise NotImplemented

