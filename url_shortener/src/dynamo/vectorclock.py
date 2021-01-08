from __future__ import annotations
import copy
import sys
import time
import typing as t


class VectorClock:
    def __init__(self):
        self.clock = {}

    def update(self, node, counter: int) -> VectorClock:
        if node in self.clock and counter <= self.clock[node]:
            raise Exception(f'Node {node} has gone backwards from {self.clock[node]} to {counter}')
        self.clock[node] = counter
        return self

    def __str__(self):
        return '{0}'.format(', '.join(['{0}:{1}'.format(node, self.clock[node])
                                   for node in sorted(self.clock.keys())]))

    def __eq__(self, other):
        return self.clock == other.clock

    def __lt__(self, other):
        for node in self.clock:
            if node not in other.clock:
                return False
            elif self.clock[node] > other.clock[node]:
                return False
        return True

    def __ne__(self, other):
        return not (self == other)

    def __le__(self, other):
        return (self == other) or (self < other)

    def __gt__(self, other):
        return (other < self)

    def __ge__(self, other):
        return (self == other) or (self > other)

    @classmethod
    def coalesce(cls, vcs: t.List[VectorClock]) -> t.List[VectorClock]:
        results = []
        for vc in vcs:
            subsumed = False
            for i, result in enumerate(results):
                if vc <= result:
                    subsumed = True
                    break
                if result < vc:
                    results[i] = copy.deepcopy(vc)
                    subsumed = True
                    break
            if not subsumed:
                results.append(copy.deepcopy(vc))
        return results

    @classmethod
    def coverage(cls, vcs: t.List[VectorClock]) -> VectorClock:
        result = cls()
        for vc in vcs:
            if vc is None:
                continue
            for node, counter in vc.clock.items():
                if node in result.clock:
                    if result.clock[node] < counter:
                        result.clock[node] = counter
                else:
                    result.clock[node] = counter
        return result


class VectorClockTimestamp(VectorClock):
    NODE_LIMIT = 10

    def __init__(self):
        super(VectorClockTimestamp, self).__init__()
        self.clock_time = {}

    def _maybe_truncate(self):
        if len(self.clock_time) < VectorClockTimestamp.NODE_LIMIT:
            return

        oldest_node = None
        oldest_time = sys.maxint
        for node, when in self.clock_time.items():
            if when < oldest_time:
                oldest_node = node
                oldest_time = when
        del self.clock_time[oldest_node]
        del self.clock[oldest_node]

    def update(self, node, counter: int) -> VectorClockTimestamp:
        VectorClock.update(self, node, counter)
        self.clock_time[node] = time.time()
        self._maybe_truncate()
        return self

