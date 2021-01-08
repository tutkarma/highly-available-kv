import hashlib
import binascii
import bisect
import typing as t


class HashTable:
    def __init__(self, nodelist, repeat: int):
        baselist = []
        for node in nodelist:
            for i in range(repeat):
                nodestring = f'{node}{i}'
                baselist.append((hashlib.md5(str(node).encode('utf-8')).digest(), node))

        self.nodelist = sorted(baselist, key=lambda x: x[0])
        self.hashlist = [hashnode[0] for hashnode in self.nodelist]

    def find_nodes(self, key: str, count: int = 1, avoid: t.Set[t.Any] = None):
        if avoid is None:
            avoid = set()

        hv = hashlib.md5(str(key).encode('utf-8')).digest()
        init_index = bisect.bisect(self.hashlist, hv)
        next_index = init_index
        results = []
        avoided = []
        while len(results) < count:
            if next_index == len(self.nodelist):
                next_index = 0
            node = self.nodelist[next_index][1]
            if node in avoid:
                if node not in avoided:
                    avoided.append(node)
            elif node not in results:
                results.append(node)
            next_index += 1
            if next_index == init_index:
                break
        return results, avoided

    def __str____(self):
        return ",".join(["(%s, %s)".format(binascii.hexlify(nodeinfo[0]), nodeinfo[1]) for nodeinfo in self.nodelist])
