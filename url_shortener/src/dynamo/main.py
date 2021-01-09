from dynamo import DynamoNode, DynamoClientNode
from framework import Framework


def main():
    for _ in range(6):
        DynamoNode()
    a = DynamoClientNode('a')
    b = DynamoClientNode('b')
    pref_list = DynamoNode.chash.find_nodes('k1', 5)[0]
    coordinator_a = pref_list[0]
    a.get('k1', destnode=coordinator_a)
    Framework.schedule(timers_to_process=0)
    getrsp = a.last_msg
    a.put('k1', getrsp.metadata, 1, destnode=coordinator_a)
    Framework.schedule(timers_to_process=0)
    a.get('k1', destnode=coordinator_a)
    Framework.schedule(timers_to_process=0)
    pref_list = DynamoNode.chash.find_nodes('k2', 5)[0]
    coordinator_b = pref_list[0]
    b.get('k2', destnode=coordinator_b)
    Framework.schedule(timers_to_process=0)
    getrsp = b.last_msg
    b.put('k2', getrsp.metadata, 2)
    Framework.schedule(timers_to_process=0)
    a.get('k2', destnode=coordinator_b)
    Framework.schedule(timers_to_process=0)


if __name__ == '__main__':
    main()
