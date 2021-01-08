import dynamo



def main():
    for _ in range(6):
        dynamo.DynamoNode()
    a = dynamo.DynamoClientNode('a')
    b = dynamo.DynamoClientNode('b')
    pref_list = dynamo.DynamoNode.chash.find_nodes('k1', 5)[0]
    coordinator = pref_list[0]
    a.get('k1', destnode=coordinator)
    getrsp = a.last_msg
    a.put('k1', getrsp.metadata, 1, destnode=coordinator)
    a.get('k1', destnode=coordinator)
    b.put('k2', None, 2)
    a.get('k2', destnode=coordinator)
    print(a)
    print(pref_list)


if __name__ == '__main__':
    main()
