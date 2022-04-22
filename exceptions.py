def read_item(name):
    global items
    myitems = list(filter(lambda x: x['name'] == name, items))
    return myitems[0]


def read_items():
    global items
    return [item for item in items]