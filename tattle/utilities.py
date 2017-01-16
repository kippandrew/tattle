def partition(fun, iterable):
    trues = list()
    falses = list()
    for i in iterable:
        if fun(i):
            trues.append(i)
        else:
            falses.append(i)
    return trues, falses
