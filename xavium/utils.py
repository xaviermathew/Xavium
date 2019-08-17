import math


def get_batchsize(iterable, num_workers):
    return int(math.ceil(len(iterable)/float(num_workers)))
