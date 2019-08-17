import logging

from xavium import settings
from xavium.utils import get_batchsize

_LOG = logging.getLogger(__name__)


class BaseOp(object):
    def get_cost(self):
        return 1


class Op(BaseOp):
    def __init__(self, fn, args):
        self.fn = fn
        self.args = args

    def get_cost(self):
        return 1

    @classmethod
    def from_args(cls, fn, *args):
        result = []
        for arg in args:
            result.append(cls(fn, arg))
        return result

    def execute(self):
        return self.fn(*self.args)

    def __repr__(self):
        return '<%s fn:%s args:%s>' % (self.__class__.__name__, self.fn, self.args)


class BaseParallelOp(BaseOp):
    def __init__(self, fn, arg_set):
        self.fn = fn
        self.arg_set = arg_set

    def __repr__(self):
        return '<%s fn:%s arg_set:%s>' % (self.__class__.__name__, self.fn, self.arg_set)


class ParallelOp(BaseParallelOp):
    def __init__(self, fn, arg_set):
        super(ParallelOp, self).__init__(fn, arg_set)
        self.op_cost = Op(self.fn, self.arg_set[0]).get_cost()

    def get_cost(self):
        batch_size = get_batchsize(self.arg_set, settings.NUM_WORKERS)
        return (self.op_cost * batch_size) + (settings.NUM_WORKERS * settings.WORKER_INIT_COST)

    def execute(self):
        return [self.fn(*args) for args in self.arg_set]


class SerialOp(BaseParallelOp):
    def __init__(self, fn, arg_set):
        super(SerialOp, self).__init__(fn, arg_set)
        self.op_cost = Op(self.fn, self.arg_set[0]).get_cost()

    def get_cost(self):
        return len(self.arg_set) * self.op_cost

    def execute(self):
        return [self.fn(*args) for args in self.arg_set]


class ParallelizableOp(BaseParallelOp):
    def __init__(self, fn, arg_set):
        super(ParallelizableOp, self).__init__(fn, arg_set)
        self.op = self.get_best_op()

    def __repr__(self):
        return '<%s fn:%s arg_set:%s op:%s>' % (self.__class__.__name__,
                                                self.fn,
                                                self.arg_set,
                                                self.op.__class__.__name__)

    def get_best_op(self):
        parallel_op = ParallelOp(self.fn, self.arg_set)
        serial_op = SerialOp(self.fn, self.arg_set)
        if parallel_op.get_cost() < serial_op.get_cost():
            op = parallel_op
        else:
            op = serial_op
        return op

    def get_cost(self):
        return self.op.get_cost()

    def execute(self):
        return self.op.execute()
