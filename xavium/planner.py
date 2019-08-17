import logging

from xavium.commands import is_parallelizable
from xavium.ops import ParallelizableOp, Op

_LOG = logging.getLogger(__name__)


class Planner(object):
    def __init__(self, steps):
        self.steps = steps

    def execute(self):
        results = None
        for step in self.steps:
            fn = step[0]
            if is_parallelizable(fn):
                if results is not None:
                    if step[2]:
                        # only a sanity check
                        raise RuntimeError('Intermediary parallelizable steps should not have parallel_args')
                    common_args = step[1]
                    arg_set = [common_args + [r] for r in results]
                else:
                    common_args, parallel_args = step[1], step[2]
                    arg_set = [common_args + [args] for args in parallel_args]
                op = ParallelizableOp(fn, arg_set)
            else:
                if step[2]:
                    # only a sanity check
                    raise RuntimeError('Non-parallelizable steps should not have parallel_args')
                cmd_args = step[1]
                if results is not None:
                    args = cmd_args + results
                else:
                    args = cmd_args
                op = Op(fn, args)
            _LOG.debug('exec op:%s', op)
            results = op.execute()
        return results
