COMMAND_REGISTRY = {}


def op(*args, **kwargs):
    if args:
        fn = args[0]
        COMMAND_REGISTRY[fn] = {'parallelizable': False}
        return fn
    else:
        def wrapper(fn):
            COMMAND_REGISTRY[fn] = {'parallelizable': False}
            COMMAND_REGISTRY[fn].update(kwargs)
            return fn
        return wrapper


def is_parallelizable(fn):
    return COMMAND_REGISTRY[fn]['parallelizable']
