def is_iter(iterable) -> bool:
    try:
        iter(iterable)
        return True
    except TypeError:
        return False