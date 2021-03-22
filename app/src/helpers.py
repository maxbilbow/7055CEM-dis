from typing import Callable


def flatten(list_list: list, predicate: Callable[[object], bool] = lambda o: True) -> list:
    flat_list = []
    for list in list_list:
        for item in list:
            if predicate(item):
                flat_list.append(item)

    return flat_list
