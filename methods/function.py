import types
import itertools


def in_range(start, end, value):
    return start <= value <= end


def chunker(iterable, size):
    if isinstance(iterable, (types.GeneratorType, itertools.chain)):
        iterator = iter(iterable)
        for first in iterator:
            yield itertools.chain([first], itertools.islice(iterator, size - 1))
    else:
        for pos in range(0, len(iterable), size):
            yield iterable[pos:pos + size]


def find_duplicates(items, hash=True):
    if hash:
        seen, duplicates = set(), set()
        for x in items:
            if x in seen:
                duplicates.add(x)
            seen.add(x)
        return duplicates
    else:
        seen, duplicates = [], []
        for x in items:
            if x in seen:
                if x not in duplicates:
                    duplicates.append(x)
            else:
                seen.append(x)
        return duplicates
