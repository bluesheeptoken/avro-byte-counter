from typing import List, Tuple

from avro_byte_counter.type import CountPerField


def merge_counts(counts: List[CountPerField]) -> CountPerField:
    # all the counts are supposed to be of the same types
    if not counts:
        return {}
    results = {}
    for key, value in counts[0].items():
        if isinstance(value, int):
            results[key] = sum(map(lambda item: item[key], counts))  # pyright: ignore
        else:
            results[key] = merge_counts(
                list(map(lambda item: item[key], counts))  # pyright: ignore
            )
    return results


def count_to_flamegraph_format(count: CountPerField) -> List[Tuple[str, int]]:
    def loop(count: CountPerField, pointer: List[str], results: List[Tuple[str, int]]):
        for key, value in count.items():
            pointer_with_key = pointer + [key]
            if isinstance(value, int):
                results.append(((";".join(pointer_with_key)), value))
            else:
                loop(value, pointer_with_key, results)
        return results

    return loop(count, [], [])
