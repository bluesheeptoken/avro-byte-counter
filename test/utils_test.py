import unittest
from src.avro_byte_counter.utils import merge_counts, count_to_flamegraph_format


class UtilsTests(unittest.TestCase):
    def test_merge_counts_with_items(self):
        items = [
            {"a": 1, "b": {"c": 2, "d": 3}, "e": 4},
            {"a": 5, "b": {"c": 6, "d": 7}, "e": 8},
            {"a": 9, "b": {"c": 10, "d": 11}, "e": 12},
        ]
        merged_item = {"a": 15, "b": {"c": 18, "d": 21}, "e": 24}
        self.assertEqual(merge_counts(items), merged_item)

    def test_merge_counts_without_items(self):
        self.assertEqual(merge_counts([]), {})

    def test_count_to_flamegraph_format(self):
        self.assertEqual(
            count_to_flamegraph_format({"A": {"B": 1, "C": 2}, "D": 3}),
            [("A;B", 1), ("A;C", 2), ("D", 3)],
        )


if __name__ == "__main__":
    unittest.main()
