from unittest import TestCase

from src.DataFrame import DataFrame


class TestDataFrame(TestCase):
    def test_iloc(self):
        df_a = DataFrame(column=["a", "b", "c"], data=[[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 20]])
        self.assertEqual(df_a.iloc[0, 0], 1)
