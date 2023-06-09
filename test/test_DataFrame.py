import math
from unittest import TestCase

from src.DataFrame import DataFrame
from src.Series import Series


class TestDataFrame(TestCase):
    def test_iloc(self):
        df_a = DataFrame(column=["a", "b", "c"], data=[[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 20]])
        self.assertEqual(df_a.iloc[0, 0], 1)

    def test_groupby(self, column_to_group):
        df_a = DataFrame(column=["Name", "Record", "Super Bowl Wins", "Uniform Coolness"],
                         data=[["Cheifs", "7-7", 3, "OK"], ["Chargers", "0-14", 0, "Sick"], ["Broncos", "7-7", 3, "OK"],
                               ["Bills", "7-7", 1, "OK"]])
        self.assertEqual()

    def test_joinShouldJoinTwoDataFramesInOne(self):
        series_left = Series([1, 2, 3, 4], "a")
        series_right = Series([3, 4, 6, 1], "b")
        dataframe_left = DataFrame([series_left], clone=True)
        dataframe_right = DataFrame([series_right], clone=True)

        dataframe_joined = dataframe_left.join(dataframe_right, left_on="a", right_on="b")

        self.assertEqual(dataframe_joined.iloc[0, 0], 1)
        self.assertEqual(dataframe_joined.iloc[0, 1], 3)

    def test_joinShouldSetRightExcessValuesAsNaN(self):
        series_left = Series([1, 2, 3, 4], "a")
        series_right = Series([3, 4], "b")
        dataframe_left = DataFrame([series_left], clone=True)
        dataframe_right = DataFrame([series_right], clone=True)

        dataframe_joined = dataframe_left.join(dataframe_right, left_on="a", right_on="b")

        self.assertTrue(dataframe_joined.iloc[2, 1] is None)
        self.assertTrue(dataframe_joined.iloc[3, 1] is None)

    def test_joinShouldJoinMultipleLeftOnValues(self):
        first_series_left = Series([1, 1, 1, 1], "a")
        second_series_left = Series([2, 2, 2, 2], "b")
        first_series_right = Series([3, 3, 3, 3], "c")

        dataframe_left = DataFrame([first_series_left, second_series_left], clone=True)
        dataframe_right = DataFrame([first_series_right], clone=True)

        dataframe_joined = dataframe_left.join(dataframe_right, left_on=["a", "b"], right_on="c")

        self.assertEqual(dataframe_joined.iloc[0, 0], 1)
        self.assertEqual(dataframe_joined.iloc[0, 1], 2)
        self.assertEqual(dataframe_joined.iloc[0, 2], 3)

    def test_joinShouldJoinMultipleRightOnValues(self):
        first_series_left = Series([1, 1, 1, 1], "a")
        first_series_right = Series([2, 2, 2, 2], "b")
        second_series_right = Series([3, 3, 3, 3], "c")

        dataframe_left = DataFrame([first_series_left], clone=True)
        dataframe_right = DataFrame([first_series_right, second_series_right], clone=True)

        dataframe_joined = dataframe_left.join(dataframe_right, left_on="a", right_on=["b", "c"])

        self.assertEqual(dataframe_joined.iloc[0, 0], 1)
        self.assertEqual(dataframe_joined.iloc[0, 1], 2)
        self.assertEqual(dataframe_joined.iloc[0, 2], 3)

    def test_joinShouldShortenRightDataframe(self):
        series_left = Series([1, 1], "a")
        series_right = Series([2, 2, 2, 2], "b")

        dataframe_left = DataFrame([series_left], clone=True)
        dataframe_right = DataFrame([series_right], clone=True)

        dataframe_joined = dataframe_left.join(dataframe_right, left_on="a", right_on="b")

        print(dataframe_joined)

        self.assertEqual(dataframe_joined["b"].size, 2)
