import copy
import numbers
from typing import List, Dict, Union, Callable, Any
from typing_extensions import Self
import csv

from src.Series import Series


class DataFrame:
    def __init__(self, data: Union[List[Series], List[List[numbers.Number]]], column=None, dtype=None, clone=False) -> None:
        self.size = len(data)
        if isinstance(data, list) and self.size > 0 and isinstance(data[0], Series):
            self.__height = get_series_list_height(data)
            self.data = [x.resize(self.__height) for x in data]
        elif isinstance(data, list) and isinstance(column, list):
            self.__height = get_list_height(data)
            self.data = [Series(value, col, clone=clone, capacity=self.__height) for value, col in zip(data, column)]
        elif self.size == 0:
            self.data = []
        else:
            raise TypeError

        self.dtype = self.data[0].dtype if self.size > 0 and dtype is None else None

        self.__columns_indexes = {}
        for x in range(self.size):
            if self.data[x].name in self.__columns_indexes.keys():
                raise ValueError("duplicate key '{0}' found".format(self.data[x].name))

            self.__columns_indexes[self.data[x].name] = self.data[x]

    @property
    def iloc(self):
        return self

    def __str__(self):
        s = ""
        for i in range(0, self.size):
            s += self.data[i].name + " "
        s += "\n"
        for i in range(0, self.data[0].size):
            for j in range(0, self.size):
                s += str(self[i, j]) + " "
            s += "\n"
        return s

    def __getitem__(self, item):
        if isinstance(item, str):
            return self.__columns_indexes[item]

        if len(item) != 2:
            raise TypeError
        # iloc[n, n] -> VALUE
        if isinstance(item[0], int) and isinstance(item[1], int):
            return self.data[item[1]][item[0]]
        # iloc[a:b, n] -> SERIES
        if isinstance(item[0], slice) and isinstance(item[1], int):
            return self.data[item[1]][item[0]].copy()
        # iloc[n, a:b] -> DATAFRAME
        if isinstance(item[1], slice) and isinstance(item[0], int):
            lst = [x[item[0]:(item[0] + 1)] for x in self.data[item[1]]]
            s = [Series(l.data, l.name) for l in lst]
            return DataFrame(data=s, clone=True)
        # iloc[x:y, a:b] -> DATAFRAME
        if isinstance(item[1], slice) and isinstance(item[0], slice):
            lst = [x[item[0]] for x in self.data[item[1]]]
            s = [Series(l.data, l.name) for l in lst]
            return DataFrame(data=s, clone=True)

        raise IndexError

    def max(self):
        return DataFrame(data=[Series([x.max() for x in self.data], "max")])

    def min(self):
        return DataFrame(data=[Series([x.min() for x in self.data], "min")])

    def mean(self):
        return DataFrame(data=[Series([x.mean() for x in self.data], "mean")])

    def std(self):
        return DataFrame(data=[Series([x.std() for x in self.data], "std")])

    def count(self):
        return DataFrame(data=[Series([x.count() for x in self.data], "std")])

    def read_csv(self: str, delimiter: str = ","):
        with open(self, newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=delimiter)
            rows = list(map(list, zip(*list(reader))))
        s = [Series(list(map(int, x[1:])), x[0]) for x in rows]
        return DataFrame(data=s)

    def read_json(
            path: str,
            orient: str = "records"
    ):
        return 0  # TODO

    def groupby(
            self,
            by: Union[List[str], str] = None,
            agg: Dict[str, Callable[[List[Any]], Any]] = None
    ):
        real_list = self.check_columns(by)
        if len(real_list) < 1:
            return

        new_df = self.new_groupby_df(real_list)

        return 0  # TODO

    def new_groupby_df(self, real_list):
        print('yes')

        for x in range(len(real_list)):
            column_to_keep = self.get_column_names().index(real_list[x])
            print(column_to_keep)
        return self

    def check_columns(self, by):
        real_list = []
        for column in by:
            if column not in self.get_column_names():
                print("error, column is not in list")
            else:
                real_list.append(column)
        if len(real_list) < 1:
            print("GroupBy aborted, no matching columns passed")
        return real_list

    def get_column_names(self):
        names = []
        for x in range(self.size):
            names.append(self.data[x].name)
        return names

    def without(
            self,
            column_name: str
    ) -> Self:
        index = self.get_col_index(column_name)
        if index == -1:
            return self

        return self.iloc[0:, 0: index].glue(self.iloc[0:, index+1:])

    def get_col_index(
            self,
            column_name: str
    ) -> int:
        for column_index in range(len(self.data)):
            if self.data[column_index].name == column_name:
                return column_index

        return -1

    def join(
            self,
            other: Self,
            left_on: Union[List[str], str],
            right_on: Union[List[str], str],
            how: str = "left") -> Self:

        validate_join_params(left_on, right_on, how)

        if how == "inner":
            print("impl")
        elif how == "outer":
            print("impl")
        elif how == "right":
            return self.join_left(other, self, right_on, left_on)
        else:
            return self.join_left(self, other, left_on, right_on)

    def join_left(
            self,
            left_df: Self,
            right_df: Self,
            left_on: Union[List[str], str],
            right_on: Union[List[str], str],
    ) -> Self:
        if isinstance(left_on, str):
            return self.join_left_unique_key(left_df, right_df, left_on, right_on)
        return self.join_left_multi_key(left_df, right_df, left_on, right_on)

    def join_left_unique_key(
            self,
            left_df: Self,
            right_df: Self,
            left_on: str,
            right_on: str
    ) -> Self:
        joined_on: Series = left_df[left_on]
        joining_on: Series = right_df[right_on]

        joining_rows_indexes: List[(int, int)] = []
        for joined_row_index in range(len(joined_on.data)):
            for joining_row_index in range(len(joining_on.data)):
                if joining_on.data[joining_row_index] == joined_on.data[joined_row_index]:
                    joining_rows_indexes.append((joined_row_index, joining_row_index))
                    break

        print("JOINING ROW INDEXES:")
        print(joining_rows_indexes)

    def join_left_multi_key(
            self,
            left_df: Self,
            right_df: Self,
            left_on: List[str],
            right_on: List[str]
    ) -> Self:
        pass

    def glue(
            self,
            other: Self) -> Self:

        glued_height = self.__height
        glued_series: List[Series] = []
        for column in self.data:
            glued_series.append(column)
        for column in other.data:
            glued_series.append(column.resize(glued_height))

        return DataFrame(glued_series)


def validate_join_params(
        left_on: Union[List[str], str],
        right_on: Union[List[str], str],
        how: str):
    left_on_and_right_on_are_same_type(left_on, right_on)

    if isinstance(left_on, list) and not len(left_on) == len(right_on):
        raise TypeError("right_on and left_on need to be the same size")

    if how not in ["left", "right", "inner", "outer"]:
        raise TypeError("how value can only be: left, right, inner, outer (got {0})".format(how))


def left_on_and_right_on_are_same_type(
        left_on: Union[List[str], str],
        right_on: Union[List[str], str]):
    if isinstance(left_on, list) and not isinstance(right_on, list):
        raise TypeError("right_on and left_on need to have the same type")
    if isinstance(left_on, str) and not isinstance(right_on, str):
        raise TypeError("right_on and left_on need to have the same type")


def get_series_indexes(indexes: Union[List[str], str]) -> List[str]:
    if isinstance(indexes, str):
        return [indexes]
    return indexes


def get_series_list_height(series_list: List[Series]) -> int:
    height = 0
    for series in series_list:
        if series.size > height:
            height = series.size

    return height


def get_list_height(some_list_of_list: List[List]) -> int:
    height = 0
    for some_list in some_list_of_list:
        if len(some_list) > height:
            height = len(some_list)

    return height
