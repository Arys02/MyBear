import copy
from typing import List, Dict, Union, Callable, Any
import csv

from src.Series import Series


class DataFrame:
    def __init__(self, data, column=None, dtype=None, clone=False) -> None:
        self.size = len(data)
        if isinstance(data, list) and self.size > 0 and isinstance(data[0], Series):
            self.__height = get_series_list_height(data)
            self.data = [x.resize(self.__height) for x in data]
        elif isinstance(data, list) and isinstance(column, list):
            self.__height = get_list_height(data)
            self.data = [Series(value, col, clone=clone, capacity=self.__height) for value, col in zip(data, column)]
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
        if len(item) != 2:
            raise TypeError
        #iloc[n, n] -> VALUE
        if isinstance(item[0], int) and isinstance(item[1], int):
            return self.data[item[1]][item[0]]
        #iloc[a:b, n] -> SERIES
        if isinstance(item[0], slice) and isinstance(item[1], int):
            return self.data[item[1]][item[0]].copy()
        #iloc[n, a:b] -> DATAFRAME
        if isinstance(item[1], slice) and isinstance(item[0], int):
            lst = [x[item[0]:(item[0] + 1)] for x in self.data[item[1]]]
            s = [Series(l.data, l.name) for l in lst]
            return DataFrame(data=s, clone=True)
        #iloc[x:y, a:b] -> DATAFRAME
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
            rows = list(map(list, zip(* list(reader))))
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
        real_list = self.check_columns(by, agg)
        if len(real_list) < 1:
            return

        self.__dict__.update(self.new_groupby_df(real_list).__dict__)
        return

    def new_groupby_df(self, real_list):
        keys = []
        values = []
        for x in range(len(real_list)):
            keys.append(self.__columns_indexes[real_list[x]].name)
            values.append(self.__columns_indexes[real_list[x]])

        new_df = DataFrame(column=keys, data=values)
        return new_df

    def check_columns(self, by, agg=None):
        if agg is None:
            agg = {}
        real_list = []

        if self.is_empty_dict(agg):
            pass
        elif self.is_overlapping_arrays(by, agg.keys()):
            raise ValueError("by and aggregate contain the same value")

        for column in by:
            if column not in self.__columns_indexes.keys():
                print("error, column is not in list")
            else:
                real_list.append(column)
        for column in agg:
            if column not in self.__columns_indexes.keys():
                print("error, column is not in list")
            else:
                real_list.append(column)
        if len(real_list) < 1:
            print("GroupBy aborted, no matching columns passed")
        return real_list

    def is_empty_dict(self, dict):
        return not bool(dict)

    def is_overlapping_arrays(self, array1, array2):
        return not set(array1).isdisjoint(set(array2))

    def get_column_names(self):
        names = []
        for x in range(self.size):
            names.append(self.data[x].name)
        return names

    def join(
            self,
            other,
            left_on: Union[List[str], str],
            right_on: Union[List[str], str],
            how: str = "left"):
        return 0  # TODO

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
