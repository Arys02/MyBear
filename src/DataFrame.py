import copy
import numbers
from typing import List, Dict, Union, Callable, Any

from Series import Series


class DataFrame:
    def __init__(self, data, column=None, dtype=None, clone=False) -> None:
        self.size = len(data)
        if isinstance(data, list) and self.size > 0 and isinstance(data[0], Series):
            self.data = [copy.copy(x) for x in data] if clone else data
        elif isinstance(data, list) and isinstance(column, list):
            self.data = [Series(value, col, clone=clone) for value, col in zip(data, column)]
        else:
            raise TypeError

        self.dtype = self.data[0].dtype if self.size > 0 and dtype is None else None

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
            lst = [x[item[0]] for x in self.data[item[1]]]
            s = [Series(lst, "col")]
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

    def read_csv(path: str, delimiter: str = ""):
        return 0  # TODO

    def read_json(
            path: str,
            orient: str = "records"
    ):
        return 0  # TODO

    def groupby(
            self,
            by: Union[List[str], str],
            agg: Dict[str, Callable[[List[Any]], Any]]
    ):
        return 0  # TODO

    def join(
            self,
            other,
            left_on: Union[List[str], str],
            right_on: Union[List[str], str],
            how: str = "left"):
        return 0  # TODO
