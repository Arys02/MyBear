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

    def new_groupby_df(self, real_list):
        keys = []
        values = []
        for x in range(len(real_list)):
            keys.append(real_list[x].name)
            values.append(real_list[x].data)

        new_df = DataFrame(column=keys, data=values)
        return new_df

    def check_columns(self, by, agg=None):
        if agg is None:
            agg = {}

        if self.is_empty_dict(agg):
            pass
        elif self.is_overlapping_arrays(by, agg.keys()):
            raise ValueError("by and aggregate contain the same key")

        real_list = self.data_to_series_list(by, agg)

        if len(real_list) < 1:
            print("GroupBy aborted, no matching columns passed")
        return real_list

    def get_columns_indexes(self):
        return self.__columns_indexes

    def data_to_series_list(self, by, agg=None):
        access_as_rows = self.makeColumnsRows()
        real_list = []
        used_values = []
        shared_values = []
        repeat_rows = []
        agg_raw_list = []

        for x in range(len(access_as_rows)):
            for y in range(len(by)):
                column_num = list(self.__columns_indexes.keys()).index(by[y])
                if by[y] not in self.__columns_indexes.keys():
                    print("error, column is not in list")
                elif access_as_rows[x][column_num] in used_values:
                    shared_values.append(access_as_rows[x])
                    repeat_rows.append([x, access_as_rows[x][column_num], by[y]])
                    #access_as_rows[x][column_num] = "repeated"
                    continue

                used_values.append(access_as_rows[x][column_num])

        first_sames = self.find_first_samesies(access_as_rows, repeat_rows)

        for x in range(len(agg)):
            agg_val_list = list(agg.values())
            agg_key_list = list(agg.keys())

            column_num = list(self.__columns_indexes.keys()).index(agg_key_list[x])

            if agg_key_list[x] not in self.__columns_indexes.keys():
                print("error, column is not in list")
            else:
                #print("your agg column is here " + str(agg_key_list[x]))
                #print("your agg function is here " + str(agg_val_list[x]))

                for y in range(len(first_sames)):
                    new_list = []
                    temp_list = []
                    new_list.append((agg_key_list[x]))
                    new_list.append(first_sames[y][1])
                    temp_list.append(access_as_rows[first_sames[y][0]][column_num])
                    for z in range(len(repeat_rows)):
                        if first_sames[y][1] == repeat_rows[z][1]:
                            temp_list.append(access_as_rows[repeat_rows[z][0]][column_num])
                    new_list.append(agg_val_list[x](temp_list))
                    agg_raw_list.append(new_list)

        reversed_repeat = repeat_rows[::-1]
        for x in range(len(repeat_rows)):
            access_as_rows.pop(reversed_repeat[x][0])

        # print("THE NEW agg_list ROWS " + str(agg_raw_list))
        # print("THE NEW ACCESS ROWS " + str(access_as_rows))

        access_as_rows = self.modify_the_aggs(agg_raw_list, access_as_rows, first_sames)


        access_as_rows = self.makeRowsColumns(access_as_rows)

        real_final_list = []
        #print("THE GOOD STUFF HERE PLZ" + str(access_as_rows))
        for x in range(len(access_as_rows)):
            name = list(self.__columns_indexes.keys())
            temp_boy = Series(data=access_as_rows[x], name=name[x])
            real_final_list.append(temp_boy)

        #print(real_final_list)

        return real_final_list

    def modify_the_aggs(self, agg_raw_list, access_as_rows, first_sames):
        final_list = []

        for x in range(len(agg_raw_list)):
            column_num_to_insert = list(self.__columns_indexes.keys()).index(agg_raw_list[x][0])
            value_to_align = agg_raw_list[x][1]
            for y in range(len(access_as_rows)):
                if first_sames[y][1] == value_to_align:
                    #print(value_to_align + "HERE")
                    access_as_rows[y][column_num_to_insert] = agg_raw_list[x][2]

        #print(access_as_rows)
        return access_as_rows

    def find_first_samesies(self, access_as_rows, repeats):
        final_array = []
        for x in range(len(repeats)):
            for y in range(len(access_as_rows)):
                column_num = list(self.__columns_indexes.keys()).index(repeats[x][2])

                if access_as_rows[y][column_num] == repeats[x][1]:
                    final_array.append([y, access_as_rows[y][column_num], repeats[x][2]])
                    for x in range(len(final_array)):
                        for y in range(x + 1, len(final_array)):
                            if final_array[x][1] == final_array[y][1]:
                                final_array.pop(y)
                    break

        # print("the original array is" + str(repeats))
        # print("the final array is" + str(final_array))

        return final_array

    def is_empty_dict(self, the_dict):
        return not bool(the_dict)

    def is_overlapping_arrays(self, array1, array2):
        return not set(array1).isdisjoint(set(array2))

    def makeColumnsRows(self):
        final_array = []
        #print(self.__columns_indexes.values())
        for x in range(self.data[0].size):
            sub_array = []
            # name = str((list(self.__columns_indexes.keys())[x]))
            # print("hihi " + str(name))
            # sub_array.append(str(name))
            for y in range(len(self.__columns_indexes.values())):

                sub_array.append(self.data[y][x])
            final_array.append(sub_array)
        #print(final_array)

        return final_array

    def makeRowsColumns(self, access_as_rows):
        final_array = []
        #print(self.__columns_indexes.values())
        for x in range(len(access_as_rows[0])):
            sub_array = []
            # name = str((list(self.__columns_indexes.keys())[x]))
            # print("hihi " + str(name))
            # sub_array.append(str(name))
            for y in range(len(access_as_rows)):

                sub_array.append(access_as_rows[y][x])
            final_array.append(sub_array)
        #print(final_array)

        return final_array

    # def data_to_series_list(self, by, agg=None):
    #     real_list = []
    #     used_columns = []
    #
    #     for x in range(len(by)):
    #         if by[x] not in self.__columns_indexes.keys():
    #             print("error, column is not in list")
    #         elif self.__columns_indexes[by[x]] in used_columns:
    #             print("used")
    #             continue
    #         else:
    #             final_series_by = Series(data=self.__columns_indexes[by[x]].data, name=by[x])
    #             real_list.append(final_series_by)
    #             #print("This is the name " + str(name))
    #             used_columns.append(by[x])
    #     for column in agg:
    #         if column not in self.__columns_indexes.keys():
    #             print("error, column is not in list")
    #         else:
    #             new_list = []
    #             for x in range(len(self.__columns_indexes[column].data)):
    #                 print(agg[column])
    #                 #new_list.append(agg[column])
    #             #final_series_arr = Series(data=new_list, name=column)
    #             #real_list.append(final_series_arr)
    #     print(used_columns)
    #     return real_list

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
