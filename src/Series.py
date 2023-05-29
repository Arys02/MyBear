import numbers

class Series:
    def __init__(self, data, name, dtype=None, clone=False):
        self.data = data.copy() if clone else data
        self.name = name
        self.size = len(data)
        self.missing_values = self.data.count

        self.dtype = type(self.data[0]) \
            if self.size > 0 and dtype is None \
            else None

    @property
    def iloc(self):
        return self.data

    def __getitem__(self, item):
        if isinstance(item, slice):
            return Series(self.data[item], self.name)
        return self.data[item]

    def max(self):
        return max(self.data)

    def min(self):
        return min(self.data)

    def mean(self):
        if self.dtype is not None and isinstance(self.data[0], numbers.Number):
            return sum(self.data) / len(self.data) if len(self.data) > 0 else None

    def std(self):
        if self.dtype is not None and isinstance(self.data[0], numbers.Number):
            mean = self.mean()
            return (sum((x - mean) ** 2 for x in self.data) / len(self.data)) ** 0.5 if len(self.data) > 0 else None

    def count(self):
        return len(self.data)

    def __str__(self):
        return "Series \nName : {} \nSize : {}\nDtype: {}\nDATA : {}".format(self.name,
                                                                             self.size,
                                                                             self.dtype,
                                                                             self.data)