import numbers
from math import sqrt


class Series:
    """
       Une classe représentant une Série, similaire à une Série pandas.

       Attributes
       ----------
       data : list
           La liste de données de la Série.
       name : str
           Le nom de la Série.
       size : int
           La taille de la Série.
       missing_values : int
           Le nombre de valeurs manquantes dans la Série.
       dtype : type, optional
           Le type de données de la Série.
       """

    def __init__(self, data, name, dtype=None, clone=False, capacity=None):
        """
                Initialise la Série avec des données, un nom et éventuellement un type de données.

                Si clone est True, crée une copie des données. Sinon, utilise les données directement.
                """
        self.data = data.copy() if clone else data
        if capacity is not None and capacity > len(data):
            for i in range(len(data), capacity):
                self.data.append(None)
        self.name = name

        self.size = len(data)
        self.missing_values = self.data.count(None)

        self.dtype = type(self.data[0]) \
            if self.size > 0 and dtype is None \
            else None

        self._existing_data = [x for x in self.data if x is not None]
        self._max = max(self._existing_data)
        self._min = min(self._existing_data)
        self._mean = sum(self._existing_data) / len(self._existing_data) \
            if self.dtype is not None and isinstance(self._existing_data[0], numbers.Number) \
            else None
        self._std = sqrt(sum((x - self._mean) ** 2 for x in self._existing_data) / (len(self._existing_data))) \
            if self.dtype is not None and isinstance(self._existing_data[0], numbers.Number) \
            else None
        self._count = len(self.data) - self.data.count(0) - self.data.count(None)

    @property
    def iloc(self):
        """
        Permet l'indexation basée sur la position des éléments dans la Série.
        """
        return self

    def copy(self):
        """
        Permet le renvois d'une copie de la Série.
        """
        return Series(self.data, self.name, clone=True)

    def resize(self, capacity):
        if capacity is not None and capacity > len(self.data):
            for i in range(len(self.data), capacity):
                self.data.append(None)

            self.size = len(self.data)
            self.missing_values = self.data.count(None)

        elif capacity is not None and capacity < len(self.data):
            return self[0:capacity]

        return self.copy()





    def __getitem__(self, item):
        """
            Permet l'accès aux éléments de la Série par indexation.
        """
        if isinstance(item, slice):
            return Series(self.data[item], self.name)
        return self.data[item]

    @property
    def max(self):
        return self._max

    @property
    def min(self):
        return self._min

    @property
    def mean(self):
        return self._mean

    @property
    def std(self):
        return self._std

    def count(self):
        return self._count

    def __str__(self):
        return "Series \nName : {} Size : {} Dtype: {} \nDATA : {}".format(self.name,
                                                                           self.size,
                                                                           self.dtype,
                                                                           self.data)
