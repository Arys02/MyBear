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
    def __init__(self, data, name, dtype=None, clone=False):
        """
                Initialise la Série avec des données, un nom et éventuellement un type de données.

                Si clone est True, crée une copie des données. Sinon, utilise les données directement.
                """
        self.data = data.copy() if clone else data
        self.name = name
        self.size = len(data)
        #TODO nombre de NaN dans la serie
        self.missing_values = self.data.count

        self.dtype = type(self.data[0]) \
            if self.size > 0 and dtype is None \
            else None

    @property
    def iloc(self):
        """
        Permet l'indexation basée sur la position des éléments dans la Série.
        """
        return self

    def copy(self):
        """
        Permet l'indexation basée sur la position des éléments dans la Série.
        """
        return Series(self.data, self.name, clone=True)

    def __getitem__(self, item):
        """
            Permet l'accès aux éléments de la Série par indexation.
        """
        if isinstance(item, slice):
            return Series(self.data[item], self.name)
        return self.data[item]

    @property
    def max(self):
        return max(self.data)

    @property
    def min(self):
        return min(self.data)

    @property
    def mean(self):
        if self.dtype is not None and isinstance(self.data[0], numbers.Number):
            return sum(self.data) / len(self.data)

    @property
    def std(self):
        if self.dtype is not None and isinstance(self.data[0], numbers.Number):
            mean = self.mean()
            return sqrt(sum((x - mean) ** 2 for x in self.data) / (len(self.data) - 1))

    def count(self):
        return len(self.data)

    def __str__(self):
        return "Series \nName : {} \nSize : {}\nDtype: {}\nDATA : {}".format(self.name,
                                                                             self.size,
                                                                             self.dtype,
                                                                             self.data)