import unittest

from src.Series import Series


class MyTestCase(unittest.TestCase):
    def test_constructor(self):
        a = Series([1, 2, 3], "test_name")
        self.assertEqual(a.data[0], 1)
        self.assertEqual(a.data[1], 2)
        self.assertEqual(a.data[2], 3)
        self.assertEqual(a.name, "test_name")
        self.assertEqual(a.dtype, int)
        list = [1, 2, 3, 4]
        b = Series(list, "test")
        b.data[0] = 0

        self.assertEqual(b.data[0], 0)
        self.assertEqual(list[0], 0)

        c = Series(list, "test", clone=True)
        c.data[0] = 10

        self.assertEqual(c.data[0], 10)
        self.assertEqual(list[0], 0)



    def test_iloc(self):
        list = [1, 2, 3, 4, 5, 6]
        a = Series(list, "test", clone=True)

        self.assertEqual(a.iloc[2], 3)
        self.assertEqual(isinstance(a.iloc[1:4], Series), True)

    def test_max(self):
        a = Series([1, 2, 3], "test_name")
        self.assertEqual(a.max(), 3)

    def test_min(self):
        a = Series([1, 2, 3], "test_name")
        self.assertEqual(a.min(), 1)

    def test_mean(self):
        a = Series([1, 2, 3], "test_name")
        self.assertEqual(a.mean(), 2)

        b = Series(["1", "2", "3"], "test_name")
        self.assertEqual(b.mean(), None)

    def test_std(self):
        a = Series([10, 8, 10, 8, 8, 4], "test_name")
        self.assertEqual(abs(a.std() - 2.19) < 0.1, True)
        b = Series(["1", "2", "3"], "test_name")
        self.assertEqual(b.std(), None)


    def test_count(self):
        a = Series([10, 8, 10, 8, 8, 4], "test_name")

        self.assertEqual(a.count(), 6)



if __name__ == '__main__':
    unittest.main()
