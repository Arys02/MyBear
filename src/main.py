from Series import Series


def main():
    a = [1.0, 2.0, 3.0]
    b = ["a", "b", "c"]
    a_s = Series(a, "name")
    b_s = Series(b, "name")

    print(a_s)
    print(b_s)

    print(type(a_s.iloc[2:1]))
    print_series(a_s)
    print_series(b_s)

def print_series(serie: Series):
    print(serie.max())
    print(serie.min())
    print(serie.mean())
    print(serie.std())
    print(serie.count())



if __name__ == '__main__':
    main()
