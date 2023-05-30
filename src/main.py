from Series import Series
from src.DataFrame import DataFrame


def main():
    a = [1.0, 2.0, 3.0]
    b = ["a", "b", "c"]
    a_s = Series(a, "name")
    b_s = Series(b, "name")

    #print(a_s)
    #print(b_s)



    c_s_a = Series([1, 2, 3, 4], "a")
    c_s_b = Series([3, 4, 6, 1], "b")
    c_s_c = Series([30, 14, 26, 31], "c")
    df_b = DataFrame([c_s_a, c_s_b, c_s_c], clone=True);
    df_a = DataFrame(column=["a", "b", "c"], data=[[1, 2, 3, 4],
                                                   [5, 6, 7, 8],
                                                   [9, 10, 11, 20]])
    #print(df_b)
    print(df_a)
    x = df_a.iloc[0, 2]
    x2 = df_a.iloc[1:3, 2]
    x3 = df_a.iloc[2, 0:3]
    x4 = df_a.iloc[0:2, 0:3]

    print("x : \n"+ str(x))
    print("x2 : \n" + str(x2))
    print("x3 :\n" + str(x3))
    print("x4 :\n" + str(x4))


    #print(type(a_s.iloc[2:1]))
    print_series(a_s)
    #print_series(b_s)




def print_series(serie: Series):
    print(serie.max())
    print(serie.min())
    print(serie.mean())
    print(serie.std())
    print(serie.count())



if __name__ == '__main__':
    main()
