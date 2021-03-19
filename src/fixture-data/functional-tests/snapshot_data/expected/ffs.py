def fml():
    with open("statement_fact_v_expected.csv") as f:
        y = f.read()
        print (y.replace("\t", ",").strip())

if __name__ == "__main__":
    fml()