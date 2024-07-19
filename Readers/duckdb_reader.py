import duckdb
import pandas as pd

class DuckdbReader:

    def __init__(self) -> None:
        pass

    def read_data(self) -> pd.DataFrame:
        df = duckdb.sql("""
                            SELECT Card_Provider, COUNT(*) AS Card_Count
                            FROM read_csv("./Data/data.csv", sep=',', columns={'Card_Number'  : VARCHAR,
                                                                               'Expiry_Date'  : VARCHAR,
                                                                               'Card_Provider': VARCHAR,
                                                                               'Security_Code': VARCHAR})
                            GROUP BY Card_Provider
        """).df()
        return df
    
if __name__ == "__main__":
    import time
    duckreader = DuckdbReader()
    print("Iniciando Leitura")
    start_time = time.time()
    df = duckreader.read_data()
    print(df.head())
    took = time.time() - start_time
    print(f"Leitura concluida em {took}")
