import time
import csv
# from data_generator.data_generator import DataGenerator
from readers.python_reader import PythonReader
from readers.pandas_reader import PandasReader
from readers.dask_reader import DaskReader
from readers.duckdb_reader import DuckdbReader
from readers.polars_reader import PolarsReader
from reporting.save_result import SaveResults


# Mock Data Creator
# test = DataGenerator()
# credit_cards = test.create_csv(2000000)

# Global Variables
sres = SaveResults()
path = "./Data/data.csv"
total_records = 2000000

if __name__ == '__main__':

    field_names = ['Library', 'Total_Records', 'Operation_Type', 'Time_Taken']
    with open('./Data/result.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        writer.writeheader()

    # Python Reader
    preader = PythonReader(path, total_records)
    start_time = time.time()
    result = preader.read_csv()
    took_python = time.time() - start_time
    sres.save_results('Python', total_records, 'Count', round(took_python, 2))
    print(f'processamento finalizado em {took_python}')
    print(result)

    # Pandas Reader
    print("Iniciando Processamento")
    start_time_pandas = time.time()
    pdreader = PandasReader(100000, total_records, path)
    df = pdreader.create_df_with_pandas()
    took_pandas = time.time() - start_time_pandas
    sres.save_results('Pandas', total_records, 'Count', round(took_pandas, 2))

    # DaskReader
    dreader = DaskReader(path)
    start_time_dask = time.time()
    df = dreader.read_data()
    result_df = df.compute().reset_index(name='Total_Cards_Emited')
    took_dask = time.time() - start_time_dask
    sres.save_results('Dask', total_records, 'Count', round(took_dask, 2))
    print(f'Processamento finalizado em {took_dask}.')
    print(result_df.head())

    # PolarsReader
    start_time_polars = time.time()
    preader = PolarsReader(path)
    lazy_df = preader.read_csv()
    took_polars = time.time() - start_time_polars
    sres.save_results('Polars', total_records, 'Count', round(took_polars, 2))
    print(f'Processamento terminado em {took_polars}')
    print(lazy_df.head())

    # DuckDBReader
    duckreader = DuckdbReader()
    print("Iniciando Leitura")
    start_time_duck = time.time()
    df = duckreader.read_data()
    print(df.head())
    took_duck = time.time() - start_time_duck
    sres.save_results('Duck DB', total_records, 'Count', round(took_duck, 2))
    print(f"Leitura concluida em {took_duck}")
