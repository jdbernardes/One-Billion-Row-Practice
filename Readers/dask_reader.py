import dask
import dask.dataframe as dd


class DaskReader:

    def __init__(self, path: str) -> None:
        self._path = path

    def read_data(self) -> dd.DataFrame:
        dask.config.set({'dataframe.query-plannin': True})
        df = dd.read_csv(self._path)
        grouped_df = df.groupby('Card_Provider')['Card_Number'].count()
        return grouped_df


if __name__ == '__main__':
    import time
    path = "./Data/data.csv"
    dreader = DaskReader(path)
    start_time = time.time()
    df = dreader.read_data()
    result_df = df.compute().reset_index(name='Total_Cards_Emited')
    took = time.time() - start_time
    print(f'Processamento finalizado em {took}.')
    print(result_df.head())
