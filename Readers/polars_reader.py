import polars as pl

class PolarsReader:

    def __init__(self, path:str) -> None:
        self._path = path

    def read_csv(self) -> pl.LazyFrame:
        pl.Config.set_streaming_chunk_size(500000)
        return(
            pl.scan_csv(self._path, new_columns=['Card_Number','Expiry_Date','Card_Provider','Security_Code'], 
                    schema={'Card_number'   :pl.Int16,
                            'Expiry_Date'   :pl.String, 
                            'Card_Provider' :pl.String,
                            'Security_Code' :pl.Int8 }).group_by('Card_Provider').count().collect(streaming=True)
        )
    
if __name__ == '__main__':
    import time
    start_time = time.time()
    path = "./Data/data.csv"
    preader = PolarsReader(path)
    lazy_df = preader.read_csv()
    took = time.time() - start_time
    print(f'Processamento terminado em {took}')
    print(lazy_df.head())
    