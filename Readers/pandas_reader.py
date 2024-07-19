import pandas as pd
from multiprocessing import Pool, cpu_count
from tqdm import tqdm


class PandasReader:

    def __init__(self, chunk_size:int, total_linhas_conhecidas:int, file_path:str) -> None:
        self._chunk_size = chunk_size
        self._total_linhas_conhecidas = total_linhas_conhecidas
        self._file_path = file_path
        self.CONCURRENCY = cpu_count()

    def process_chunk(self, chunk:pd.DataFrame) -> pd.DataFrame:
        #aggregated = chunk.groupby('Card_Provider')['Card_Number'].count().reset_index()
        aggregated = chunk.groupby('Card_Provider')['Card_Number'].count().reset_index()
        return aggregated
    
    def create_df_with_pandas(self) -> pd.DataFrame:
        total_chunks = self._total_linhas_conhecidas // self._chunk_size + (1 if self._total_linhas_conhecidas % self._chunk_size else 0)
        results = []
        total_records_processed:int = 0
        

        with pd.read_csv(self._file_path, chunksize=self._chunk_size) as reader:
            with Pool(self.CONCURRENCY) as pool:
                for chunk in tqdm(reader, total=total_chunks, desc="Processando Chunks"):
                    total_records_processed += len(chunk)
                    result = pool.apply_async(self.process_chunk, (chunk, ))
                    results.append(result)
                results = [result.get() for result in results]
        print(f"Total de chunks {total_chunks}")
        print(f"Total de registros processados: {total_records_processed}")
        print(f"Num records por result: {len(results[0])}")
        final_df = pd.concat(results, ignore_index=True)
        #final_aggregate = final_df.groupby('Card_Provider')['Card_Number'].count().reset_index()
        final_aggregate = final_df.groupby('Card_Provider')['Card_Number'].sum()
        return final_aggregate

    
if __name__ == "__main__":
    import time
    print("Iniciando Processamento")
    start_time = time.time()
    #path = r"C:\Users\julio\OneDrive\Área de Trabalho\Projects\One-Billion-Row-Pratica\One-Billion-Row-Practice\Data\data.csv"
    path ="./Data/data.csv"
    pdreader = PandasReader(100000, 2000000, path)
    df = pdreader.create_df_with_pandas()
    total_time = time.time() - start_time
    print(f"Tempo de Processamento: {total_time}")
    print(df.head())
    #print(df.info()

