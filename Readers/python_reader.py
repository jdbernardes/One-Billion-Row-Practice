from csv import reader
from tqdm import tqdm


class PythonReader:

    def __init__(self, path: str, total_records: int) -> None:
        self._path = path
        self._total_records = total_records

    def read_csv(self) -> dict:
        agg_dict = {}
        sorted_dict = {}
        with open(self._path, 'r') as file:
            _reader = reader(file, delimiter=',')
            next(_reader, None)  # skipping the header
            for row in tqdm(_reader, total=self._total_records,
                            desc="Processando"):
                if row[2] in agg_dict:
                    agg_dict[row[2]] += 1
                else:
                    agg_dict[row[2]] = 1
            sorted_dict = dict(sorted(agg_dict.items()))
        print("Processamento Finalizado")
        return sorted_dict


if __name__ == '__main__':
    import time
    path = "./Data/data.csv"
    total_records = 2000000
    preader = PythonReader(path, total_records)
    start_time = time.time()
    result = preader.read_csv()
    took = time.time() - start_time
    print(f'processamento finalizado em {took}')
    print(result)
