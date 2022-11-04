import time
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool
import multiprocessing


def map_tasks(reading_info: list, data: str = 'datasets/Combined_Flights_2021.csv'):
    inp = pd.read_csv(data, nrows=reading_info[0], skiprows=reading_info[1], header=None)
    # filtering dataframe to fetch all values where departure time (DepTime) was not recorded/went missing
    filtered_data = inp[(inp.iloc[:, 7].isna() == True)]
    # fetching and returning the dates and their counts, corresponding to missing data
    return filtered_data.iloc[:, 0].value_counts()


# aggregating the data from different processes and displaying the result
def reduce_task(mapping_output: list):
    reduce = {}
    for out in tqdm(mapping_output):
        for key, value in out.to_dict().items():
            if key not in reduce:
                reduce[key] = value
    final_result = list(reduce.keys())
    final_result.sort()
    print(f'Departure time (DepTime) was not recorded/went missing for dates {final_result}')


def compute_multiprocessing():
    # distributing the data
    def distribute_rows(n_rows: int, n_processes):
        reading_info = []
        skip_rows = 1
        reading_info.append([n_rows - skip_rows, skip_rows])
        skip_rows = n_rows

        for _ in range(1, n_processes - 1):
            reading_info.append([n_rows, skip_rows])
            skip_rows = skip_rows + n_rows
        reading_info.append([None, skip_rows])
        return reading_info

    # processes = multiprocessing.cpu_count()
    processes = 4
    p = Pool(processes=processes)
    start = time.time()
    result = p.map(map_tasks, distribute_rows(n_rows=1600000, n_processes=processes))
    reduce_task(result)
    p.close()
    p.join()

    print(
        f'time taken with {processes} processes (multiprocessing execution): {round(time.time() - start, 2)} second(s)')


if __name__ == '__main__':
    compute_multiprocessing()
