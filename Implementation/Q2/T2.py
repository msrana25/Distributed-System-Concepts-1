import time
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool

out_list = []


def map_tasks(reading_info: list, data: str = 'datasets/Combined_Flights_2021.csv'):
    inp = pd.read_csv(data, nrows=reading_info[0], skiprows=reading_info[1], header=None)
    inp.isetitem(0, pd.to_datetime(inp.iloc[:, 0]))
    # filtering dataframe to fetch diverted flights from 20th to 30th Nov. 2021 for each chunk
    filtered_data = inp[(inp.iloc[:, 5] == True) & (inp.iloc[:, 0].dt.month == 11) & (inp.iloc[:, 0].dt.year == 2021) &
                        (inp.iloc[:, 0].dt.day >= 20)]
    # calculating and returning sum of all flights diverted per chunk
    filtered_data.iloc[:, 1].value_counts().sum()
    return filtered_data.iloc[:, 1].value_counts().sum()


# The aggregator function takes the list of sums for every chunk and further adds all values in the output list.
def reduce_task():
    total = 0
    for out in tqdm(out_list):
        for item in out:
            total += int(item)

    print(f'{total} flights were diverted between the period of 20th-30th November 2021')


def compute_multiprocessing():
    # distributing data among processes
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
    # adding results from each process into a list
    out_list.append(result)
    # sending data for aggregation
    reduce_task()
    p.close()
    p.join()
    print(
        f'time taken with {processes} processes (multiprocessing execution): {round(time.time() - start, 2)} second(s)')


if __name__ == '__main__':
    compute_multiprocessing()
