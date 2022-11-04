import time
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool

out_list = []


def map_tasks(reading_info: list, data: str = 'datasets/Combined_Flights_2021.csv'):
    inp = pd.read_csv(data, nrows=reading_info[0], skiprows=reading_info[1], header=None)
    # filtering dataframe to fetch airtime for flights that were flying from Nashville to Chicago
    filtered_data = inp[(inp.iloc[:, 34].str.match('Nashville')) & (inp.iloc[:, 42].str.match('Chicago'))]
    # calculating and returning average airtime per chunk for flights that were flying from Nashville to Chicago
    return filtered_data.iloc[:, 12].mean()


# Aggregator function takes mean value from each process stored in the list
# and calculates the overall mean.
def reduce_task(processes: int):
    avg = 0
    total = 0
    for out in tqdm(out_list):
        for item in out:
            total += float(item)
    avg = total / processes
    print(f'{avg} is the average airtime for flights that were flying from Nashville to Chicago')


def compute_multiprocessing():
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
    reduce_task(processes)
    p.close()
    p.join()

    print(
        f'time taken with {processes} processes (multiprocessing execution): {round(time.time() - start, 2)} second(s)')


if __name__ == '__main__':
    compute_multiprocessing()
