import time
import multiprocessing
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool

out_dict = {}


# map_tasks finds out count of all cancelled flights for Sep 2021 for each chunk of data
def map_tasks(reading_info: list, data: str = 'datasets/Combined_Flights_2021.csv'):
    inp = pd.read_csv(data, nrows=reading_info[0], skiprows=reading_info[1], header=None)
    # In order to filter out date values using ".dt.month" and ".dt.year" changing datatype of FlightDate column to
    # datetime
    inp.isetitem(0, pd.to_datetime(inp.iloc[:, 0]))
    # filtering dataframe to fetch cancelled flights in Sep. 2021 for each chunk
    filtered_data = inp[(inp.iloc[:, 4] == True) & (inp.iloc[:, 0].dt.month == 9) & (inp.iloc[:, 0].dt.year == 2021)]
    # returning number of flights cancelled per airline
    return filtered_data.iloc[:, 1].value_counts()


# aggregating the data received from multiple processes and storing flight name as key and total no of cancelled flights
# as the values
def reduce_task(mapping_output: list):
    reduce_out = {}
    for out in tqdm(mapping_output):
        for key, value in out.to_dict().items():
            if key in reduce_out:
                reduce_out[key] = reduce_out.get(key) + value
            else:
                reduce_out[key] = value
    find_max(reduce_out)


# find_max fn. iterates over the aggregated input and picks up the key corresponding to the highest integer value
def find_max(final_out: dict):
    most_cancelled = ""
    maximum = 0
    for key, value in final_out.items():
        if final_out.get(key) > maximum:
            most_cancelled = key
            maximum = value
    print(f'{most_cancelled} had the most canceled flights in September 2021')


def compute_multiprocessing():
    # distribute_rows distributes data among processes
    def distribute_rows(n_rows: int, n_processes):
        reading_info = []
        skip_rows = 1
        # Assigning first chunk of data to first process
        reading_info.append([n_rows - skip_rows, skip_rows])
        skip_rows = n_rows

        # Assigning chunks from second process to second last process
        for _ in range(1, n_processes - 1):
            reading_info.append([n_rows, skip_rows])
            skip_rows = skip_rows + n_rows

        # Assigning whatever chunk of data is remaining to the last process by specifying n_rows = None
        reading_info.append([None, skip_rows])
        return reading_info

    # processes = multiprocessing.cpu_count()
    processes = 4
    p = Pool(processes=processes)
    start = time.time()
    result = p.map(map_tasks, distribute_rows(n_rows=1600000, n_processes=processes))
    # sending data for aggregation
    reduce_task(result)
    p.close()
    p.join()
    print(
        f'time taken with {processes} processes (multiprocessing execution): {round(time.time() - start, 2)} second(s)')


if __name__ == '__main__':
    compute_multiprocessing()
