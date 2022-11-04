import time
from threading import Thread
import pandas as pd
from tqdm import tqdm

out_dict = {}


# compute_using_threads finds out count of all cancelled flights for Sep 2021 for each chunk of data
def compute_using_threads(n_rows: int, skip: int, data: str = 'datasets/Combined_Flights_2021.csv'):
    inp = pd.read_csv(data, nrows=n_rows, skiprows=skip, header=None)
    # In order to filter out date values using ".dt.month" and ".dt.year" changing datatype of FlightDate column to
    # datetime
    inp.isetitem(0, pd.to_datetime(inp.iloc[:, 0]))
    # filtering dataframe to fetch cancelled flights in Sep. 2021 for each chunk
    filtered_data = inp[(inp.iloc[:, 4] == True) & (inp.iloc[:, 0].dt.month == 9) & (inp.iloc[:, 0].dt.year == 2021)]
    # calculating number of flights cancelled per airline
    out = filtered_data.iloc[:, 1].value_counts()
    # filtering out empty chunks from adding to intermediate output dictionary.
    filter_2 = filtered_data.iloc[:, 1].value_counts().index
    if not str(filter_2) == str("Index([], dtype='object')"):
        # adding actual and valid data values to intermediate output dictionary.
        for key, value in out.to_dict().items():
            if key in out_dict:
                out_dict[key] = out_dict.get(key) + value
            else:
                out_dict[key] = value


# thread_function distributes data among threads
def thread_function(n_rows: int, no_of_threads):
    thread_handle = []
    skip_rows = 1
    # Assigning first chunk of data to first thread
    thr = Thread(target=compute_using_threads, args=(n_rows - skip_rows, skip_rows))
    thread_handle.append(thr)
    skip_rows = n_rows

    # Assigning chunks from second thread to second last thread
    for i in range(1, no_of_threads - 1):
        thr = Thread(target=compute_using_threads, args=(n_rows, skip_rows))
        thread_handle.append(thr)
        skip_rows = skip_rows + n_rows

    # Assigning whatever chunk of data is remaining to the last thread by specifying n_rows = None
    thr = Thread(target=compute_using_threads, args=(None, skip_rows))
    thread_handle.append(thr)
    # Starting and joining all the threads
    for t in thread_handle:
        t.start()

    for j in range(0, no_of_threads):
        thread_handle[j].join()
    find_max(out_dict)


# aggregating the data received from multiple threads and storing flight name as key and total no of cancelled flights
# as the values
def reduce_task(mapping_output: list):
    reduce_out = {}
    for out in tqdm(mapping_output):
        for key, value in out.to_dict().items():
            if key in reduce_out:
                reduce_out[key] = reduce_out.get(key) + value
            else:
                reduce_out[key] = value
    # passing output to find_max function
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


if __name__ == '__main__':
    start_time = time.time()
    n_threads = 10
    thread_function(630000, n_threads)
    end_time = time.time()
    print("Time taken using {n_threads} threads comes out to be : " + str(end_time - start_time))
