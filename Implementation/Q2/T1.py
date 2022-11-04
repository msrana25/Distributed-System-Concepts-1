import time
from threading import Thread
import pandas as pd


out_list_thread = []


def compute_using_threads(n_rows: int, skip: int, data: str = 'datasets/Combined_Flights_2021.csv'):
    inp = pd.read_csv(data, nrows=n_rows, skiprows=skip, header=None)
    inp.isetitem(0, pd.to_datetime(inp.iloc[:, 0]))
    # filtering dataframe to fetch diverted flights from 20th to 30th Nov. 2021 for each chunk
    filtered_data = inp[
        (inp.iloc[:, 5] == True) & (inp.iloc[:, 0].dt.month == 11) & (inp.iloc[:, 0].dt.year == 2021) & (
                inp.iloc[:, 0].dt.day >= 20)]
    # calculating number of flights diverted per airline
    out = filtered_data.iloc[:, 1].value_counts()
    # filtering out empty chunks from adding to prevent them adding to output list
    filter_2 = filtered_data.iloc[:, 1].value_counts().index
    if not str(filter_2) == str("Index([], dtype='object')"):
        # appending sum of all the diverted flights irrespective of the airline, to the output list for
        # each chunk
        out_list_thread.append(str(out.sum()))


# distributes data among threads and starts and joins them.
def thread_function(n_rows: int, no_of_threads):
    thread_handle = []
    skip_rows = 1
    thr = Thread(target=compute_using_threads, args=(n_rows - skip_rows, skip_rows))
    thread_handle.append(thr)
    skip_rows = n_rows

    for i in range(1, no_of_threads - 1):
        thr = Thread(target=compute_using_threads, args=(n_rows, skip_rows))
        thread_handle.append(thr)
        skip_rows = skip_rows + n_rows
    thr = Thread(target=compute_using_threads, args=(None, skip_rows))
    thread_handle.append(thr)
    for t in thread_handle:
        t.start()

    for j in range(0, no_of_threads):
        thread_handle[j].join()

    # calling the aggregator function
    reduce_task_thread()


# The aggregator function takes the list of sums for every chunk and further adds all values in the output list.
def reduce_task_thread():
    total = 0
    for item in out_list_thread:
        total += int(item)

    print(f'{total} flights were diverted between the period of 20th-30th November 2021')


if __name__ == '__main__':
    start_time = time.time()
    thread_function(630000, 10)
    end_time = time.time()
    print("Time taken using threads comes out to be : " + str(end_time - start_time))
