import time
from threading import Thread
import pandas as pd

out_list_thread = []


def compute_using_threads(n_rows: int, skip: int, data: str = 'datasets/Combined_Flights_2021.csv'):
    inp = pd.read_csv(data, nrows=n_rows, skiprows=skip, header=None)
    # filtering dataframe to fetch airtime for flights that were flying from Nashville to Chicago
    filtered_data = inp[(inp.iloc[:, 34].str.match('Nashville')) & (inp.iloc[:, 42].str.match('Chicago'))]
    out = filtered_data.iloc[:, 12]
    filter_2 = filtered_data.iloc[:, 1].value_counts().index
    if not str(filter_2) == str("Index([], dtype='object')"):
        # calculating average airtime per chunk for flights that were flying from Nashville to Chicago adds to
        # intermediate output list
        out_list_thread.append(str(out.mean()))


# distributing data among threads and starting and joining them
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

    reduce_task_thread(no_of_threads)


# Aggregator function takes mean value from each thread stored in list and calculates the overall mean.
def reduce_task_thread(no_of_threads: int):
    total = 0
    avg = 0
    for item in out_list_thread:
        total += float(item)
    avg = total / no_of_threads
    print(f'{avg} is the average airtime for flights that were flying from Nashville to Chicago')


if __name__ == '__main__':
    start_time = time.time()
    thread_function(630000, 10)
    end_time = time.time()
    print("Time taken using threads comes out to be : " + str(end_time - start_time))
