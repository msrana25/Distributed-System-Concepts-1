import time
from threading import Thread
import pandas as pd

out_list_thread = []


def compute_using_threads(n_rows: int, skip: int, data: str = 'datasets/Combined_Flights_2021.csv'):
    inp = pd.read_csv(data, nrows=n_rows, skiprows=skip, header=None)
    # filtering dataframe to fetch all values where departure time (DepTime) was not recorded/went missing
    filtered_data = inp[(inp.iloc[:, 7].isna() == True)]
    # fetching the dates corresponding to missing data
    out = filtered_data.iloc[:, 0]
    filter_2 = filtered_data.iloc[:, 1].value_counts().index
    if not str(filter_2) == str("Index([], dtype='object')"):
        # performing value count of all the dates extracted and appending to a list
        out_list_thread.append(out.value_counts())


# distribute data among threads
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

    reduce_task_thread(out_list_thread)


# aggregating the data from different threads and displaying the result
def reduce_task_thread(mapping_output: list):
    reduce_thread = {}
    for out in mapping_output:
        for key, value in out.to_dict().items():
            if key not in reduce_thread:
                reduce_thread[key] = value

    final_result = list(reduce_thread.keys())
    final_result.sort()
    print(f'Departure time (DepTime) was not recorded/went missing for dates {final_result}')


if __name__ == '__main__':
    start_time = time.time()
    thread_function(630000, 10)
    end_time = time.time()
    print("Time taken using threads comes out to be : " + str(end_time - start_time))
