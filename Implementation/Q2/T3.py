import time
import pandas as pd
from mpi4py import MPI

start = time.time()
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
start = time.time()
dataset = 'datasets/Combined_Flights_2021.csv'

if rank == 0:
    """
    Master worker (with rank 0) is responsible for distributes the workload evenly 
    between slave workers.
    """


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


    slave_workers = size - 1
    # # distributing data among 4 slaves
    chunk_distribution = distribute_rows(n_rows=1600000, n_processes=slave_workers)

    # distribute tasks to slaves
    for worker in range(1, size):
        chunk_to_process = worker - 1
        comm.send(chunk_distribution[chunk_to_process], dest=worker)

    # receive and aggregate results from slave
    results = []
    for worker in (range(1, size)):  # receive
        result = comm.recv(source=worker)
        results.append(result)

    # sums all values in the list to give out final result
    total = 0
    for item in results:
        total += int(item)

    print(f'{total} flights were diverted between the period of 20th-30th November 2021')
    end = time.time()
    print(f'time taken with (MPI execution): {round(end - start, 2)} second(s)')


elif rank > 0:
    chunk_to_process = comm.recv()
    inp = pd.read_csv(dataset, nrows=chunk_to_process[0], skiprows=chunk_to_process[1], header=None)
    inp.isetitem(0, pd.to_datetime(inp.iloc[:, 0]))
    # filtering dataframe to fetch diverted flights from 20th to 30th Nov. 2021 for each slave
    filtered_data = inp[(inp.iloc[:, 5] == True) & (inp.iloc[:, 0].dt.month == 11) & (inp.iloc[:, 0].dt.year == 2021) &
                        (inp.iloc[:, 0].dt.day >= 20)]
    filtered_data.iloc[:, 1].value_counts().sum()
    # calculating and returning sum of all flights diverted per data chunk assigned to slave
    result = filtered_data.iloc[:, 1].value_counts().sum()
    comm.send(result, dest=0)
