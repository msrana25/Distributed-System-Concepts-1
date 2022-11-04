import time
import pandas as pd
from mpi4py import MPI

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
start = time.time()
dataset = 'datasets/Combined_Flights_2021.csv'

if rank == 0:
    start = time.time()
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
    # distributing data among 4 slaves
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

    # Take mean value from each slave stored in list and calculate the overall mean.
    avg = 0
    total = 0

    for item in results:
        total += float(item)

    avg = total / (size-1)
    print(f'{avg} is the average airtime for flights that were flying from Nashville to Chicago')
    end = time.time()
    print(f'time taken with (MPI execution): {round(end - start, 2)} second(s)')


elif rank > 0:
    chunk_to_process = comm.recv()
    inp = pd.read_csv(dataset, nrows=chunk_to_process[0], skiprows=chunk_to_process[1], header=None)
    # filtering dataframe to fetch airtime for flights that were flying from Nashville to Chicago
    filtered_data = inp[(inp.iloc[:, 34].str.match('Nashville')) & (inp.iloc[:, 42].str.match('Chicago'))]
    # calculating and returning average airtime per slave for flights that were flying from Nashville to Chicago
    result = filtered_data.iloc[:, 12].mean()
    comm.send(result, dest=0)
