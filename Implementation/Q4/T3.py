import time
import pandas as pd
from mpi4py import MPI

start = time.time()
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
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

    reduce = {}
    for out in results:
        for key, value in out.to_dict().items():
            if key not in reduce:
                reduce[key] = value
    final_result = list(reduce.keys())
    final_result.sort()
    print(f'Departure time (DepTime) was not recorded/went missing for dates {final_result}')
    end = time.time()
    print(f'time taken with (MPI execution): {round(end - start, 2)} second(s)')


elif rank > 0:
    chunk_to_process = comm.recv()
    inp = pd.read_csv(dataset, nrows=chunk_to_process[0], skiprows=chunk_to_process[1], header=None)
    # filtering dataframe to fetch all values where departure time (DepTime) was not recorded/went missing
    filtered_data = inp[(inp.iloc[:, 7].isna() == True)]
    # fetching the dates corresponding to missing data and sending the result back to master.
    result = filtered_data.iloc[:, 0].value_counts()
    comm.send(result, dest=0)
