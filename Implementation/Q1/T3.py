import time
import pandas as pd
from mpi4py import MPI

start = time.time()
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
dataset = 'datasets/Combined_Flights_2021.csv'


# find_max fn. iterates over the aggregated input and picks up the key corresponding to the highest integer value
def find_max(final_out: dict):
    most_cancelled = ""
    maximum = 0
    for k, val in final_out.items():
        if final_out.get(k) > maximum:
            most_cancelled = k
            maximum = val

    print(f'{most_cancelled} had the most canceled flights in September 2021')
    end = time.time()
    print(f'time taken with (MPI execution): {round(end - start, 2)} second(s)')


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

    out = {}
    for r in results:
        for key, value in r.to_dict().items():
            if key in out:
                out[key] = out[key] + value
            else:
                out[key] = value
    find_max(out)


# All workers perform processing on the given chunk of data and return the output to master
elif rank > 0:
    chunk_to_process = comm.recv()
    inp = pd.read_csv(dataset, nrows=chunk_to_process[0], skiprows=chunk_to_process[1], header=None)
    # In order to filter out date values using ".dt.month" and ".dt.year" changing datatype of FlightDate column to
    # datetime
    inp.isetitem(0, pd.to_datetime(inp.iloc[:, 0]))
    # filtering dataframe to fetch cancelled flights in Sep. 2021
    filtered_data = inp[(inp.iloc[:, 4] == True) & (inp.iloc[:, 0].dt.month == 9) & (inp.iloc[:, 0].dt.year == 2021)]
    # calculating number of flights cancelled per airline
    result = filtered_data.iloc[:, 1].value_counts()
    # sending processed result to master
    comm.send(result, dest=0)

