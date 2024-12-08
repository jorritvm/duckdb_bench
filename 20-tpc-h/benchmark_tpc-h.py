""" performing the full benchmark """


import csv
import os
import time
import datetime
import duckdb


# List of the databases we will test on
# dbs = ["tpch-sf1", 
#         "tpch-sf3", 
#         "tpch-sf10", 
#         "tpch-sf30", 
#         "tpch-sf100"]
dbs = ["tpch-sf100"]

# List of the queries we will test for
queries = [f"PRAGMA tpch({i});" for i in range(10,23)]

SUBFOLDER_DATA = "data"
SUBFOLDER_LOGS = "results"

def get_timestamp():
    # Get current datetime in ISO 8601 format
    now = datetime.datetime.now()
    filename_safe_iso8601 = now.strftime("%Y-%m-%dT%H-%M-%S")
    return filename_safe_iso8601

def start_logfile(): 
    file_name = get_timestamp() + "_results.csv"
    file_path = os.path.join(SUBFOLDER_LOGS, file_name)
    with open(file_path, 'w') as f:
        f.write("timestamp,db,query,t_first,t_second\n")
    return file_path
    
def run_query_on_db(conn, db, query):
    print(f"Executing query: {query}")
    ts = get_timestamp()

    t_0 = time.time()

    result = conn.execute(query).fetchall()
    t_1 = time.time()

    result = conn.execute(query).fetchall()
    t_2 = time.time()

    t_first =  t_1 - t_0
    t_second =  t_2 - t_1
    
    return {'timestamp': ts, 
            'db': db,
            'query': query,
            't_first': t_first,
            't_second': t_second
            }

def log_result(logfile, measurement):
    with open(logfile, 'a', newline='') as f:
        writer = csv.writer(f)  # Use csv.writer to handle CSV format
        writer.writerow([measurement['timestamp'], 
                         measurement['db'], 
                         measurement['query'], 
                         measurement['t_first'], 
                         measurement['t_second']])

def main():
    logfile = start_logfile()
    
    for db in dbs:
        # start new db connection per database we are testing
        conn = duckdb.connect(f"{SUBFOLDER_DATA}/{db}.db")

        # run all queries
        for query in queries:
            measurement = run_query_on_db(conn, db, query)
            log_result(logfile, measurement)

        # Close the connection
        conn.close()


if __name__ == "__main__":
    main()