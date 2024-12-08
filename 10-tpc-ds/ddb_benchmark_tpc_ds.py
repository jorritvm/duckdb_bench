import duckdb
import os
import datetime
import time
import csv


# specify the input path
scale_factor = 100
db_file_name = f"duckdb_sf{scale_factor}.db"

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
        f.write("timestamp,db,sf,query,t_first,t_second\n")
    return file_path


def get_queries(conn):
    try:
        results = conn.execute("CALL tpcds_queries()").fetchall()       
        return results
    except Exception as e:
        print(f"An error occurred: {e}")


def run_query_on_db(conn, db_file_name, query):
    query_id = query[0]
    query_body = query[1]
    print(f"Executing query: {query_id}")

    try:
        # Connect to DuckDB and create a persistent database
        conn = duckdb.connect(db_file_path)
        
        # Execute all queries and record execution times
        ts = get_timestamp()

        t_0 = time.time()

        result = conn.execute(query_body).fetchall()
        t_1 = time.time()

        result = conn.execute(query_body).fetchall()
        t_2 = time.time()

        t_first =  t_1 - t_0
        t_second =  t_2 - t_1

        return {'timestamp': ts, 
                'db': db_file_name,
                'sf': scale_factor,
                'query': query_id,
                't_first': t_first,
                't_second': t_second
                }
    except Exception as e:
        print(f"An error occurred: {e}")


def log_result(logfile, measurement):
    with open(logfile, 'a', newline='') as f:
        writer = csv.writer(f)  # Use csv.writer to handle CSV format
        writer.writerow([measurement['timestamp'], 
                         measurement['db'],
                         measurement['sf'],
                         measurement['query'], 
                         measurement['t_first'], 
                         measurement['t_second']])

if __name__ == "__main__":
    logfile = start_logfile()
    
    # start new db connection per database we are testing
    db_file_path = os.path.join(SUBFOLDER_DATA, db_file_name)
    print(db_file_path)
    conn = duckdb.connect(db_file_path)

    # run all queries
    print("Fetchingq queries...")
    queries = get_queries(conn)

    # perform benchmark
    print("Starting benchmark...")
    total_start_time = time.time()
    for query in queries: # tuples
        measurement = run_query_on_db(conn, db_file_name, query)
        print(measurement)
        log_result(logfile, measurement)
    total_end_time = time.time()
    total_execution_time = total_end_time - total_start_time
    print("\nBenchmark complete! (double query exeuction)")
    print(f"Total execution time: {total_execution_time:.4f} seconds")

    # Close the connection
    conn.close()
    print("Done.")