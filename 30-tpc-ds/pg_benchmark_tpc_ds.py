import os
import subprocess
import time
import datetime
import csv

# Database credentials
PGUSER = 'postgres'
PGPASSWORD = 'my_temp_password'
PGHOST = 'localhost'
PGPORT = '5432'
PGDATABASE = 'tcpds_1'
PGSCHEMA = 'tcpds_1'

SUBFOLDER_LOGS = "results"
queries_dir = './queries/'
scale_factor = 10
TIMEOUT = 360 # 360 seconds = 6 minutes


def get_timestamp():
    # Get current datetime in ISO 8601 format
    now = datetime.datetime.now()
    filename_safe_iso8601 = now.strftime("%Y-%m-%dT%H-%M-%S")
    return filename_safe_iso8601


def start_logfile(): 
    file_name = get_timestamp() + "_results_pg.csv"
    file_path = os.path.join(SUBFOLDER_LOGS, file_name)
    with open(file_path, 'w') as f:
        f.write("timestamp,db,sf,query,t_first,t_second\n")
    return file_path


def run_query(query_number):
    query_file = os.path.join(queries_dir, f'query_{query_number}.sql')

    # Construct the psql batch run file
    with open("pg_run.bat", 'w') as file:
        file.write(f"@echo off\n")
        file.write(f"set PGPASSWORD={PGPASSWORD}\n")
        # file.write(f"psql -U {PGUSER} -h {PGHOST} -p {PGPORT} -d {PGDATABASE} -v ON_ERROR_STOP=1 --set schema={PGSCHEMA} -f {query_file}") # output pagination issue
        file.write(f"psql -U {PGUSER} -h {PGHOST} -p {PGPORT} -d {PGDATABASE} -v ON_ERROR_STOP=1 --set schema={PGSCHEMA} -A -o NUL -f {query_file}")
    
    # Execute the query
    ts = get_timestamp()
    t_0 = time.time()
    try:
        subprocess.run("pg_run.bat", shell=True, check=True, timeout=TIMEOUT)  
    except subprocess.TimeoutExpired:
        print(f"Query {query_number} timed out after {TIMEOUT} seconds.")
    
    t_1 = time.time()
    t_first =  t_1 - t_0
    print(f"Executed in {t_first} seconds")

    return {'timestamp': ts, 
        'db': 'postgres',
        'sf': scale_factor,
        'query': query_number,
        't_first': t_first,
        't_second': 0
        }


def log_result(logfile, measurement):
    with open(logfile, 'a', newline='') as f:
        writer = csv.writer(f)  # Use csv.writer to handle CSV format
        writer.writerow([measurement['timestamp'], 
                         measurement['db'],
                         measurement['sf'],
                         measurement['query'], 
                         measurement['t_first'], 
                         measurement['t_second']])


def main():
    logfile = start_logfile()
    
    # Loop through all the queries (1 to 99)
    for i in range(1, 100):
        print(f"Evaluating query: {i}")
        measurement = run_query(i)
        print(measurement)
        log_result(logfile, measurement)
    os.remove("pg_run.bat")
    print("Done.")

if __name__ == '__main__':
    main()
