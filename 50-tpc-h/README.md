# TPC-H DuckDB scalability benchmark
## Goal
Perform the TPC-H benchmark on multiple sizes of the TPC-H database and evaluate impact on query performance.

## How to use
1. download the desized DB sizes from https://duckdb.org/docs/extensions/tpch.html into data/ subfolder
2. (optional) store the queries in the queries/ subfolder by executing `list_all_queries.py`
3. perform the benchmark in `benchmark_tpc-h.py` 
   1. set up the list of the databases to test on
   2. run the script
4. evaluate the results by executing `process_results.py`
   1. set up the path to the benchmark output .csv file to process
   2. run the script