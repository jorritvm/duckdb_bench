setlocal

set scale_factor=10

REM Enable verbose logging
set VERBOSE=true

REM Generate the data within DuckDB and export it to CSV
duckdb -c "CALL dsdgen(sf=%scale_factor%); EXPORT DATABASE 'data\sf%scale_factor%' (FORMAT CSV, DELIMITER '|');"
