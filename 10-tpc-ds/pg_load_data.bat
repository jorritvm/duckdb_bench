set PGUSER=postgres
set PGPASSWORD=my_temp_password
set PGHOST=localhost
set PGPORT=5432
set PGDATABASE=tcpds_1
set PGSCHEMA=tcpds_1

setlocal

REM This script uses DuckDB to generate a TPC-DS database in PostgreSQL.
REM This script uses PostgreSQL connection environment variables from the shell, such as:
REM PGUSER, PGPASSWORD, PGHOST, PGPORT, and PGDATABASE.

set scale_factor=10
set schema_name=tcpds_1

REM Enable verbose logging
set VERBOSE=true

REM Load the data into PostgreSQL
psql -v ON_ERROR_STOP=1 -c "CREATE SCHEMA IF NOT EXISTS %schema_name%;"
psql -f "data\sf%scale_factor%\schema.sql"
psql -f "data\sf%scale_factor%\load-psql.sql"
psql -c "ANALYZE;"
