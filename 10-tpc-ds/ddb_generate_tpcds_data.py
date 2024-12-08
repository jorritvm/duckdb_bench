import duckdb
import time
import os


# specify scale factor
sf = 100

# specify the output paths
out_folder_path = "data"
out_file_name = f"duckdb_sf{sf}.db"


def build_database(): 
    # Connect to DuckDB and create a persistent database
    file_path = os.path.join(out_folder_path, out_file_name)
    conn = duckdb.connect(file_path)

    try:
        # Install and load the TPC-DS extension
        conn.execute("INSTALL tpcds")
        conn.execute("LOAD tpcds")
        
        # Generate TPC-DS tables for the given scale factor
        print(f"Generating TPC-DS data at scale factor {sf}...")
        conn.execute(f"CALL dsdgen(sf={sf})")

        # The CHECKPOINT statement synchronizes data in the write-ahead log (WAL) to the database data file.
        conn.execute("CHECKPOINT") 
        print("Database generation and persistence complete.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the DuckDB connection
        conn.close()
        print("Database connection closed.")



if __name__ == "__main__":
    # build the databse
    build_database()

    print("Done.")