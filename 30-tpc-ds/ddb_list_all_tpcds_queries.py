import duckdb
import os


# specify the input path
db_file_path = "data/duckdb_sf1.db"

# specify the output paths
out_folder_path = "queries"


def get_queries():
    try:
        conn = duckdb.connect(db_file_path)
        results = conn.execute("CALL tpcds_queries()").fetchall()       
        conn.close()
        return results
    except Exception as e:
        print(f"An error occurred: {e}")


def write_queries(results):
    for index,query in results:
        file_name = f"query_{index}.sql"
        print(f"Writing {file_name}")
        file_path = os.path.join(out_folder_path, file_name)
        with open(file_path, 'w') as f:
            f.writelines(query)

if __name__ == "__main__":
    # extract the queries from the duckdb extension
    res = get_queries()
    write_queries(res)
    print("Done.")