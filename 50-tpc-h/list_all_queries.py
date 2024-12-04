"""listing the queries in the tcp-h duckdb extension"""

import duckdb
import os

SUBFOLDER = 'queries'

def get_queries():
    conn = duckdb.connect("data/tpch-sf1.db")
    results = conn.execute("FROM tpch_queries();").fetchall()
    conn.close()
    return results


def write_queries(results):
    for index,query in results:
        file_name = f"query_{index}.sql"
        print(f"Writing {file_name}")
        file_path = os.path.join(SUBFOLDER, file_name)
        with open(file_path, 'w') as f:
            f.writelines(query)

    
if __name__ == "__main__":
    results = get_queries()
    write_queries(results)

