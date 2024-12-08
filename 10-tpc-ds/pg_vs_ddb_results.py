import os
import pandas as pd
import matplotlib.pyplot as plt


folder_name_logs = 'analysis'
file_name_ddb = "2024-12-07T10-08-29_results_eu.csv"
file_name_pg = "2024-12-07T18-52-54_results_pg_eu.csv"


def combine_data():
    # read and clean both source files
    ddb_file_path = os.path.join(folder_name_logs, file_name_ddb)
    dfa = pd.read_csv(ddb_file_path, delimiter=";", decimal=",")
    dfa["source"] = "duckdb"
    dfa = dfa[["source", "query", "t_first"]]

    pg_file_path = os.path.join(folder_name_logs, file_name_pg)
    dfb = pd.read_csv(pg_file_path, delimiter=";", decimal=",")
    dfb["source"] = "postgres"
    dfb = dfb[["source", "query", "t_first"]]

    # make long df
    long = pd.concat([dfa, dfb], ignore_index=True)

    # make wide df
    wide = pd.merge(dfa.rename(columns={'t_first': 't_duckdb'}), 
                    dfb.rename(columns={'t_first': 't_postgres'}), 
                               on='query')
    wide['ratio'] = wide['t_postgres'] / wide['t_duckdb']
    return long, wide


def monotonous_ratio_curve(df):
    # Sort the DataFrame by the `ratio` column
    df_sorted = df.sort_values(by="ratio").reset_index(drop=True)

    # Plot the bar chart
    plt.figure(figsize=(10, 6))
    plt.bar(range(len(df_sorted)), df_sorted["ratio"], color="blue", edgecolor="black")

    # Set log scale for y-axis
    plt.yscale("log")

    # Add horizontal grid lines at specific values
    plt.hlines([100, 1000, 10000], xmin=-1, xmax=len(df_sorted), colors='gray', linestyles='dashed', linewidth=0.8)

    # Remove x-axis labels
    plt.xticks([], [])

    # Add axis labels and title
    plt.ylabel("Ratio (Log Scale)")
    plt.title("Monotonous Bar Chart of Increasing Ratios")
    plt.show()


def side_by_side_chart(wide):
    # Set up the figure dimensions
    n_rows, n_cols = 20, 5
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(15, 30), constrained_layout=True)

    # Flatten the axes for easier iteration
    axes = axes.flatten()

    # Plot each query as a small bar chart
    for i, ax in enumerate(axes):
        if i < len(wide):
            query_row = wide.iloc[i]
            t_duckdb = query_row["t_duckdb"]
            t_postgres = query_row["t_postgres"]
            
            # Check for timeout condition
            if t_postgres >= 360:
                ax.bar(["DuckDB"], [t_duckdb], color="yellow")  # Plot only DuckDB bar
                ax.text(0.5, 0.5, "timeout", color="red", ha="center", va="center", fontsize=10, transform=ax.transAxes)
            else:
                ax.bar(["DuckDB", "Postgres"], [t_duckdb, t_postgres], color=["yellow", "blue"])
            
            # Add title and y-axis labels
            ax.set_title(f"Query {int(query_row['query'])}", fontsize=8)
            ax.set_ylabel("Time (s)", fontsize=6)
            
            # Remove x-axis labels
            ax.set_xticks([])
        else:
            ax.axis("off")  # Hide empty subplots

    # Add an overall title
    fig.suptitle("DuckDB vs Postgres Execution Times by Query", fontsize=16, y=1.02)

    # Save or show the plot
    plt.show()


def main_analyse(wide):
    # remove the postgres time out records!
    df = wide[wide["t_postgres"] < 360].reset_index()
    n_timeout = sum(wide["t_postgres"] >= 360)
    print(f"Amount of postgres results that timed out: {n_timeout}")
        
    # ratio analysis
    monotonous_ratio_curve(df)


if __name__ == "__main__":
    long, wide = combine_data()
    main_analyse(wide)
    