import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import seaborn as sns
import re

def read_logfile_to_dataframe(file_path: str) -> pd.DataFrame:
    """Reads a logfile in CSV format and loads it into a pandas DataFrame."""
    try:
        # Read the file into a pandas DataFrame
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        print(f"Error reading the file: {e}")
        return None
    

def restrict_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Filters the DataFrame to keep only the specified columns."""
    columns_to_keep = ['task', 'data', 'in_rows', 'question', 'solution', 'run', 'time_sec', 'mem_gb']
    df_restricted = df[columns_to_keep]
    return df_restricted


def show_large_groupby_comparison(df):
    # Filter data based on the given conditions
    filtered_df = df[(df['run'] == 1) & 
                     (df['task'] == 'groupby') & 
                     (df['in_rows'] == 100000000)] # 1e8
    
    # Check if there are enough solutions to plot
    if filtered_df.empty:
        print("No data found matching the criteria.")
        return
    
    # Group the data by 'solution' and 'question', then calculate the mean 'time_sec' for each group
    grouped_df = filtered_df.groupby(['question', 'solution'])['time_sec'].mean().reset_index()

    # Define custom colors for each solution
    solution_colors = {
        'data.table': 'gray',
        'duckdb': 'yellow',
        'pandas': 'green',
        'polars': 'blue'
    }
    
    # Set up the FacetGrid with 'question' as the facet variable, 2 columns and 5 rows, sharing y-axis
    g = sns.FacetGrid(grouped_df, col='question', col_wrap=2, height=4, sharey=True, sharex=False)

    # Map the horizontal barplot function to the grid with custom colors
    g.map(sns.barplot, 'time_sec', 'solution', palette=solution_colors, orient='h')

    # Customize the plots
    g.set_xlabels('Average Time (sec)')
    g.set_titles('{col_name}')
    g.fig.suptitle('Time Comparison for Groupby Task (in_rows = 1e8, run = 1)', fontsize=16)

    # Adjust layout to avoid overlap between subtitle and x-axis labels
    g.fig.tight_layout(pad=3)  # Increase padding between subplots
    g.fig.subplots_adjust(top=0.85, right=0.85)  # Adjust top and right space for the title

    # Show the plot
    plt.show()


def show_join_task_comparison(df):
    # Filter data based on the given conditions
    filtered_df = df[(df['task'] == 'join') & 
                     (df['run'] == 1) & 
                     (df['in_rows'].isin([10000000, 100000000]))]  # Consider two sizes
    
    # Check if there are enough solutions to plot
    if filtered_df.empty:
        print("No data found matching the criteria.")
        return
    
    # Group the data by 'question', 'solution', and 'in_rows', then calculate the mean 'time_sec' for each group
    grouped_df = filtered_df.groupby(['question', 'solution', 'in_rows'])['time_sec'].mean().reset_index()
    grouped_df['in_rows2'] = grouped_df['in_rows'].map({10000000: '1e7', 100000000: '1e8'})

    # Define custom colors for each solution
    solution_colors = {
        'data.table': 'gray',
        'duckdb': 'yellow',
        'pandas': 'green',
        'polars': 'blue'
    }
    
    # Set up the FacetGrid with 'question' as rows and 'in_rows' as columns
    g = sns.FacetGrid(grouped_df, row='question', col='in_rows2', height=4, aspect=1.5, sharey=True, sharex=False)

    # Map the horizontal barplot function to the grid with custom colors
    g.map(sns.barplot, 'time_sec', 'solution', palette=solution_colors, orient='h')

    # Customize the plots
    g.set_xlabels('Average Time (sec)')
    g.set_ylabels('Solution')
    g.set_titles(row_template='{row_name}', col_template='Size: {col_name}') 
    g.fig.suptitle('Time Comparison for Join Task (run = 1)', fontsize=16)

    # Adjust layout to avoid overlap between the title and subplots
    g.fig.tight_layout(pad=3)  # Add padding between subplots
    g.fig.subplots_adjust(top=0.9)  # Adjust top space for the title

    # Show the plot
    plt.show()


if __name__ == "__main__":
    # Load the data from CSV.
    file_path = "results/time.csv"
    df = read_logfile_to_dataframe(file_path)
    df_restricted = restrict_columns(df)
    print(df_restricted.tail())

    show_large_groupby_comparison(df_restricted) # 1e8

    show_join_task_comparison(df_restricted)

    