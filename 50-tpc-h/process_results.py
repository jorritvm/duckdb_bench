import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import seaborn as sns
import re


## helpers
def extract_query_number(query):
    # Use regex to extract the number from PRAGMA tpch(i);
    match = re.search(r'PRAGMA tpch\((\d+)\);', query)
    if match:
        return int(match.group(1))  # Extracted query number as an integer
    return None


## plots
def show_caching_effect_on_slowest_run(df):
    # Filter data for db = tpch-sf100
    df_filtered = df[df['db'] == 'tpch-sf100']

    # Set up the figure and axis for plotting
    fig, ax = plt.subplots(figsize=(10, 8))

    # Define the position for each bar
    query_numbers = list(range(1, 23))  # Query numbers from 1 to 22

    # Plot bars for each query
    for i in query_numbers:
        # Get data for this query
        query_data = df_filtered[df_filtered['query'] == f"PRAGMA tpch({i});"]
        
        # Extract the t_first and t_second values
        t_first = query_data['t_first'].values[0]
        t_second = query_data['t_second'].values[0]
        
        # Plot the bars: `t_first` as the full bar, `t_second` as a smaller overlapping bar
        ax.barh(i-1, t_first, color='black', label=f"{i} - t_first" if i == 0 else "")
        ax.barh(i-1, t_second, color='lightgray', edgecolor='black', height=0.5)

    # Add labels and title
    ax.set_xlabel('Time (seconds)')
    ax.set_ylabel('Query Number')
    ax.set_title('Query Execution Time for first and second run')

    # Set y-axis labels to query numbers
    ax.set_yticks(range(22))
    ax.set_yticklabels([f'{i}' for i in query_numbers])

    # Add a legend to differentiate the bars
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels)

    # Display the plot
    plt.tight_layout()
    plt.show()


def show_execution_time_evolution(df):


    # Filter data for queries 7, 9, 10, 13
    query_numbers = [7, 9, 10, 13] # these are queries with various runtimes
    df_filtered = df[df['query'].isin([f"PRAGMA tpch({i});" for i in query_numbers])]
    df_filtered['query_short'] = df_filtered['query'].apply(extract_query_number)

    # Set up the figure with facets for different queries
    g = sns.FacetGrid(df_filtered, col='query_short', col_wrap=2, height=5, sharey=False)

    # Plot bars for each query
    g.map(sns.barplot, 'db', 't_first', color='lightgray', edgecolor="black")

    # Adjust the axis labels and title
    g.set_axis_labels('Database', 'Execution Time (seconds)')
    g.set_titles('Query {col_name}')
    g.fig.suptitle('Query Execution Time for increasing scale factor', fontsize=16)
    
    # Add a legend manually
    handles, labels = g.axes[0].get_legend_handles_labels()
    g.fig.legend(handles, labels, loc='upper center', ncol=2)

    # Display the plot
    plt.tight_layout()
    plt.show()


def show_normalized_execution_time_evolution(df):
    db_sizes = {
        "tpch-sf1": 254.732,
        "tpch-sf3": 771.852,
        "tpch-sf10": 2612.492,
        "tpch-sf30": 7965.708,
        "tpch-sf100": 26962.956
    }

    # Filter data for queries 7, 9, 10, 13
    query_numbers = [7, 9, 10, 13] # these are queries with various runtimes
    df_filtered = df[df['query'].isin([f"PRAGMA tpch({i});" for i in query_numbers])]
    df_filtered['query_short'] = df_filtered['query'].apply(extract_query_number)

    # Add a new column 'normalized_t_first' 
    df_filtered['normalized_t_first'] = df_filtered.apply(
    lambda row: row['t_first'] / db_sizes.get(row['db'], 1), axis=1)

    # Set the figure size
    plt.figure(figsize=(10, 8))

    # Create a seaborn bar plot
    sns.barplot(data=df_filtered, x='query_short', y='normalized_t_first', hue='db', dodge=True)

    # Add labels and title
    plt.xlabel('Query Number')
    plt.ylabel('Normalized Execution Time')
    plt.title('Execution Time for first run divided by Database Size')

    # Display the plot
    plt.tight_layout()
    plt.show()



if __name__ == "__main__":
    # Load the data from CSV
    file_path = 'results/2024-12-04T14-41-47_results.csv'
    df = pd.read_csv(file_path)

    # show_caching_effect_on_slowest_run(df)

    # show_execution_time_evolution(df)

    show_normalized_execution_time_evolution(df)
