"""compare execution times of tpc-ds on duckdb for different scale factors"""


import os
import pandas as pd
import matplotlib.pyplot as plt


log_folder = 'results'
results = {'sf1': '2024-12-07T14-16-24_results.csv', # sf1
 'sf10': '2024-12-07T10-00-44_results.csv', # sf10
 'sf50': '2024-12-07T10-08-29_results.csv', # sf50
 'sf100': '2024-12-07T14-09-36_results.csv'} # sf100


def combine_data():
    # Load the data from CSV
    data_frames = []    
    for sf,file_name in results.items():
        file_path = os.path.join(log_folder, file_name)
        dfx = pd.read_csv(file_path)
        data_frames.append(dfx)
    df = pd.concat(data_frames, ignore_index=True)
    return df


def compare_runtimes_for_different_sf(df):  
    # Get unique queries and scale factors
    queries = sorted(df['query'].unique())
    scale_factors = sorted(df['sf'].unique())
    
    # Set up the figure and axes
    fig, axes = plt.subplots(5, 20, figsize=(30, 15), constrained_layout=True)
    axes = axes.flatten()  # Flatten to easily iterate
    
    # Define colors for scale factors
    colors = ['blue', 'orange', 'green', 'red']
    
    # Create the subplots
    for i, query in enumerate(queries):
        ax = axes[i]
        query_data = df[df['query'] == query]
        
        # Ensure scale factors are categorical and ordered
        query_data = query_data.sort_values('sf')
        sf_labels = query_data['sf'].astype(str).tolist()
        
        # Create horizontal bar plot
        ax.barh(sf_labels, query_data['t_first'], color=colors[:len(sf_labels)])
        
        # Set labels and title for each subplot
        ax.set_title(f"Query {query}", fontsize=8)
        # ax.set_xlabel("Time (t_first)")
        # ax.set_ylabel("Scale Factor")
    
    # Hide unused subplots
    for i in range(len(queries), len(axes)):
        axes[i].axis('off')
    
    # Show the plot
    plt.suptitle("Performance of Queries Across Scale Factors", fontsize=16)
    plt.show()


def plot_scale_factors(df):
    # Get unique scale factors and queries
    scale_factors = sorted(df['sf'].unique())
    queries = sorted(df['query'].unique())
    
    # Set up the figure and axes
    fig, axes = plt.subplots(4, 1, figsize=(15, 20), constrained_layout=True)
    
    # Define colors for bars
    colors = ['blue', 'orange', 'green', 'red']
    
    for i, sf in enumerate(scale_factors):
        ax = axes[i]
        sf_data = df[df['sf'] == sf]
        
        # Ensure queries are ordered
        sf_data = sf_data.sort_values('query')
        query_numbers = sf_data['query']
        query_labels = query_numbers.astype(str).tolist()
        
        # Create vertical bar plot
        ax.bar(query_numbers, sf_data['t_first'], color=colors[i])
        
        # Customize x-axis ticks to show every 5th query
        ticks = list(range(5, len(query_numbers) + 1, 5))
        ax.set_xticks(ticks)
        ax.set_xticklabels([str(tick) for tick in ticks], rotation=90)
        
        # Set labels and title
        ax.set_title(f"Scale Factor {sf}", fontsize=14)
        # ax.set_xlabel("Query")
        ax.set_ylabel("Time (s)")
    
    # Add a main title
    plt.suptitle("Query Performance Across Scale Factors", fontsize=16)
    plt.show()


if __name__ == "__main__":
    df = combine_data()
    # compare_runtimes_for_different_sf(df)
    plot_scale_factors(df)