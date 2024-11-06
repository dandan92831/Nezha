import pandas as pd

def extract_unique_trace_ids(input_file, output_file):
    # Read the input CSV file
    df = pd.read_csv(input_file)

    # Get unique TraceIDs
    unique_trace_ids = df['TraceID'].unique()

    # Create a new DataFrame for unique TraceIDs
    unique_df = pd.DataFrame(unique_trace_ids, columns=['TraceID'])

    # Save the unique TraceIDs to a new CSV file
    unique_df.to_csv(output_file, index=False)

# Example usage
if __name__ == "__main__":
    input_file = '/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/Nezha/trace.csv'  # Replace with your input CSV file path
    output_file = '/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/Nezha/traceid.csv'  # Desired output CSV file path
    extract_unique_trace_ids(input_file, output_file)