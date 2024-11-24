import pandas as pd
from datetime import datetime
import os


def calculate_end_time_unix_nano(csv_file, outputfile):
    # Read the CSV file
    df = pd.read_csv(csv_file)

    # List to store EndTimeUnixNano values
    end_time_nano_list = []

    # Process each row in the DataFrame
    for index, row in df.iterrows():
        # Split the timestamp to separate nanoseconds
        timestamp_str = row['Timestamp']
        dt_part, nano_part = timestamp_str.split('.')

        # Parse the datetime part
        timestamp_dt = datetime.strptime(dt_part, '%Y-%m-%d %H:%M:%S')

        # Convert timestamp to Unix time in seconds
        unix_time_seconds = int(timestamp_dt.timestamp())

        # Convert Unix time to nanoseconds and add nanoseconds part
        start_time_nano = unix_time_seconds * 10 ** 9 + int(nano_part)

        # Get the duration in microseconds and convert it to nanoseconds
        duration_microseconds = row['Duration']
        duration_nano = duration_microseconds * 1000

        # Calculate the EndTimeUnixNano
        end_time_nano = start_time_nano + duration_nano

        # Append the result to the list
        end_time_nano_list.append(end_time_nano)

    # Add the EndTimeUnixNano as a new column in the original DataFrame
    df['EndTimeUnixNano'] = end_time_nano_list

    # Save the updated DataFrame to a new CSV file
    df.to_csv(outputfile, index=False)



def process_trace_data(output_file):
    # Read the CSV file
    df = pd.read_csv(output_file)

    # Define a list to store results
    results = []

    # Process each row in the DataFrame
    for index, row in df.iterrows():
        # Split the timestamp to separate nanoseconds
        timestamp_str = row['Timestamp']
        dt_part, nano_part = timestamp_str.split('.')

        # Parse the datetime part
        timestamp_dt = datetime.strptime(dt_part, '%Y-%m-%d %H:%M:%S')

        # Convert timestamp to Unix time in seconds
        unix_time_seconds = int(timestamp_dt.timestamp())

        # Convert Unix time to nanoseconds and add nanoseconds part
        start_time_nano = unix_time_seconds * 10 ** 9 + int(nano_part)

        # Get EndTimeUnixNano from the row
        end_time_nano = row['EndTimeUnixNano']

        # Append the reformatted data to results
        results.append({
            'TraceID': row['TraceId'],
            'SpanID': row['SpanId'],
            'ParentID': row['ParentSpanId'],
            'PodName': row['ServiceName'],
            'OperationName': row['SpanName'],
            'StartTimeUnixNano': start_time_nano,
            'EndTimeUnixNano': end_time_nano
        })

    # Create a DataFrame for the results
    results_df = pd.DataFrame(results)
    file_exists = os.path.isfile('/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/Nezha/trace.csv')

    # Save the updated DataFrame to a new CSV file
    results_df.to_csv(output_file, index=False)
    results_df.to_csv('/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/Nezha/trace.csv', mode='a', header=not file_exists, index=False)


# Example usage
if __name__ == "__main__":
    csv_file = '../traces.csv'  # Replace with your input CSV file path
    outputfile = 'Nezha/1021/trace'
    calculate_end_time_unix_nano(csv_file, outputfile)
    process_trace_data(outputfile)