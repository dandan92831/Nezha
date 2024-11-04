import pandas as pd

def modify_trace_data(csv_file):
    # Read the CSV file
    df = pd.read_csv(csv_file)

    # Remove the SeverityText and SeverityNumber columns
    df.drop(columns=['SeverityText', 'SeverityNumber'], inplace=True)

    # Add the Node column with empty values
    df['Node'] = ''  # All values set to empty strings

    # Add the Container column with the value 'server'
    df['Container'] = 'server'  # All values set to 'server'

    # Rename ServiceName to PodName
    df.rename(columns={'ServiceName': 'PodName'}, inplace=True)
    df.rename(columns={'Body': 'Log'}, inplace=True)
    df.rename(columns={'SpanId': 'SpanID'}, inplace=True)
    df.rename(columns={'TraceId': 'TraceID'}, inplace=True)

    # Specify the new column order
    new_order = ['Timestamp','Node','PodName','Container','TraceID','SpanID','Log']

    # Reorder the DataFrame columns
    df = df[new_order]

    # Save the updated DataFrame to a new CSV file
    df.to_csv(csv_file, index=False)

"""
# Example usage
if __name__ == "__main__":
    csv_file = 'logs.csv'  # Replace with your input CSV file path
    modify_trace_data(csv_file)
"""
