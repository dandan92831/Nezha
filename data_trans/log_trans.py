import pandas as pd
import os


def modify_log_data(csv_file, output_file, log_output_file):
    # Read the CSV file
    df = pd.read_csv(csv_file)

    # Remove the SeverityText and SeverityNumber columns
    df.drop(columns=["SeverityText", "SeverityNumber"], inplace=True)

    # Add the Node column with empty values
    df["Node"] = ""  # All values set to empty strings

    # Add the Container column with the value 'server'
    df["Container"] = "server"  # All values set to 'server'

    # Rename columns
    df.rename(
        columns={
            "ServiceName": "PodName",
            "Body": "Log",
            "SpanId": "SpanID",
            "TraceId": "TraceID",
        },
        inplace=True,
    )

    # Specify the new column order
    new_order = [
        "Timestamp",
        "Node",
        "PodName",
        "Container",
        "TraceID",
        "SpanID",
        "Log",
    ]

    # Reorder the DataFrame columns
    df = df[new_order]

    # Check if the output file already exists
    file_exists = os.path.isfile(log_output_file)

    # Save the updated DataFrame to a new CSV file, appending if it exists
    df.to_csv(output_file, index=False)
    df.to_csv(log_output_file, mode="a", header=not file_exists, index=False)


# # Example usage
# if __name__ == "__main__":
#     csv_file = '../logs.csv'  # Replace with your input CSV file path
#     output_file = '../Nezha/log.csv'  # Replace with your desired output CSV file path
#     modify_log_data(csv_file, output_file)
