import os
from pathlib import Path
import pandas as pd
from log_trans import modify_log_data
from trace_trans import (
    calculate_end_time_unix_nano,
    process_trace_data,
)
from metric import (
    read_csv_file,
    create_output_folder,
    split_and_save_by_service,
    normalize_bucket_counts,
    process_bucket_counts,
    process_files_in_folder,
    calculate_weighted_sum,
    add_latency_columns,
    re_read_csv_file,
    process_data,
    save_to_csv,
    process_folder,
)
from merge import (
    process_data_merge,
    merge_files,
    process_csv_file_merge,
    trans_nezha,
)


def log_trans_runner(input_dir, output_dir, log_output_file):
    modify_log_data(input_dir, output_dir, log_output_file)


def trace_trans_runner(input_dir, output_dir, trace_output_path):
    calculate_end_time_unix_nano(input_dir, output_dir)
    process_trace_data(output_dir, trace_output_path)

def metric_runner(input_dir, output_dir):
    df = read_csv_file(input_dir)
    create_output_folder(output_dir)
    split_and_save_by_service(df, output_dir)
    process_files_in_folder(output_dir)

    # 定义权重
    weights = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10, 10]

    # 计算加权和并添加到文件
    for filename in os.listdir(output_dir):
        if filename.endswith('.csv'):
            file_path = os.path.join(output_dir, filename)
            df = pd.read_csv(file_path)

            df['P90'] = df['Latency_P90'].apply(lambda x: calculate_weighted_sum(x, weights)) / 10
            df['P95'] = df['Latency_P95'].apply(lambda x: calculate_weighted_sum(x, weights)) / 5
            df['P99'] = df['Latency_P99'].apply(lambda x: calculate_weighted_sum(x, weights))

            df = add_latency_columns(df)

            # 只保留需要的列
            df = df[['TimeUnix', 'client_P90', 'client_P95', 'client_P99', 'server_P90', 'server_P95', 'server_P99']]

            # 保存修改后的DataFrame，覆盖原文件
            df.to_csv(file_path, index=False)
            print(f"Updated and saved {file_path}")
    process_folder(output_dir, output_dir)
    
def merge_runner(input_dir, output_dir, merged_output_dir, output_folder, metric_output_path):
    if not os.path.exists(input_dir):
        os.makedirs(input_dir)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    for filename in os.listdir(input_dir):
        if filename.endswith('.csv'):
            input_file = os.path.join(input_dir, filename)
            output_file = os.path.join(output_folder, f'{filename}')
            try:
                df = read_csv_file(input_file)
                result = process_data_merge(df)
                save_to_csv(result, output_file)
                print(f"结果已保存到 {output_file}")
            except KeyError as e:
                print(f"跳过文件 {filename}，错误：{e}")
            except Exception as e:
                print(f"处理文件 {filename} 时发生错误：{e}")
    merge_files(output_dir, output_folder, merged_output_dir)
    trans_nezha(merged_output_dir, metric_output_path)

def merge_abnormal_normal_to_overall(prefix_dir):
    """
    Merges the CSV files from 'abnormal' and 'normal' directories, 
    including their 'processed_metrics' subdirectories, into an 'overall' directory.

    :param prefix_dir: Path object pointing to the directory containing 'abnormal' and 'normal' folders.
    """
    abnormal_dir = prefix_dir / "abnormal"
    normal_dir = prefix_dir / "normal"
    overall_dir = prefix_dir / "overall"
    overall_dir.mkdir(parents=True, exist_ok=True)  # Create the 'overall' directory if it doesn't exist

    # Define all directories to process
    sub_dirs = ["", "processed_metrics"]

    for sub_dir in sub_dirs:
        abnormal_sub_dir = abnormal_dir / sub_dir
        normal_sub_dir = normal_dir / sub_dir
        overall_sub_dir = overall_dir / sub_dir

        # Ensure the corresponding overall subdirectory exists
        overall_sub_dir.mkdir(parents=True, exist_ok=True)

        # Identify common CSV files in both subdirectories
        abnormal_files = set(f.name for f in abnormal_sub_dir.glob("*.csv") if f.is_file())
        normal_files = set(f.name for f in normal_sub_dir.glob("*.csv") if f.is_file())
        common_files = abnormal_files & normal_files

        for file_name in common_files:
            try:
                # Read files from abnormal and normal
                abnormal_file = abnormal_sub_dir / file_name
                normal_file = normal_sub_dir / file_name

                df_abnormal = pd.read_csv(abnormal_file)
                df_normal = pd.read_csv(normal_file)

                # Combine the data
                combined_df = pd.concat([df_normal, df_abnormal], ignore_index=True)

                # Save to the 'overall' subdirectory
                combined_file = overall_sub_dir / file_name
                combined_df.to_csv(combined_file, index=False)
                print(f"Merged {file_name} into {combined_file}")
            except Exception as e:
                print(f"Error merging {file_name} in {sub_dir}: {e}")
                

def process_metric(metric_path, metric_output_path):
    df = pd.read_csv(metric_path)
    df.rename(columns={'TimeUnix': 'Time'}, inplace=True)
    df['Time'] = pd.to_datetime(df['Time'], errors='coerce')
    if df['Time'].isna().any():
        df.dropna(subset=['Time'], inplace=True)
    df['TimeStamp'] = df['Time'].apply(lambda x: int(time.mktime(x.timetuple())))
    pod_name = os.path.splitext(os.path.basename(metric_path))[0]
    df['PodName'] = pod_name
    first_columns = ['Time', 'TimeStamp', 'PodName']
    other_columns = [col for col in df.columns if col not in first_columns]
    df = df[first_columns + other_columns]
    output_file_path = os.path.join(metric_output_path,   os.path.basename(metric_path))
    df.to_csv(output_file_path, index=False)


def main():
    root_base_dir = Path(r"E:\Project\Git\RCA_Dataset\test\ts-small")
    output_base_dir = Path(r"E:\Project\Git\RCA_Dataset\test\nezha-ts-small")

    log_output_path = output_base_dir / 'log.csv'
    trace_output_path = output_base_dir / 'trace.csv'
    metric_output_path = output_base_dir / 'metric'
    
    for existing_file in os.listdir(metric_output_path):
        file_path = os.path.join(metric_output_path, existing_file)
        if os.path.isfile(file_path):
            os.remove(file_path)
    
    output_base_dir.mkdir(parents=True, exist_ok=True)  # Ensure the base output directory exists
    for prefix_dir in root_base_dir.iterdir():
        if prefix_dir.is_dir():
            # 合并normal和abnormal的数据到overall目录
            merge_abnormal_normal_to_overall(prefix_dir)

            subdir_name = prefix_dir.name  # Use the name of the subdirectory to create output paths
            output_dir_sub = output_base_dir / subdir_name
            output_dir_sub.mkdir(parents=True, exist_ok=True)  # Ensure subdirectory exists in output base

            input_dir_log = prefix_dir / 'overall/logs.csv'
            output_dir_log = output_dir_sub / 'Nezha_log.csv'
            input_dir_trace = prefix_dir / 'overall/traces.csv'
            output_dir_trace = output_dir_sub / 'Nezha_traces.csv'
            input_dir_metric = prefix_dir / 'overall/request_metrics.csv'
            output_dir_metric = output_dir_sub / 'Nezha_output_files'
            input_dir_merge = prefix_dir / 'overall/processed_metrics'
            output_dir_merge = output_dir_sub / 'Nezha_output_files'
            output_dir_merge_output = output_dir_sub / 'Nezha_merged_output'
            output_folder = output_dir_sub / 'overall/Nezha_processed_metrics'       

            if not os.path.exists(metric_output_path):
                os.makedirs(metric_output_path)
            if not os.path.exists(output_dir_merge):
                os.makedirs(output_dir_merge)
            if not os.path.exists(output_dir_merge_output):
                os.makedirs(output_dir_merge_output)
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)

            log_trans_runner(input_dir_log, output_dir_log, log_output_path)
            trace_trans_runner(input_dir_trace, output_dir_trace,trace_output_path)
            metric_runner(input_dir_metric, output_dir_metric)
            merge_runner(input_dir_merge, output_dir_merge, output_dir_merge_output, output_folder, metric_output_path)
            for metric_path in glob.glob(os.path.join(metric_output_path, '*.csv')):
                process_metric(metric_path, metric_output_path)


if __name__ == "__main__":
    main()
