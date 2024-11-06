import os
import pandas as pd
from log_trans import modify_log_data
from trace_trans import calculate_end_time_unix_nano
from trace_trans import process_trace_data
from metric import read_csv_file
from metric import create_output_folder
from pathlib import Path
from metric import split_and_save_by_service
from metric import normalize_bucket_counts
from metric import process_bucket_counts
from metric import process_files_in_folder
from metric import calculate_weighted_sum
from metric import add_latency_columns
from metric import re_read_csv_file
from metric import process_data
from metric import save_to_csv
from metric import process_folder
from merge import process_data_merge
from merge import merge_files
from merge import process_csv_file_merge
from merge import trans_nezha

def log_trans_runner(input_dir, output_dir):
    modify_log_data(input_dir, output_dir)


def trace_trans_runner(input_dir, output_dir):
    calculate_end_time_unix_nano(input_dir, output_dir)
    process_trace_data(output_dir)

def metric_runner(input_dir, output_dir):
    df = read_csv_file(input_dir)

    # 创建输出文件夹
    create_output_folder(output_dir)

    # 按服务名分组并保存到不同的文件
    split_and_save_by_service(df, output_dir)

    process_files_in_folder(output_dir)

    # 定义权重
    weights = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10, 10]

    # 计算加权和并添加到文件
    for filename in os.listdir(output_dir):
        if filename.endswith('.csv'):
            file_path = os.path.join(output_dir, filename)
            df = pd.read_csv(file_path)

            # 计算加权和
            df['P90'] = df['Latency_P90'].apply(lambda x: calculate_weighted_sum(x, weights)) / 10
            df['P95'] = df['Latency_P95'].apply(lambda x: calculate_weighted_sum(x, weights)) / 5
            df['P99'] = df['Latency_P99'].apply(lambda x: calculate_weighted_sum(x, weights))

            # 添加client和server延迟的列
            df = add_latency_columns(df)

            # 只保留需要的列
            df = df[['TimeUnix', 'client_P90', 'client_P95', 'client_P99', 'server_P90', 'server_P95', 'server_P99']]

            # 保存修改后的DataFrame，覆盖原文件
            df.to_csv(file_path, index=False)
            print(f"Updated and saved {file_path}")
    process_folder(output_dir, output_dir)
    
def merge_runner(input_dir, output_dir, merged_output_dir):
    if not os.path.exists(input_dir):
        os.makedirs(input_dir)
    for filename in os.listdir(input_dir):
        if filename.endswith('.csv'):
            input_file = os.path.join(input_dir, filename)
            output_file = os.path.join(input_dir, f'{filename}')

            try:
                # 读取数据
                df = read_csv_file(input_file)

                # 处理数据
                result = process_data_merge(df)

                # 保存结果
                save_to_csv(result, output_file)
                append_dir = os.path.join('/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/Nezha/metric', f'{filename}')
                file_exists = os.path.isfile(append_dir)
                result.to_csv(append_dir, mode='a', header=not file_exists, index=False)
                print(f"结果已保存到 {output_file}")

            except KeyError as e:
                print(f"跳过文件 {filename}，错误：{e}")

            except Exception as e:
                print(f"处理文件 {filename} 时发生错误：{e}")
    merge_files(output_dir, input_dir, merged_output_dir)
    trans_nezha(merged_output_dir)

def main():
    prefix_dirs = [
        '/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-basic-service-1024-1756',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-basic-service-1027-0246',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-basic-service-1027-0546',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-basic-service-1027-0846',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-basic-service-1027-1146',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-basic-service-1027-1446',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-basic-service-1027-1746',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-consign-service-1024-1636',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-consign-service-1027-0126',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-consign-service-1027-0426',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-consign-service-1027-0726',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-consign-service-1027-1026',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-consign-service-1027-1326',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-consign-service-1027-1626',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-food-service-1024-1816',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-food-service-1027-0306',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-food-service-1027-0606',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-food-service-1027-0906',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-food-service-1027-1206',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-food-service-1027-1506',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-food-service-1027-1806',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-route-service-1027-0146',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-route-service-1027-0446',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-route-service-1027-0746',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-route-service-1027-1046',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-route-service-1027-1346',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-route-service-1027-1646',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-seat-service-1024-1856',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-seat-service-1027-0346',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-seat-service-1027-0646',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-seat-service-1027-0946',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-seat-service-1027-1246',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-seat-service-1027-1546',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-seat-service-1027-1846',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-security-service-1024-1836',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-security-service-1027-0326',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-security-service-1027-0626',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-security-service-1027-0926',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-security-service-1027-1226',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-security-service-1027-1526',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-security-service-1027-1826',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-train-service-1024-1716',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-train-service-1027-0206',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-train-service-1027-0506',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-train-service-1027-0806',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-train-service-1027-1106',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-train-service-1027-1406',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-train-service-1027-1706',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-travel-service-1024-1736',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-travel-service-1027-0226',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-travel-service-1027-0526',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-travel-service-1027-0826',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-travel-service-1027-1126',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-travel-service-1027-1426',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-travel-service-1027-1726',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-travel2-service-1024-1936',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-travel2-service-1027-0406',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-travel2-service-1027-0706',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-travel2-service-1027-1006',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-travel2-service-1027-1306',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-travel2-service-1027-1606',
'/Users/phoebe/Library/CloudStorage/OneDrive-CUHK-Shenzhen/RCA_Dataset/test/ts/ts-travel2-service-1027-1906'
           ]
    for prefix_dir in prefix_dirs:
        input_dir_log = Path(prefix_dir) / 'abnormal/logs.csv'
        output_dir_log = Path(prefix_dir) / 'Nezha_log.csv'
        input_dir_trace = Path(prefix_dir) / 'abnormal/traces.csv'
        output_dir_trace = Path(prefix_dir) / 'Nezha_traces.csv'
        input_dir_metric = Path(prefix_dir) / 'abnormal/request_metrics.csv'
        output_dir_metric = Path(prefix_dir) / 'Nezha_output_files'
        input_dir_merge = Path(prefix_dir) / 'abnormal/processed_metrics'
        output_dir_merge = Path(prefix_dir) / 'Nezha_output_files'
        output_dir_merge_output = Path(prefix_dir) / 'Nezha_merged_output'
        log_trans_runner(input_dir_log, output_dir_log)
        trace_trans_runner(input_dir_trace, output_dir_trace)
        metric_runner(input_dir_metric, output_dir_metric)
        merge_runner(input_dir_merge, output_dir_merge, output_dir_merge_output)

if __name__ == "__main__":
    main()

