import pandas as pd
import os
import ast

def read_csv_file(input_file):
    return pd.read_csv(input_file)

# 创建输出文件夹的函数
def create_output_folder(folder_name):
    os.makedirs(folder_name, exist_ok=True)

# 按服务名分组并保存到不同的文件的函数
def split_and_save_by_service(df, output_folder):
    for service_name, group in df.groupby('ServiceName'):
        # 删除ServiceName列
        group = group.drop(columns=['ServiceName'])

        # 生成输出文件路径
        output_file = os.path.join(output_folder, f"{service_name}.csv")

        # 保存分组的数据到CSV文件
        group.to_csv(output_file, index=False)
        print(f"Saved: {output_file}")


# 处理BucketCounts列的函数
def normalize_bucket_counts(df):
    def normalize(row):
        bucket_counts = eval(row['BucketCounts'])  # 使用eval将字符串转换为列表
        count = row['Count']
        normalized_counts = [value / count for value in bucket_counts]
        return str(normalized_counts)  # 返回字符串形式

    df['BucketCounts'] = df.apply(normalize, axis=1)
    return df


# 处理延迟百分比的函数
def process_bucket_counts(bucket_counts_str, percentage):
    data = ast.literal_eval(bucket_counts_str)
    total_data = sum(data)
    target = total_data * percentage  # 计算目标数据量

    accumulated = 0
    percentages = [0] * len(data)  # 初始化输出列表

    for i in reversed(range(len(data))):
        if accumulated < target:
            current_data = data[i]
            if accumulated + current_data >= target:
                needed = target - accumulated
                percentages[i] = needed / total_data * 100  # 计算百分比
                accumulated += needed
                break
            else:
                percentages[i] = current_data / total_data * 100  # 计算百分比
                accumulated += current_data

    return percentages


# 处理文件夹中的所有CSV文件
def process_files_in_folder(folder_path):
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            df = pd.read_csv(file_path)

            # 确保 'BucketCounts' 列存在并进行归一化
            if 'BucketCounts' in df:
                df = normalize_bucket_counts(df)

                # 计算延迟百分比
                df['Latency_P90'] = df['BucketCounts'].apply(lambda x: process_bucket_counts(x, 0.10))
                df['Latency_P95'] = df['BucketCounts'].apply(lambda x: process_bucket_counts(x, 0.05))
                df['Latency_P99'] = df['BucketCounts'].apply(lambda x: process_bucket_counts(x, 0.01))

                # 保存修改后的文件
                df.to_csv(file_path, index=False)
                print(f"Processed and saved {file_path}")
            else:
                print(f"'BucketCounts' column not found in {file_path}")


# 计算加权和的函数
def calculate_weighted_sum(latency_values, weights):
    latency = eval(latency_values)
    weighted_sum = sum(latency[i] * weights[i] for i in range(len(latency)))
    return weighted_sum


# 添加client和server延迟的列
def add_latency_columns(df):
    df['client_P90'] = 0
    df['client_P95'] = 0
    df['client_P99'] = 0
    df['server_P90'] = 0
    df['server_P95'] = 0
    df['server_P99'] = 0

    server_mask = df['MetricName'] == 'http.server.request.duration'
    client_mask = df['MetricName'] == 'http.client.request.duration'

    df.loc[server_mask, ['server_P90', 'server_P95', 'server_P99']] = df.loc[server_mask, ['P90', 'P95', 'P99']].values
    df.loc[client_mask, ['client_P90', 'client_P95', 'client_P99']] = df.loc[client_mask, ['P90', 'P95', 'P99']].values
    return df


def re_read_csv_file(file_path):
    return pd.read_csv(file_path)


def process_data(df):
    # 将 TimeUnix 转换为日期时间格式
    df['TimeUnix'] = pd.to_datetime(df['TimeUnix'])

    # 将空值替换为0
    df.fillna(0, inplace=True)

    # 按照 TimeUnix 分组计算不为0的平均值
    result = df.groupby('TimeUnix').agg(
        client_P90=('client_P90', lambda x: x[x != 0].mean()),
        client_P95=('client_P95', lambda x: x[x != 0].mean()),
        client_P99=('client_P99', lambda x: x[x != 0].mean()),
        server_P90=('server_P90', lambda x: x[x != 0].mean()),
        server_P95=('server_P95', lambda x: x[x != 0].mean()),
        server_P99=('server_P99', lambda x: x[x != 0].mean())
    ).reset_index()

    # 将结果中的空值替换为0
    result.fillna(0, inplace=True)

    return result


# 保存结果到CSV文件的函数
def save_to_csv(df, output_file):
    df.to_csv(output_file, index=False)


# 处理文件夹中的所有CSV文件
def process_folder(input_folder, output_folder):
    # 创建输出文件夹（如果不存在）
    os.makedirs(output_folder, exist_ok=True)

    # 遍历文件夹中的所有CSV文件
    for filename in os.listdir(input_folder):
        if filename.endswith('.csv'):
            file_path = os.path.join(input_folder, filename)
            print(f"Processing {file_path}...")

            # 读取数据
            df = re_read_csv_file(file_path)

            # 处理数据
            result = process_data(df)

            # 生成输出文件路径
            output_file = os.path.join(output_folder, f"{filename}")
            save_to_csv(result, output_file)
            print(f"Saved result to {output_file}")


def main(input_file, output_folder):
    # 处理BucketCounts列
    df = read_csv_file(input_file)

    # 创建输出文件夹
    create_output_folder(output_folder)

    # 按服务名分组并保存到不同的文件
    split_and_save_by_service(df, output_folder)

    process_files_in_folder(output_folder)

    # 定义权重
    weights = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10, 10]

    # 计算加权和并添加到文件
    for filename in os.listdir(output_folder):
        if filename.endswith('.csv'):
            file_path = os.path.join(output_folder, filename)
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



# if __name__ == "__main__":
#     input_file = '../request_metrics.csv'
#     output_folder = 'output_files'
#     main(input_file, output_folder)
#     process_folder(output_folder, output_folder)
