import os
import pandas as pd

def read_csv_file(file_path):
    return pd.read_csv(file_path)

# 处理数据，每五行取均值
def process_data(df):
    # 将 TimeUnix 转换为日期时间格式a
    df['TimeUnix'] = pd.to_datetime(df['TimeUnix'])

    # 每五行分组，计算均值
    result = df.groupby(df.index // 12).agg({
        'TimeUnix': 'first',  # 取每组中的第一个时间戳
        'k8s.pod.cpu.usage': 'mean',
        'k8s.pod.cpu_limit_utilization': 'mean',
        'k8s.pod.memory.usage': 'mean',
        'k8s.pod.memory_limit_utilization': 'mean',
        'k8s.pod.network.errors': 'mean',
        'receive_bytes': 'mean',
        'transmit_bytes': 'mean'
    }).reset_index(drop=True)

    return result

# 保存结果到CSV文件的函数
def save_to_csv(df, output_file):
    df.to_csv(output_file, index=False)

def merge_files(output_dir, processed_dir, merged_output_dir):
    # 创建输出文件夹
    if not os.path.exists(merged_output_dir):
        os.makedirs(merged_output_dir)

    # 遍历 output_files 目录中的所有 CSV 文件
    for filename in os.listdir(output_dir):
        if filename.endswith('.csv'):
            output_file = os.path.join(output_dir, filename)

            # 提取截止至 "service" 之前的文件名部分
            prefix = filename.split('-service')[0]

            # 查找 processed_metrics 目录下同名文件，使用字符串匹配
            processed_file = None
            for processed_filename in os.listdir(processed_dir):
                if processed_filename.startswith(prefix):
                    processed_file = os.path.join(processed_dir, processed_filename)
                    break

            if processed_file and os.path.exists(processed_file):
                output_df = pd.read_csv(output_file)
                processed_df = pd.read_csv(processed_file)

                # 合并数据，不保留 processed_metrics 中的 TimeUnix 列
                merged_data = pd.concat([output_df, processed_df.drop('TimeUnix', axis=1)], axis=1)

                # 保存合并后的结果，输出文件名加上 "-service"
                output_path = os.path.join(merged_output_dir, f"{prefix}-service.csv")
                merged_data.to_csv(output_path, index=False)
                print(f"已保存文件: {output_path}")
            else:
                print(f"跳过文件: {output_file}，因为 processed_metrics 目录下没有匹配的文件")


def process_csv_file(input_path, output_path):
    # 读取 CSV 文件
    df = pd.read_csv(input_path)

    # 重命名列
    rename_mapping = {
        'TimeUnix': 'TimeUnix',
        'k8s.pod.cpu.usage': 'CpuUsage(m)',
        'k8s.pod.cpu_limit_utilization': 'CpuUsageRate(%)',
        'k8s.pod.memory.usage': 'MemoryUsage(Mi)',
        'k8s.pod.memory_limit_utilization': 'MemoryUsageRate(%)',
        'receive_bytes': 'NetworkReceiveBytes',
        'transmit_bytes': 'NetworkTransmitBytes',
        'client_P90': 'PodClientLatencyP90(s)',
        'server_P90': 'PodServerLatencyP90(s)',
        'client_P95': 'PodClientLatencyP95(s)',
        'server_P95': 'PodServerLatencyP95(s)',
        'client_P99': 'PodClientLatencyP99(s)',
        'server_P99': 'PodServerLatencyP99(s)'
    }

    # 应用列重命名
    df = df.rename(columns=rename_mapping)

    # 按指定的列顺序重新排序
    column_order = [
        'TimeUnix', 'CpuUsage(m)', 'CpuUsageRate(%)', 'MemoryUsage(Mi)', 'MemoryUsageRate(%)',
        'NetworkReceiveBytes', 'NetworkTransmitBytes', 'PodClientLatencyP90(s)',
        'PodServerLatencyP90(s)', 'PodClientLatencyP95(s)', 'PodServerLatencyP95(s)',
        'PodClientLatencyP99(s)', 'PodServerLatencyP99(s)'
    ]

    # 只保留指定列并按照顺序排列
    df = df[column_order]

    # 将处理后的数据写入新的 CSV 文件
    df.to_csv(output_path, index=False)
    print(f"Processed file saved to {output_path}")

def trans_nezha(merged_data):
    # 遍历输入文件夹中的所有 CSV 文件
    for filename in os.listdir(merged_data):
        if filename.endswith('.csv'):
            input_path = os.path.join(merged_data, filename)
            output_path = os.path.join(merged_data, filename)

            # 处理每个 CSV 文件
            process_csv_file(input_path, output_path)

# 主函数
def main(input_folder):
    # 遍历输入文件夹中的所有CSV文件
    for filename in os.listdir(input_folder):
        if filename.endswith('.csv'):
            input_file = os.path.join(input_folder, filename)
            output_file = os.path.join(input_folder, f'{filename}')

            try:
                # 读取数据
                df = read_csv_file(input_file)

                # 处理数据
                result = process_data(df)

                # 保存结果
                save_to_csv(result, output_file)
                print(f"结果已保存到 {output_file}")

            except KeyError as e:
                print(f"跳过文件 {filename}，错误：{e}")

            except Exception as e:
                print(f"处理文件 {filename} 时发生错误：{e}")


"""
示例输入
if __name__ == "__main__":
    input_folder = 'processed_metrics'  # 替换为你的输入文件夹路径
    output_dir = 'output_files'  # 输出文件夹路径
    merged_output_dir = 'merged_output'  # 合并输出文件夹路径

    # 如果输出文件夹不存在，则创建它
    if not os.path.exists(input_folder):
        os.makedirs(input_folder)

    main(input_folder)
    merge_files(output_dir, input_folder, merged_output_dir)
    trans_nezha(merged_output_dir)
"""

