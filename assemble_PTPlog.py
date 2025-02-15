import pandas as pd

# 读取csv文件，假设csv的列名为 'timestamp' 和 'data'
csv_file_path = 'merged_result.csv'
df_csv = pd.read_csv(csv_file_path, usecols=['timestamp', 'data'],dtype={'timestamp':float,'data':float})

# 读取txt文件，假设每行格式如下：时间戳 数据
txt_file_path = 'logs.txt'
with open(txt_file_path, 'r') as file:
    txt_lines = file.readlines()


# 提取txt中的数据和时间戳
txt_data = []
for line in txt_lines:
    parts = line.split()
    
    # 检查是否有足够的数据项（至少有时间戳 + master offset + s2 freq + path delay）
    if len(parts) < 11:
        continue  # 如果数据不完整，跳过此行
    
    timestamp = float(parts[0])  # 时间戳
    master_offset = int(parts[4])  # master offset
    s2_freq = int(parts[7])  # s2 freq
    path_delay = int(parts[10])  # path delay
    
    txt_data.append((timestamp, master_offset, s2_freq, path_delay))

# 函数：根据整数时间戳找到最接近的匹配
def find_closest_timestamp(ts, df_csv):
    # 仅考虑整数部分
    int_ts = int(ts)
    # 查找整数部分匹配的行
    possible_matches = df_csv[abs(df_csv['timestamp'] - int_ts) <= 5]
    if not possible_matches.empty:
        # 找到最近的匹配行
        closest_row = possible_matches.iloc[(possible_matches['timestamp'] - int_ts).abs().argmin()]
        return closest_row
    return None

# 匹配并合并数据
for timestamp, master_offset, s2_freq, path_delay in txt_data:
    # 找到最接近的csv时间戳
    closest_row = find_closest_timestamp(timestamp, df_csv)
    
    if closest_row is not None:
        # 计算时间戳差值
        timestamp_diff = abs(closest_row['timestamp'] - timestamp)
        
        # 更新csv对应行
        df_csv.loc[closest_row.name, 'master_offset'] = master_offset
        df_csv.loc[closest_row.name, 's2_freq'] = s2_freq
        df_csv.loc[closest_row.name, 'path_delay'] = path_delay
        df_csv.loc[closest_row.name, 'timestamp_diff'] = timestamp_diff

# 将更新后的DataFrame保存为新csv
df_csv.to_csv('updated_data.csv', index=False)
