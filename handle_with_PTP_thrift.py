import pandas as pd
import numpy as np
threshold=0.5
# 读取CSV文件
df = pd.read_csv('updated_data.csv', header=0, usecols=range(6))

# 步骤1: 添加第七列（列索引6）
df['PTPts'] = df.apply(lambda row: row.iloc[0] + row.iloc[5] 
                 if pd.notna(row.iloc[0]) and pd.notna(row.iloc[5]) 
                 else np.nan, axis=1)
df.to_csv('ts.csv', index=False)

# 步骤2: 处理需要移动的行
df.reset_index(drop=True, inplace=True)
mask = (df.iloc[:, 5].notna()) & (df.iloc[:, 5] > threshold)
move_indices = df[mask].index.tolist()

for idx in sorted(move_indices, reverse=True):
    value = df.at[idx, df.columns[5]]
    n = int(value // threshold)
    if n == 0:
        continue
    
    # 获取需要移动的数据
    data_to_move = df.loc[idx, df.columns[2:7]].copy()
    
    # 清空原行数据
    df.loc[idx, df.columns[2:7]] = np.nan
    
    # 计算目标位置
    target_idx = idx + n
    
    # 扩展DataFrame如果必要
    if target_idx >= len(df):
        extension = pd.DataFrame(
            index=range(len(df), target_idx + 1),
            columns=df.columns
        )
        df = pd.concat([df, extension], ignore_index=False)
    
    # 移动数据
    df.loc[target_idx, df.columns[2:7]] = data_to_move.values

# 步骤3: 生成配对数据
pairs = []
for i in range(len(df)):
    if not df.iloc[i, 2:7].isna().all():
        last_empty = None
        for j in range(i+1, len(df)):
            if df.iloc[j, 2:7].isna().all():
                last_empty = j
            else:
                break
        if last_empty is not None:
            # 获取配对数据
            empty_col0 = df.iloc[last_empty, 0]
            empty_col1 = df.iloc[last_empty, 1]
            current_data = df.iloc[i, 2:7].tolist()
            new_row = [empty_col0, empty_col1] + current_data
            pairs.append(new_row)

# 创建新DataFrame并保存
new_df = pd.DataFrame(
    pairs,
    columns=df.columns[[0, 1] + list(range(2, 7))]
)
new_df.to_csv('output.csv', index=False)

print("处理完成，结果已保存至output.csv")