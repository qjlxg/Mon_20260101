import os
import shutil
import glob

def sync_csv_files():
    # 定义路径
    source_dir = 'qjlxg/4.0/stock_data'
    target_dir = 'stock_data'

    # 确保目标目录存在
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
        print(f"创建目录: {target_dir}")

    # 查找源目录下所有的 csv 文件
    csv_files = glob.glob(os.path.join(source_dir, '*.csv'))
    
    if not csv_files:
        print("未在源目录找到 CSV 文件。")
        return

    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        dest_path = os.path.join(target_dir, file_name)
        
        # 执行复制（覆盖模式）
        shutil.copy2(file_path, dest_path)
        print(f"已同步: {file_name}")

    print(f"同步完成，共处理 {len(csv_files)} 个文件。")

if __name__ == "__main__":
    sync_csv_files()
