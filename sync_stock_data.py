import os
import shutil
import glob

def sync_csv_files():
    # 这里的路径对应工作流中 checkout 设定的 path
    source_dir = 'source_repo/stock_data' 
    target_dir = 'main_repo/stock_data'

    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    # 获取源目录下所有 csv
    csv_files = glob.glob(os.path.join(source_dir, '*.csv'))
    
    if not csv_files:
        print(f"错误: 在 {source_dir} 未找到 CSV 文件。请检查源仓库路径是否正确。")
        return

    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        dest_path = os.path.join(target_dir, file_name)
        
        shutil.copy2(file_path, dest_path)
        print(f"已同步: {file_name}")

    print(f"同步任务结束。")

if __name__ == "__main__":
    sync_csv_files()
