import os
import shutil
import glob

def sync_csv_files():
    # 保持你原始脚本的路径变量名不变
    source_dir = 'source_repo/stock_data' 
    target_dir = 'main_repo/stock_data'

    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    # 获取源目录下所有 csv
    csv_files = glob.glob(os.path.join(source_dir, '*.csv'))
    
    if not csv_files:
        print(f"错误: 在 {source_dir} 未找到 CSV 文件。请检查源仓库路径是否正确。")
        return

    # --- 新增功能：清理冗余文件（镜像同步） ---
    # 获取源端所有文件名集合
    source_filenames = {os.path.basename(f) for f in csv_files}
    # 获取目标端现有所有 csv
    target_files = glob.glob(os.path.join(target_dir, '*.csv'))
    for t_path in target_files:
        # 如果目标有的文件，源端没了，就执行删除
        if os.path.basename(t_path) not in source_filenames:
            os.remove(t_path)
            print(f"已清理旧文件: {os.path.basename(t_path)}")
    # ---------------------------------------

    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        dest_path = os.path.join(target_dir, file_name)
        
        shutil.copy2(file_path, dest_path)
        print(f"已同步: {file_name}")

    print(f"同步任务结束。")

if __name__ == "__main__":
    sync_csv_files()
